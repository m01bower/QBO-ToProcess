"""QuickBooks Online API service."""

import json
import time
import webbrowser
from datetime import datetime, date
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlparse, parse_qs
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass
import threading

import requests
from requests.auth import HTTPBasicAuth

from config import (
    QBO_AUTH_ENDPOINT,
    QBO_TOKEN_ENDPOINT,
    QBO_API_BASE_URL,
    QBO_SANDBOX_API_BASE_URL,
    QBO_SCOPES,
    QBO_REPORTS,
    REPORT_BASIS,
)
from settings import (
    QBOAppSettings,
    load_qbo_token,
    save_qbo_token,
)
from logger_setup import get_logger

logger = get_logger()


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler for OAuth callback."""

    def do_GET(self):
        """Handle GET request from OAuth callback."""
        query = parse_qs(urlparse(self.path).query)

        if "code" in query:
            self.server.auth_code = query["code"][0]
            self.server.realm_id = query.get("realmId", [None])[0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Authorization successful!</h1>")
            self.wfile.write(b"<p>You can close this window.</p></body></html>")
        else:
            error = query.get("error", ["Unknown error"])[0]
            self.server.auth_code = None
            self.server.error = error
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(f"<html><body><h1>Authorization failed: {error}</h1></body></html>".encode())

    def log_message(self, format, *args):
        """Suppress HTTP server logging."""
        pass


class QBOService:
    """Service for interacting with QuickBooks Online API."""

    def __init__(self, app_settings: QBOAppSettings, client_name: str):
        """
        Initialize the QBO service.

        Args:
            app_settings: QBO OAuth app settings
            client_name: Name of the client (for token storage)
        """
        self.app_settings = app_settings
        self.client_name = client_name
        self.session = requests.Session()
        self._token_data: Optional[dict] = None
        self._realm_id: Optional[str] = None

        # Load existing token if available
        self._load_token()

    def _load_token(self) -> None:
        """Load token from storage."""
        token_data = load_qbo_token(self.client_name)
        if token_data:
            self._token_data = token_data
            self._realm_id = token_data.get("realm_id")
            logger.info(f"Loaded existing token for {self.client_name}")

    def _save_token(self) -> None:
        """Save token to storage."""
        if self._token_data:
            self._token_data["realm_id"] = self._realm_id
            save_qbo_token(self.client_name, self._token_data)

    @property
    def api_base_url(self) -> str:
        """Get the appropriate API base URL."""
        if self.app_settings.environment == "sandbox":
            return QBO_SANDBOX_API_BASE_URL
        return QBO_API_BASE_URL

    @property
    def is_authenticated(self) -> bool:
        """Check if we have valid authentication."""
        return bool(self._token_data and self._realm_id)

    def get_authorization_url(self) -> str:
        """Generate the OAuth authorization URL."""
        params = {
            "client_id": self.app_settings.client_id,
            "response_type": "code",
            "scope": " ".join(QBO_SCOPES),
            "redirect_uri": self.app_settings.redirect_uri,
            "state": f"{self.client_name}_{int(time.time())}",
        }
        return f"{QBO_AUTH_ENDPOINT}?{urlencode(params)}"

    def authenticate_interactive(self) -> bool:
        """
        Run interactive OAuth flow with local callback server.

        Returns:
            True if authentication successful
        """
        try:
            # Parse redirect URI to get port
            parsed = urlparse(self.app_settings.redirect_uri)
            port = parsed.port or 8080

            # Start local server for callback
            server = HTTPServer(("localhost", port), OAuthCallbackHandler)
            server.auth_code = None
            server.realm_id = None
            server.error = None

            # Open browser for authorization
            auth_url = self.get_authorization_url()
            logger.info(f"Opening browser for {self.client_name} authorization...")
            webbrowser.open(auth_url)

            print(f"\nAuthorize {self.client_name} in the browser...")
            print("Waiting for callback...")

            # Wait for callback (with timeout)
            server.timeout = 120
            while server.auth_code is None and server.error is None:
                server.handle_request()

            if server.auth_code:
                # Exchange code for tokens
                success = self._exchange_code(server.auth_code, server.realm_id)
                if success:
                    logger.info(f"Successfully authenticated {self.client_name}")
                    return True

            if server.error:
                logger.error(f"OAuth error for {self.client_name}: {server.error}")

            return False

        except Exception as e:
            logger.error(f"Authentication failed for {self.client_name}: {e}")
            return False

    def _exchange_code(self, code: str, realm_id: Optional[str]) -> bool:
        """Exchange authorization code for tokens."""
        try:
            auth = HTTPBasicAuth(
                self.app_settings.client_id,
                self.app_settings.client_secret,
            )

            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.app_settings.redirect_uri,
            }

            response = requests.post(
                QBO_TOKEN_ENDPOINT,
                auth=auth,
                data=data,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()

            self._token_data = response.json()
            self._token_data["obtained_at"] = time.time()
            self._realm_id = realm_id
            self._save_token()

            return True

        except Exception as e:
            logger.error(f"Token exchange failed: {e}")
            return False

    def _refresh_token(self) -> bool:
        """Refresh the access token."""
        if not self._token_data or "refresh_token" not in self._token_data:
            return False

        try:
            auth = HTTPBasicAuth(
                self.app_settings.client_id,
                self.app_settings.client_secret,
            )

            data = {
                "grant_type": "refresh_token",
                "refresh_token": self._token_data["refresh_token"],
            }

            response = requests.post(
                QBO_TOKEN_ENDPOINT,
                auth=auth,
                data=data,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()

            new_token = response.json()
            new_token["obtained_at"] = time.time()
            new_token["realm_id"] = self._realm_id
            self._token_data = new_token
            self._save_token()

            logger.info(f"Refreshed token for {self.client_name}")
            return True

        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            return False

    def _ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token."""
        if not self._token_data:
            return False

        # Check if token is expired (with 5 min buffer)
        obtained_at = self._token_data.get("obtained_at", 0)
        expires_in = self._token_data.get("expires_in", 3600)
        if time.time() > obtained_at + expires_in - 300:
            return self._refresh_token()

        return True

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        retries: int = 3,
    ) -> Optional[dict]:
        """Make an authenticated API request."""
        if not self._ensure_valid_token():
            logger.error("No valid token available")
            return None

        url = f"{self.api_base_url}/{self._realm_id}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        for attempt in range(retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    timeout=60,
                )

                if response.status_code == 401:
                    # Token expired, try refresh
                    if self._refresh_token():
                        headers["Authorization"] = f"Bearer {self._token_data['access_token']}"
                        continue
                    return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(1 * (attempt + 1))

        return None

    def test_connection(self) -> bool:
        """Test the QBO API connection."""
        result = self._make_request("GET", "companyinfo/" + self._realm_id)
        if result and "CompanyInfo" in result:
            company_name = result["CompanyInfo"].get("CompanyName", "Unknown")
            logger.info(f"Connected to QBO: {company_name}")
            return True
        return False

    def get_report(
        self,
        report_name: str,
        year: int,
        display: str = "Monthly",
        basis: str = "Accrual",
        full_year: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a report from QuickBooks.

        Args:
            report_name: Report name (e.g., "Balance Sheet", "P&L")
            year: Year for the report
            display: Display type (Monthly, Quarterly, etc.)
            basis: Accounting basis (Cash, Accrual)
            full_year: If True, always use Jan 1 - Dec 31 (don't cap at today)

        Returns:
            Report data dictionary or None
        """
        # Map report name to QBO endpoint
        qbo_report = QBO_REPORTS.get(report_name)
        if not qbo_report:
            logger.error(f"Unknown report type: {report_name}")
            return None

        # Build parameters
        params = {}

        # Date parameters based on report type
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # Cap end date at today if in current year (unless full_year requested)
        today = date.today()
        if not full_year and year == today.year:
            end_date = today.isoformat()

        if qbo_report in ["BalanceSheet"]:
            # Balance Sheet uses as_of date for point-in-time, but for comparison we use date range
            if full_year:
                # Use explicit date range to get all months in the year
                params["start_date"] = start_date
                params["end_date"] = end_date
                if display.lower() in ["monthly", "months"]:
                    params["summarize_column_by"] = "Month"
                elif display.lower() in ["weekly", "weeks"]:
                    params["summarize_column_by"] = "Week"
            elif display.lower() in ["monthly", "months"]:
                params["date_macro"] = "This Fiscal Year-to-date"
                params["summarize_column_by"] = "Month"
            elif display.lower() in ["weekly", "weeks"]:
                params["date_macro"] = "This Fiscal Year-to-date"
                params["summarize_column_by"] = "Week"
            else:
                params["start_date"] = start_date
                params["end_date"] = end_date
        elif qbo_report in ["ProfitAndLoss", "CustomerSales", "ItemSales"]:
            params["start_date"] = start_date
            params["end_date"] = end_date

            if display.lower() in ["monthly", "months"]:
                params["summarize_column_by"] = "Month"
            elif display.lower() in ["quarterly", "quarters"]:
                params["summarize_column_by"] = "Quarter"
            elif display.lower() in ["yearly", "years"]:
                params["summarize_column_by"] = "Year"
        elif qbo_report in ["AgedReceivables", "AgedReceivablesSummary"]:
            # AR reports use as_of date
            params["report_date"] = today.isoformat()
            params["aging_period"] = "15"
            params["num_periods"] = "6"

        # Accounting basis
        if basis.lower() in ["cash", "accrual"]:
            params["accounting_method"] = basis.capitalize()

        logger.info(f"Fetching {report_name} report for {year} ({display}, {basis})")

        result = self._make_request("GET", f"reports/{qbo_report}", params)
        return result

    def parse_report_to_rows(
        self,
        report_data: Dict[str, Any],
        row_max: str = "*",
        col_max: str = "*",
    ) -> Tuple[List[List[Any]], List[str]]:
        """
        Parse QBO report data into rows for spreadsheet export.

        Args:
            report_data: Raw report data from QBO API
            row_max: Row limit ("*", "T", "-T")
            col_max: Column limit ("*", "T", "-T")

        Returns:
            Tuple of (data_rows, headers)
        """
        if not report_data:
            return [], []

        headers = []
        rows = []

        # Extract columns (headers)
        columns = report_data.get("Columns", {}).get("Column", [])
        for col in columns:
            col_title = col.get("ColTitle", "")
            headers.append(col_title)

        # Extract rows
        def process_row_data(row_data: dict, depth: int = 0) -> List[List[Any]]:
            """Recursively process row data."""
            result = []

            row_type = row_data.get("type", "")
            header = row_data.get("Header", {})
            summary = row_data.get("Summary", {})
            col_data = row_data.get("ColData", [])
            sub_rows = row_data.get("Rows", {}).get("Row", [])

            # Check for TOTAL in row (for -T handling)
            is_total_row = False
            if col_data:
                first_col_value = col_data[0].get("value", "") if col_data else ""
                if "total" in first_col_value.lower():
                    is_total_row = True

            # Handle -T: stop before TOTAL
            if row_max == "-T" and is_total_row:
                return result

            # Process header row if present
            if header and header.get("ColData"):
                header_row = [c.get("value", "") for c in header.get("ColData", [])]
                result.append(header_row)

            # Process this row's data
            if col_data:
                row_values = [c.get("value", "") for c in col_data]
                result.append(row_values)

            # Handle T: stop at TOTAL (include the total row but stop after)
            if row_max == "T" and is_total_row:
                return result

            # Process sub-rows
            if sub_rows:
                for sub_row in sub_rows:
                    sub_result = process_row_data(sub_row, depth + 1)
                    result.extend(sub_result)

            # Process summary row if present
            if summary and summary.get("ColData"):
                summary_row = [c.get("value", "") for c in summary.get("ColData", [])]

                # Check if summary is a TOTAL
                if summary_row and "total" in str(summary_row[0]).lower():
                    if row_max == "-T":
                        pass  # Skip total
                    else:
                        result.append(summary_row)
                        if row_max == "T":
                            return result
                else:
                    result.append(summary_row)

            return result

        # Process all rows
        report_rows = report_data.get("Rows", {}).get("Row", [])
        for row in report_rows:
            processed = process_row_data(row)
            rows.extend(processed)

        # Apply column max
        if col_max in ["-T", "T"] and headers:
            # Find TOTAL column
            total_col_idx = None
            for i, h in enumerate(headers):
                if "total" in h.lower():
                    total_col_idx = i
                    break

            if total_col_idx is not None:
                if col_max == "-T":
                    # Exclude total column
                    headers = headers[:total_col_idx]
                    rows = [r[:total_col_idx] for r in rows]
                elif col_max == "T":
                    # Include total column but nothing after
                    headers = headers[:total_col_idx + 1]
                    rows = [r[:total_col_idx + 1] for r in rows]

        return rows, headers

    # ---- Chart of Accounts merge methods ----

    # Map report section group names to QBO AccountType values.
    # These map at every nesting level — both top-level groups (e.g. "Income")
    # and sub-groups (e.g. "BankAccounts") are checked.
    SECTION_ACCOUNT_TYPES = {
        # P&L sections
        "Income": ["Income"],
        "CostOfGoodsSold": ["Cost of Goods Sold"],
        "Expenses": ["Expense"],
        "OtherIncome": ["Other Income"],
        "OtherExpenses": ["Other Expense"],
        # Balance Sheet sub-groups (leaf-level sections that contain accounts)
        "BankAccounts": ["Bank"],
        "AR": ["Accounts Receivable"],
        "OtherCurrentAssets": ["Other Current Asset"],
        "FixedAssets": ["Fixed Asset"],
        "OtherAssets": ["Other Asset"],
        "AP": ["Accounts Payable"],
        "CreditCards": ["Credit Card"],
        "OtherCurrentLiabilities": ["Other Current Liability"],
        "LongTermLiabilities": ["Long Term Liability"],
        "Equity": ["Equity"],
    }

    def get_accounts(self, account_types: List[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch active accounts from Chart of Accounts.

        Args:
            account_types: Filter to specific account types. If None, returns all.

        Returns:
            List of account dictionaries.
        """
        result = self._make_request("GET", "query", {
            "query": "SELECT * FROM Account WHERE Active = true",
            "minorversion": "75",
        })
        if not result or "QueryResponse" not in result:
            logger.error("Failed to fetch Chart of Accounts")
            return []

        accounts = result["QueryResponse"].get("Account", [])
        if account_types:
            accounts = [a for a in accounts if a.get("AccountType") in account_types]
        return accounts

    def inject_missing_accounts(
        self,
        report_data: Dict[str, Any],
        accounts: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Inject zero-balance rows into report data for accounts not present.

        Modifies report_data in-place and returns it. Accounts are inserted
        into the correct section based on their AccountType and in
        alphabetical order within their parent group.

        Args:
            report_data: Raw report data from QBO API (modified in-place)
            accounts: List of COA account dicts from get_accounts()

        Returns:
            The modified report_data
        """
        if not report_data or not accounts:
            return report_data

        # Determine number of columns from report
        num_cols = len(report_data.get("Columns", {}).get("Column", []))
        if num_cols == 0:
            return report_data

        # Collect all account IDs already present in the report
        present_ids = set()
        self._collect_present_ids(report_data.get("Rows", {}).get("Row", []), present_ids)

        # Build account lookup and parent-children map
        account_map = {a["Id"]: a for a in accounts}
        children_map = defaultdict(list)
        for acct in accounts:
            parent_id = None
            if acct.get("SubAccount"):
                parent_id = acct.get("ParentRef", {}).get("value")
            if parent_id and parent_id in account_map:
                children_map[parent_id].append(acct)
            # Sort children alphabetically
        for parent_id in children_map:
            children_map[parent_id].sort(key=lambda a: a["Name"])

        # Process sections recursively (Balance Sheet has nested sub-groups)
        top_rows = report_data.get("Rows", {}).get("Row", [])
        self._process_sections(
            top_rows, accounts, children_map, present_ids, num_cols,
        )

        logger.info(f"Injected zero-balance accounts ({len(present_ids)} present, "
                     f"{len(accounts)} total in COA)")
        return report_data

    def _process_sections(
        self,
        rows: List[dict],
        accounts: List[Dict[str, Any]],
        children_map: Dict[str, List[dict]],
        present_ids: Set[str],
        num_cols: int,
    ) -> None:
        """Recursively find sections that match SECTION_ACCOUNT_TYPES and inject."""
        for section in rows:
            group = section.get("group", "")

            if group in self.SECTION_ACCOUNT_TYPES:
                # This section maps to COA account types — inject missing accounts
                matching_types = self.SECTION_ACCOUNT_TYPES[group]
                section_accounts = [a for a in accounts if a["AccountType"] in matching_types]

                if section_accounts:
                    # Get top-level accounts for this section
                    section_ids = {a["Id"] for a in section_accounts}
                    top_level = []
                    for acct in section_accounts:
                        parent_id = None
                        if acct.get("SubAccount"):
                            parent_id = acct.get("ParentRef", {}).get("value")
                        if not parent_id or parent_id not in section_ids:
                            top_level.append(acct)
                    top_level.sort(key=lambda a: a["Name"])

                    # Ensure section has a Rows container
                    if "Rows" not in section:
                        section["Rows"] = {"Row": []}
                    elif "Row" not in section.get("Rows", {}):
                        section["Rows"]["Row"] = []

                    self._inject_into_rows(
                        section["Rows"]["Row"],
                        top_level,
                        children_map,
                        present_ids,
                        num_cols,
                    )
            else:
                # Not a matching group — recurse into sub-rows to find nested sections
                sub_rows = section.get("Rows", {}).get("Row", [])
                if sub_rows:
                    self._process_sections(
                        sub_rows, accounts, children_map, present_ids, num_cols,
                    )

    def _collect_present_ids(self, rows: List[dict], present_ids: Set[str]) -> None:
        """Recursively collect all account IDs present in report rows."""
        for row in rows:
            # Check ColData for id
            col_data = row.get("ColData", [])
            if col_data and col_data[0].get("id"):
                present_ids.add(col_data[0]["id"])

            # Check Header for id
            header = row.get("Header", {})
            header_cols = header.get("ColData", [])
            if header_cols and header_cols[0].get("id"):
                present_ids.add(header_cols[0]["id"])

            # Recurse into sub-rows
            sub_rows = row.get("Rows", {}).get("Row", [])
            if sub_rows:
                self._collect_present_ids(sub_rows, present_ids)

    def _make_zero_coldata(self, name: str, account_id: str, num_cols: int) -> List[dict]:
        """Create a ColData array with the account name and zeros."""
        cols = [{"value": name, "id": account_id}]
        for _ in range(num_cols - 1):
            cols.append({"value": ""})
        return cols

    def _inject_into_rows(
        self,
        existing_rows: List[dict],
        coa_accounts: List[dict],
        children_map: Dict[str, List[dict]],
        present_ids: Set[str],
        num_cols: int,
    ) -> None:
        """
        Inject missing accounts into an existing row list.

        For each COA account:
        - If already present and has children, recurse into its sub-rows
        - If missing and is a leaf, add a Data row
        - If missing and has children, add a Section with Header/Rows/Summary
        """
        for acct in coa_accounts:
            acct_id = acct["Id"]
            acct_name = acct["Name"]
            children = children_map.get(acct_id, [])

            if acct_id in present_ids:
                # Account exists in report — check if it has children to inject
                if children:
                    # Find the existing section/row for this account
                    for row in existing_rows:
                        row_id = None
                        header = row.get("Header", {})
                        header_cols = header.get("ColData", [])
                        if header_cols and header_cols[0].get("id") == acct_id:
                            row_id = acct_id
                        col_data = row.get("ColData", [])
                        if col_data and col_data[0].get("id") == acct_id:
                            row_id = acct_id

                        if row_id == acct_id:
                            # Found the row — ensure it has sub-rows container
                            if "Rows" not in row:
                                # Convert Data row to Section with sub-rows
                                row["Header"] = {"ColData": row.pop("ColData")}
                                row["Rows"] = {"Row": []}
                                row["Summary"] = {
                                    "ColData": self._make_zero_coldata(
                                        f"Total {acct_name}", "", num_cols
                                    )
                                }
                                row["type"] = "Section"
                            if "Row" not in row.get("Rows", {}):
                                row["Rows"]["Row"] = []
                            self._inject_into_rows(
                                row["Rows"]["Row"],
                                children,
                                children_map,
                                present_ids,
                                num_cols,
                            )
                            break
            else:
                # Account is missing — inject it
                if children:
                    # Parent account with children: create a Section
                    new_section = {
                        "Header": {
                            "ColData": self._make_zero_coldata(acct_name, acct_id, num_cols)
                        },
                        "Rows": {"Row": []},
                        "Summary": {
                            "ColData": self._make_zero_coldata(
                                f"Total {acct_name}", "", num_cols
                            )
                        },
                        "type": "Section",
                    }
                    # Recursively inject children
                    self._inject_into_rows(
                        new_section["Rows"]["Row"],
                        children,
                        children_map,
                        present_ids,
                        num_cols,
                    )
                    self._insert_sorted(existing_rows, new_section, acct_name)
                else:
                    # Leaf account: create a Data row
                    new_row = {
                        "ColData": self._make_zero_coldata(acct_name, acct_id, num_cols),
                        "type": "Data",
                    }
                    self._insert_sorted(existing_rows, new_row, acct_name)

    def _insert_sorted(self, rows: List[dict], new_row: dict, name: str) -> None:
        """Insert a row into the list maintaining alphabetical order by name."""
        insert_idx = len(rows)
        for i, row in enumerate(rows):
            existing_name = self._get_row_name(row)
            if existing_name.lower() > name.lower():
                insert_idx = i
                break
        rows.insert(insert_idx, new_row)

    @staticmethod
    def _get_row_name(row: dict) -> str:
        """Get the display name from a report row."""
        # Check Header first (Section rows)
        header_cols = row.get("Header", {}).get("ColData", [])
        if header_cols:
            return header_cols[0].get("value", "")
        # Then ColData (Data rows)
        col_data = row.get("ColData", [])
        if col_data:
            return col_data[0].get("value", "")
        return ""
