"""QuickBooks Online API service.

Thin wrapper around the shared QBO module at _shared_config/integrations/qbo_service.py.
Delegates token management, API requests, and token refresh to the shared module.
Keeps project-specific logic: report fetching/parsing, Chart of Accounts merge,
interactive auth flows, and HTTP callback handlers.
"""

import re
import time
import webbrowser
from collections import defaultdict
from datetime import date
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from requests.auth import HTTPBasicAuth

from integrations.qbo_service import QBOService as _QBOService

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


class _TokenReceiverHandler(BaseHTTPRequestHandler):
    """HTTP handler to receive auth code redirected from the server callback.

    The server at bosoptimization.com/qbo/callback redirects here with the
    auth code and realmId. This handler exchanges the code for tokens locally.
    """

    def do_GET(self):
        """Handle GET request with auth code from server redirect."""
        query = parse_qs(urlparse(self.path).query)

        if "code" in query:
            code = query["code"][0]
            realm_id = query.get("realmId", [""])[0]

            # Exchange code for tokens locally
            try:
                auth = HTTPBasicAuth(
                    self.server.app_settings.client_id,
                    self.server.app_settings.client_secret,
                )
                response = requests.post(
                    QBO_TOKEN_ENDPOINT,
                    auth=auth,
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": self.server.app_settings.redirect_uri,
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                token_data = response.json()
                token_data["obtained_at"] = time.time()
                token_data["realm_id"] = realm_id

                self.server.token_data = token_data
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    f"<html><body><h1>Authorization successful!</h1>"
                    f"<p>Company ID: {realm_id}</p>"
                    f"<p>You can close this window.</p></body></html>".encode()
                )
            except Exception as e:
                self.server.error = str(e)
                self.send_response(500)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    f"<html><body><h1>Token exchange failed</h1>"
                    f"<p>{e}</p></body></html>".encode()
                )
        else:
            self.server.error = "No auth code received"
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Authorization failed</h1></body></html>")

    def log_message(self, format, *args):
        """Suppress HTTP server logging."""
        pass


class QBOService:
    """Service for interacting with QuickBooks Online API.

    Composes the shared _QBOService for token management and API requests,
    while keeping project-specific report fetching, parsing, and CoA logic.
    """

    def __init__(self, app_settings: QBOAppSettings, client_name: str):
        """
        Initialize the QBO service.

        Args:
            app_settings: QBO OAuth app settings
            client_name: Name of the client (for token storage)
        """
        self.app_settings = app_settings
        self.client_name = client_name

        # Compose the shared QBO service
        use_sandbox = app_settings.environment == "sandbox"
        self._qbo = _QBOService(
            client_id=app_settings.client_id,
            client_secret=app_settings.client_secret,
            token_client=client_name,
            use_sandbox=use_sandbox,
        )

        # Sync token data from shared module for local access
        self._sync_from_shared()

    def _sync_from_shared(self) -> None:
        """Sync local token state from the shared module."""
        self._token_data = None
        self._realm_id = self._qbo.realm_id

        if self._qbo.access_token:
            self._token_data = {
                "access_token": self._qbo.access_token,
                "refresh_token": self._qbo.refresh_token,
                "realm_id": self._qbo.realm_id,
                "obtained_at": time.time(),
                "expires_in": 3600,
            }
            if self._token_data:
                logger.info(f"Loaded existing token for {self.client_name}")

    def _sync_to_shared(self) -> None:
        """Push local token state to the shared module and persist."""
        if self._token_data:
            self._qbo.set_tokens(
                access_token=self._token_data["access_token"],
                refresh_token=self._token_data.get("refresh_token"),
                realm_id=self._realm_id,
                expires_in=self._token_data.get("expires_in", 3600),
            )
            # Also save via project's own persistence
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
            "state": f"{self.client_name}_FinancialSysUpdate_{int(time.time())}",
        }
        return f"{QBO_AUTH_ENDPOINT}?{urlencode(params)}"

    def authenticate_interactive(self) -> bool:
        """
        Run interactive OAuth flow.

        For server-based redirect URIs (https://), opens the browser and
        polls for the token file to appear (written by the server callback).
        For localhost redirect URIs, runs a local callback server.

        Returns:
            True if authentication successful
        """
        try:
            redirect_uri = self.app_settings.redirect_uri
            is_localhost = "localhost" in redirect_uri or "127.0.0.1" in redirect_uri

            auth_url = self.get_authorization_url()
            logger.info(f"Opening browser for {self.client_name} authorization...")
            webbrowser.open(auth_url)

            if is_localhost:
                return self._auth_local_callback(redirect_uri)
            else:
                return self._auth_poll_for_token()

        except Exception as e:
            logger.error(f"Authentication failed for {self.client_name}: {e}")
            return False

    def _auth_local_callback(self, redirect_uri: str) -> bool:
        """Auth flow using local HTTP callback server."""
        parsed = urlparse(redirect_uri)
        port = parsed.port or 8080

        server = HTTPServer(("localhost", port), OAuthCallbackHandler)
        server.auth_code = None
        server.realm_id = None
        server.error = None

        print(f"\nAuthorize {self.client_name} in the browser...")
        print("Waiting for callback...")

        server.timeout = 120
        while server.auth_code is None and server.error is None:
            server.handle_request()

        if server.auth_code:
            success = self._exchange_code(server.auth_code, server.realm_id)
            if success:
                logger.info(f"Successfully authenticated {self.client_name}")
                return True

        if server.error:
            logger.error(f"OAuth error for {self.client_name}: {server.error}")

        return False

    def _auth_poll_for_token(self) -> bool:
        """Auth flow for server-based callbacks.

        Runs a local HTTP server on port 8080 to receive the auth code
        redirected from the ProjectKickoff server callback. The local
        handler exchanges the code for tokens using local keyring credentials.
        """
        print(f"\nAuthorize {self.client_name} in the browser...")
        print("Waiting for authorization...")

        server = HTTPServer(("localhost", 8080), _TokenReceiverHandler)
        server.token_data = None
        server.error = None
        server.app_settings = self.app_settings

        server.timeout = 120
        while server.token_data is None and server.error is None:
            server.handle_request()

        if server.token_data:
            self._token_data = server.token_data
            self._realm_id = server.token_data.get("realm_id")
            self._sync_to_shared()
            logger.info(f"Successfully authenticated {self.client_name}")
            return True

        if server.error:
            logger.error(f"OAuth error for {self.client_name}: {server.error}")

        return False

    def _exchange_code(self, code: str, realm_id: Optional[str]) -> bool:
        """Exchange authorization code for tokens."""
        success, error = self._qbo.exchange_code(
            code=code,
            redirect_uri=self.app_settings.redirect_uri,
            realm_id=realm_id,
        )
        if success:
            self._realm_id = realm_id or self._qbo.realm_id
            self._sync_from_shared()
            # Also save via project's own persistence
            if self._token_data:
                save_qbo_token(self.client_name, self._token_data)
            return True
        logger.error(f"Token exchange failed: {error}")
        return False

    def _ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token, delegating to the shared module."""
        if not self._token_data:
            return False

        # Delegate expiry checking and refresh to the shared module
        if not self._qbo.is_authenticated():
            # Shared module's is_authenticated auto-refreshes if expired
            self._sync_from_shared()
            return self._qbo.is_authenticated()

        return True

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        retries: int = 3,
    ) -> Optional[dict]:
        """Make an authenticated API request.

        Delegates to the shared module's api_request and parses the JSON response.
        """
        if not self._ensure_valid_token():
            logger.error("No valid token available")
            return None

        response = self._qbo.api_request(
            method=method,
            endpoint=endpoint,
            params=params,
            retries=retries,
        )

        if response is None:
            return None

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error(f"API request failed: HTTP {response.status_code}")
            return None

        # Capture intuit_tid for Intuit support troubleshooting
        intuit_tid = response.headers.get('intuit_tid')
        if intuit_tid:
            logger.debug(f"intuit_tid: {intuit_tid}")

        return response.json()

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
        date_range: str = "Year",
        special_options: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a report from QuickBooks.

        Args:
            report_name: Report name (e.g., "Balance Sheet", "P&L")
            year: Year for the report
            display: Display type (Monthly, Quarterly, etc.)
            basis: Accounting basis (Cash, Accrual)
            full_year: If True, always use Jan 1 - Dec 31 (don't cap at today)
            date_range: Date range override ("Year", "ALL", or custom)
            special_options: Comma-separated special processing flags
                             (e.g., "Comparison", product names for filtering)

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
        today = date.today()

        # Handle date_range overrides
        if date_range.upper() == "ALL":
            # Let QBO determine the full date range (no years of zeros)
            start_date = None
            end_date = None
        elif date_range.upper() == "LAST":
            start_date = f"{year - 1}-01-01"
            end_date = f"{year - 1}-12-31"
        else:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

        # Handle "Comparison" special option: expand to prior year
        if "Comparison" in special_options:
            start_date = f"{year - 1}-01-01"

        # Cap end date at today if in current year (unless full_year or ALL)
        if not full_year and year == today.year and start_date is not None:
            end_date = today.isoformat()

        # Set date params: use date_macro for ALL, explicit dates otherwise
        use_all_dates = start_date is None  # date_range == ALL
        if use_all_dates:
            params["date_macro"] = "All"
        else:
            params["start_date"] = start_date
            params["end_date"] = end_date

        if qbo_report in ["BalanceSheet"]:
            if not use_all_dates and not full_year:
                # Override with fiscal YTD macro for Balance Sheet
                if display.lower() in ["month", "monthly", "months"]:
                    params.pop("start_date", None)
                    params.pop("end_date", None)
                    params["date_macro"] = "This Fiscal Year-to-date"

            if display.lower() in ["month", "monthly", "months"]:
                params["summarize_column_by"] = "Month"
            elif display.lower() in ["week", "weekly", "weeks"]:
                params["summarize_column_by"] = "Week"
        elif qbo_report in ["ProfitAndLoss", "CustomerSales", "ItemSales"]:
            if display.lower() in ["month", "monthly", "months"]:
                params["summarize_column_by"] = "Month"
            elif display.lower() in ["quarter", "quarterly", "quarters"]:
                params["summarize_column_by"] = "Quarter"
            elif display.lower() in ["year", "yearly", "years"]:
                params["summarize_column_by"] = "Year"
            elif display.lower() in ["week", "weekly", "weeks"]:
                params["summarize_column_by"] = "Week"
            elif display.lower() == "total":
                params["summarize_column_by"] = "Total"
        elif qbo_report in ["AgedReceivables", "AgedReceivablesSummary"]:
            # AR reports use as_of date
            params["report_date"] = today.isoformat()
            # Column M = aging period in days (e.g. "7", "15", "30")
            # Extract numeric value, default to 15
            period_match = re.search(r"\d+", display)
            aging_days = int(period_match.group()) if period_match else 15
            params["aging_period"] = str(aging_days)
            # Scale num_periods to cover ~90 days regardless of period size
            params["num_periods"] = str(max(4, min(12, 90 // aging_days)))

        # Accounting basis
        if basis.lower() in ["cash", "accrual"]:
            params["accounting_method"] = basis.capitalize()

        logger.info(f"Fetching {report_name} report for {year} ({display}, {basis})"
                     f"{f' [{special_options}]' if special_options else ''}")

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
            Tuple of (data_rows, headers).
            Each row also has a ``_depth`` attribute (int) indicating its
            nesting level in the QBO hierarchy (0 = top section, 1 = category,
            2 = sub-category, etc.).  Access via ``getattr(row, '_depth', 0)``
            or through the ``row_depths`` list stored on the returned rows list
            object (``rows.row_depths``).
        """
        if not report_data:
            return [], []

        headers = []
        rows = []
        row_depths: List[int] = []

        # Extract columns (headers)
        columns = report_data.get("Columns", {}).get("Column", [])
        for col in columns:
            col_title = col.get("ColTitle", "").strip()
            headers.append(col_title)

        # Extract rows
        def process_row_data(row_data: dict, depth: int = 0) -> List[Tuple[List[Any], int]]:
            """Recursively process row data. Returns (row, depth) tuples."""
            result = []

            row_type = row_data.get("type", "")
            header = row_data.get("Header", {})
            summary = row_data.get("Summary", {})
            col_data = row_data.get("ColData", [])
            sub_rows = row_data.get("Rows", {}).get("Row", [])

            # Check for TOTAL in row (for -T handling)
            is_total_row = False
            if col_data:
                first_col_value = col_data[0].get("value", "").strip() if col_data else ""
                if "total" in first_col_value.lower():
                    is_total_row = True

            # Handle -T: stop before TOTAL
            if row_max == "-T" and is_total_row:
                return result

            # Process header row if present
            if header and header.get("ColData"):
                header_row = [c.get("value", "").strip() for c in header.get("ColData", [])]
                result.append((header_row, depth))

            # Process this row's data
            if col_data:
                row_values = [c.get("value", "").strip() for c in col_data]
                result.append((row_values, depth))

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
                summary_row = [c.get("value", "").strip() for c in summary.get("ColData", [])]

                # Check if summary is a TOTAL
                if summary_row and "total" in str(summary_row[0]).lower():
                    if row_max == "-T":
                        pass  # Skip total
                    else:
                        result.append((summary_row, depth))
                        if row_max == "T":
                            return result
                else:
                    result.append((summary_row, depth))

            return result

        # Process all rows — unpack (row, depth) tuples
        report_rows = report_data.get("Rows", {}).get("Row", [])
        for row in report_rows:
            processed = process_row_data(row)
            for row_data, depth in processed:
                rows.append(row_data)
                row_depths.append(depth)

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

        # Deduplicate row labels: when a category name appears more than once
        # (e.g. "Cost of Goods Sold" as both section header and account),
        # rename the second occurrence to "COGS" so downstream sheets can
        # distinguish them.
        seen_labels: dict[str, int] = {}
        for row in rows:
            if row:
                label = str(row[0]).strip()
                if label in seen_labels:
                    seen_labels[label] += 1
                    row[0] = "COGS" if label == "Cost of Goods Sold" else f"{label} ({seen_labels[label]})"
                else:
                    seen_labels[label] = 1

        return rows, headers, row_depths

    # ---- Chart of Accounts merge methods ----

    # Map report section group names to QBO AccountType values.
    # These map at every nesting level — both top-level groups (e.g. "Income")
    # and sub-groups (e.g. "BankAccounts") are checked.
    SECTION_ACCOUNT_TYPES = {
        # P&L sections
        "Income": ["Income"],
        "CostOfGoodsSold": ["Cost of Goods Sold"],
        "COGS": ["Cost of Goods Sold"],
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

    def get_accounts(self, account_types: List[str] = None,
                     include_inactive: bool = True) -> List[Dict[str, Any]]:
        """
        Fetch accounts from Chart of Accounts.

        Args:
            account_types: Filter to specific account types. If None, returns all.
            include_inactive: If True, include inactive/deleted accounts so
                that historical rows remain stable in destination sheets.

        Returns:
            List of account dictionaries.
        """
        if include_inactive:
            query = "SELECT * FROM Account WHERE Active IN (true, false) MAXRESULTS 1000"
        else:
            query = "SELECT * FROM Account WHERE Active = true MAXRESULTS 1000"

        result = self._make_request("GET", "query", {
            "query": query,
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

    # Canonical section order for P&L reports. Used when creating missing
    # sections so they appear in the correct position.
    _PNL_SECTION_ORDER = [
        "Income", "CostOfGoodsSold", "COGS",
        "Expenses", "OtherIncome", "OtherExpenses",
    ]

    # Groups that share the same account type — if one exists, treat the
    # alias as already present so we don't create a duplicate section.
    _SECTION_ALIASES = {
        "COGS": "CostOfGoodsSold",
        "CostOfGoodsSold": "COGS",
    }

    def _process_sections(
        self,
        rows: List[dict],
        accounts: List[Dict[str, Any]],
        children_map: Dict[str, List[dict]],
        present_ids: Set[str],
        num_cols: int,
        is_top_level: bool = True,
    ) -> None:
        """Recursively find sections that match SECTION_ACCOUNT_TYPES and inject.

        Also creates entirely missing sections (e.g. OtherExpenses) when the
        COA contains accounts of a type that maps to a section absent from the
        report.  Missing section creation only runs at the top level to avoid
        injecting P&L sections into Balance Sheet reports.
        """
        existing_groups = set()

        for section in rows:
            group = section.get("group", "")

            if group in self.SECTION_ACCOUNT_TYPES:
                existing_groups.add(group)
                # Mark alias as existing too
                alias = self._SECTION_ALIASES.get(group)
                if alias:
                    existing_groups.add(alias)
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
                        is_top_level=False,
                    )

        if not is_top_level:
            return

        # Only create missing sections if this is a P&L report (has at least
        # one existing P&L section like Income, COGS, or Expenses)
        pnl_groups = {"Income", "CostOfGoodsSold", "COGS", "Expenses",
                      "OtherIncome", "OtherExpenses"}
        if not existing_groups & pnl_groups:
            return

        # Create missing sections for account types that have COA entries
        # but no section in the report (e.g. OtherExpenses with zero activity)
        for group, matching_types in self.SECTION_ACCOUNT_TYPES.items():
            if group in existing_groups:
                continue
            # Skip if an alias group already exists (e.g. COGS vs CostOfGoodsSold)
            alias = self._SECTION_ALIASES.get(group)
            if alias and alias in existing_groups:
                continue

            section_accounts = [a for a in accounts if a["AccountType"] in matching_types]
            if not section_accounts:
                continue

            # Only create top-level P&L sections here; Balance Sheet sub-groups
            # are nested and handled by recursion above
            if group not in self._PNL_SECTION_ORDER:
                continue

            section_ids = {a["Id"] for a in section_accounts}
            top_level = []
            for acct in section_accounts:
                parent_id = None
                if acct.get("SubAccount"):
                    parent_id = acct.get("ParentRef", {}).get("value")
                if not parent_id or parent_id not in section_ids:
                    top_level.append(acct)
            top_level.sort(key=lambda a: a["Name"])

            # Build human-readable section name
            section_label = {
                "OtherExpenses": "Other Expenses",
                "OtherIncome": "Other Income",
                "CostOfGoodsSold": "Cost of Goods Sold",
                "COGS": "Cost of Goods Sold",
            }.get(group, group)

            new_section = {
                "Header": {"ColData": self._make_zero_coldata(
                    section_label, "", num_cols)},
                "Rows": {"Row": []},
                "Summary": {"ColData": self._make_zero_coldata(
                    f"Total {section_label}", "", num_cols)},
                "type": "Section",
                "group": group,
            }

            self._inject_into_rows(
                new_section["Rows"]["Row"],
                top_level, children_map, present_ids, num_cols,
            )

            # Insert in canonical order.
            # Use both the PNL_SECTION_ORDER and well-known summary groups
            # (NetOperatingIncome, NetOtherIncome, NetIncome) for positioning.
            _FULL_ORDER = [
                "Income", "CostOfGoodsSold", "COGS", "GrossProfit",
                "Expenses", "NetOperatingIncome",
                "OtherIncome", "OtherExpenses", "NetOtherIncome", "NetIncome",
            ]
            insert_idx = len(rows)
            try:
                target_pos = _FULL_ORDER.index(group)
                for i, row in enumerate(rows):
                    rg = row.get("group", "")
                    if rg in _FULL_ORDER:
                        if _FULL_ORDER.index(rg) > target_pos:
                            insert_idx = i
                            break
            except ValueError:
                pass
            rows.insert(insert_idx, new_section)
            existing_groups.add(group)
            logger.info(f"Created missing report section: {section_label}")

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

    # ---- Customer / Item methods for Sales reports ----

    def get_customers(self) -> List[Dict[str, Any]]:
        """Fetch all active customers."""
        result = self._make_request("GET", "query", {
            "query": "SELECT * FROM Customer WHERE Active = true MAXRESULTS 1000",
            "minorversion": "75",
        })
        if not result or "QueryResponse" not in result:
            logger.error("Failed to fetch customers")
            return []
        return result["QueryResponse"].get("Customer", [])

    def get_items(self) -> List[Dict[str, Any]]:
        """Fetch all active products/services (items)."""
        result = self._make_request("GET", "query", {
            "query": "SELECT * FROM Item WHERE Active = true MAXRESULTS 1000",
            "minorversion": "75",
        })
        if not result or "QueryResponse" not in result:
            logger.error("Failed to fetch items")
            return []
        return result["QueryResponse"].get("Item", [])

    def inject_missing_entities(
        self,
        report_data: Dict[str, Any],
        entities: List[Dict[str, Any]],
        name_field: str = "DisplayName",
    ) -> Dict[str, Any]:
        """
        Inject zero-value rows for customers/items not present in a Sales report.

        Sales reports have a flat list of rows (no nested sections), so this
        is simpler than COA injection.

        Args:
            report_data: Raw report data from QBO API (modified in-place)
            entities: List of customer or item dicts
            name_field: Field name for the display name (DisplayName for
                        customers, Name for items)

        Returns:
            The modified report_data
        """
        if not report_data or not entities:
            return report_data

        num_cols = len(report_data.get("Columns", {}).get("Column", []))
        if num_cols == 0:
            return report_data

        top_rows = report_data.get("Rows", {}).get("Row", [])

        # Collect names already in the report (exclude TOTAL/GrandTotal)
        present_names = set()
        for row in top_rows:
            name = self._get_row_name(row)
            group = row.get("group", "")
            if name and group != "GrandTotal" and "total" not in name.lower():
                present_names.add(name.strip())

        # Find the data rows list (before GrandTotal)
        # GrandTotal section is typically the last row
        data_rows = []
        grand_total_idx = None
        for i, row in enumerate(top_rows):
            if row.get("group") == "GrandTotal":
                grand_total_idx = i
                break
            data_rows.append(row)

        # Inject missing entities
        injected = 0
        for entity in sorted(entities, key=lambda e: e.get(name_field, "")):
            name = entity.get(name_field, "").strip()
            if not name or name in present_names:
                continue

            # Skip sub-customers (they have a Parent ref)
            if entity.get("ParentRef"):
                continue

            new_row = {
                "ColData": self._make_zero_coldata(name, "", num_cols),
                "type": "Data",
            }
            self._insert_sorted(data_rows, new_row, name)
            injected += 1

        # Rebuild top_rows with data + GrandTotal
        if grand_total_idx is not None:
            report_data["Rows"]["Row"] = data_rows + [top_rows[grand_total_idx]]
        else:
            report_data["Rows"]["Row"] = data_rows

        if injected:
            logger.info(f"Injected {injected} missing entities "
                        f"({len(present_names)} present, "
                        f"{len(entities)} total)")
        return report_data
