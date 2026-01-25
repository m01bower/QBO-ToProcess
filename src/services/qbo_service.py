"""QuickBooks Online API service."""

import json
import time
import webbrowser
from datetime import datetime, date
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlparse, parse_qs
from typing import Optional, Dict, Any, List, Tuple
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
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a report from QuickBooks.

        Args:
            report_name: Report name (e.g., "Balance Sheet", "P&L")
            year: Year for the report
            display: Display type (Monthly, Quarterly, etc.)
            basis: Accounting basis (Cash, Accrual)

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

        # Cap end date at today if in current year
        today = date.today()
        if year == today.year:
            end_date = today.isoformat()

        if qbo_report in ["BalanceSheet"]:
            # Balance Sheet uses as_of date for point-in-time, but for comparison we use date range
            if display.lower() in ["monthly", "months"]:
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
