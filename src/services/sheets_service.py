"""Google Sheets service for QBO ToProcess."""

import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

from config import GOOGLE_SCOPES, TOPROCESS_TAB_NAME
from settings import (
    get_service_account_path,
    get_google_credentials_path,
    get_google_token_path,
    save_google_token,
    load_google_token,
)
from logger_setup import get_logger

logger = get_logger()


class SheetsService:
    """Service for interacting with Google Sheets."""

    def __init__(
        self,
        auth_method: str = "oauth",
        client_name: str = "default",
    ):
        """
        Initialize the Sheets service.

        Args:
            auth_method: "oauth" or "service_account"
            client_name: Client name for OAuth token storage
        """
        self.auth_method = auth_method
        self.client_name = client_name
        self._service: Optional[Resource] = None
        self._credentials = None

    def authenticate(self) -> bool:
        """
        Authenticate with Google Sheets API.

        Returns:
            True if authentication successful
        """
        try:
            if self.auth_method == "service_account":
                return self._authenticate_service_account()
            else:
                return self._authenticate_oauth()

        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {e}")
            return False

    def _authenticate_service_account(self) -> bool:
        """Authenticate using service account."""
        sa_path = get_service_account_path()
        if not sa_path.exists():
            logger.error(f"Service account file not found: {sa_path}")
            return False

        self._credentials = service_account.Credentials.from_service_account_file(
            str(sa_path),
            scopes=GOOGLE_SCOPES,
        )
        self._service = build("sheets", "v4", credentials=self._credentials)
        logger.info("Authenticated with Google Sheets (service account)")
        return True

    def _authenticate_oauth(self) -> bool:
        """Authenticate using OAuth (browser flow)."""
        creds = None
        token_path = get_google_token_path(self.client_name)
        credentials_path = get_google_credentials_path()

        # Load existing token if available
        if token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(token_path), GOOGLE_SCOPES)
            except Exception as e:
                logger.warning(f"Failed to load existing token: {e}")

        # If no valid credentials, initiate OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.warning(f"Token refresh failed: {e}")
                    creds = None

            if not creds:
                if not credentials_path.exists():
                    logger.error(f"Google credentials.json not found at {credentials_path}")
                    logger.error("Download OAuth credentials from Google Cloud Console")
                    return False

                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials_path),
                    GOOGLE_SCOPES,
                )
                creds = flow.run_local_server(port=0)

            # Save the token for future use
            save_google_token(self.client_name, {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
            })

        self._credentials = creds
        self._service = build("sheets", "v4", credentials=creds)
        logger.info(f"Authenticated with Google Sheets (OAuth - {self.client_name})")
        return True

    def is_authenticated(self) -> bool:
        """Check if already authenticated."""
        return self._service is not None

    @property
    def sheets(self) -> Resource:
        """Get the Sheets API service."""
        if not self._service:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return self._service

    def read_toprocess_config(
        self,
        spreadsheet_id: str,
    ) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        """
        Read the ToProcess configuration tab.

        Args:
            spreadsheet_id: Google Sheet ID

        Returns:
            Tuple of (year, list of report configs)
        """
        try:
            # First, get the year from A1
            year_result = self.sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{TOPROCESS_TAB_NAME}'!A1",
            ).execute()

            year_value = year_result.get("values", [[None]])[0][0]
            try:
                year = int(year_value)
            except (ValueError, TypeError):
                year = datetime.now().year
                logger.warning(f"Could not parse year from A1, using current year: {year}")

            # Get all data from row 2 onwards (row 1 is headers)
            data_result = self.sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{TOPROCESS_TAB_NAME}'!A2:P100",
            ).execute()

            rows = data_result.get("values", [])

            # Also get headers
            header_result = self.sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{TOPROCESS_TAB_NAME}'!A1:P1",
            ).execute()
            headers = header_result.get("values", [[]])[0]

            # Map column letters to indices
            col_map = {
                "D": 3,   # Row Max
                "E": 4,   # Column Max
                "F": 5,   # Google Sheet ID
                "G": 6,   # Google Sheet Name
                "H": 7,   # Starting Cell
                "I": 8,   # Temp
                "J": 9,   # New Tab Name
                "K": 10,  # Processed Date
                "L": 11,  # QBO Report
                "M": 12,  # Report Display
                "N": 13,  # Report Basis
            }

            configs = []
            for row_idx, row in enumerate(rows):
                # Skip empty rows
                if not row or len(row) < 12:
                    continue

                # Get values with safe indexing
                def get_val(col: str, default: str = "") -> str:
                    idx = col_map.get(col, 0)
                    return row[idx] if idx < len(row) else default

                qbo_report = get_val("L")
                if not qbo_report:
                    continue  # Skip rows without a report name

                config = {
                    "row_index": row_idx + 2,  # 1-indexed, +1 for header
                    "row_max": get_val("D", "*"),
                    "col_max": get_val("E", "*"),
                    "dest_sheet_id": get_val("F"),
                    "dest_tab_name": get_val("G"),
                    "starting_cell": get_val("H", "A1"),
                    "temp_tab": get_val("I"),
                    "new_tab_name_format": get_val("J"),
                    "qbo_report": qbo_report,
                    "report_display": get_val("M", "Monthly"),
                    "report_basis": get_val("N", "Accrual"),
                }
                configs.append(config)

            logger.info(f"Loaded {len(configs)} report configurations for year {year}")
            return year, configs

        except HttpError as e:
            logger.error(f"Failed to read ToProcess config: {e}")
            return None, []

    def verify_sheet_access(self, spreadsheet_id: str) -> bool:
        """
        Verify we can access a spreadsheet.

        Args:
            spreadsheet_id: Google Sheet ID

        Returns:
            True if accessible
        """
        try:
            result = self.sheets.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
            ).execute()
            title = result.get("properties", {}).get("title", "Unknown")
            logger.info(f"Verified access to: {title}")
            return True
        except HttpError as e:
            logger.error(f"Cannot access spreadsheet {spreadsheet_id}: {e}")
            return False

    def get_tab_id(self, spreadsheet_id: str, tab_name: str) -> Optional[int]:
        """
        Get the sheet ID for a tab by name.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name

        Returns:
            Sheet ID or None if not found
        """
        try:
            result = self.sheets.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
            ).execute()

            for sheet in result.get("sheets", []):
                props = sheet.get("properties", {})
                if props.get("title") == tab_name:
                    return props.get("sheetId")

            return None
        except HttpError:
            return None

    def duplicate_tab(
        self,
        spreadsheet_id: str,
        source_tab_name: str,
        new_tab_name: str,
    ) -> bool:
        """
        Duplicate a tab with a new name.

        Args:
            spreadsheet_id: Google Sheet ID
            source_tab_name: Name of tab to duplicate
            new_tab_name: Name for the new tab

        Returns:
            True if successful
        """
        try:
            source_id = self.get_tab_id(spreadsheet_id, source_tab_name)
            if source_id is None:
                logger.error(f"Source tab not found: {source_tab_name}")
                return False

            # Check if destination already exists
            if self.get_tab_id(spreadsheet_id, new_tab_name) is not None:
                logger.info(f"Tab already exists: {new_tab_name}")
                return True

            # Duplicate the sheet
            request = {
                "duplicateSheet": {
                    "sourceSheetId": source_id,
                    "newSheetName": new_tab_name,
                }
            }

            self.sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [request]},
            ).execute()

            logger.info(f"Duplicated {source_tab_name} to {new_tab_name}")
            return True

        except HttpError as e:
            logger.error(f"Failed to duplicate tab: {e}")
            return False

    def clear_tab_data(
        self,
        spreadsheet_id: str,
        tab_name: str,
        starting_cell: str,
    ) -> bool:
        """
        Clear data from a tab starting at a specific cell.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            starting_cell: Cell to start clearing from

        Returns:
            True if successful
        """
        try:
            # Parse starting cell to get row
            match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
            if not match:
                logger.error(f"Invalid starting cell: {starting_cell}")
                return False

            col = match.group(1)
            row = int(match.group(2))

            # Clear from starting cell to end of sheet
            range_name = f"'{tab_name}'!{col}{row}:ZZ"

            self.sheets.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=range_name,
            ).execute()

            logger.debug(f"Cleared {tab_name} from {starting_cell}")
            return True

        except HttpError as e:
            logger.error(f"Failed to clear tab: {e}")
            return False

    def write_data(
        self,
        spreadsheet_id: str,
        tab_name: str,
        starting_cell: str,
        data: List[List[Any]],
        include_headers: bool = False,
        headers: Optional[List[str]] = None,
    ) -> Tuple[bool, int]:
        """
        Write data to a tab.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            starting_cell: Cell to start writing
            data: 2D list of values
            include_headers: Whether to write headers first
            headers: Header row if include_headers=True

        Returns:
            Tuple of (success, rows_written)
        """
        if not data:
            logger.warning(f"No data to write to {tab_name}")
            return True, 0

        try:
            values_to_write = []

            if include_headers and headers:
                values_to_write.append(headers)

            values_to_write.extend(data)

            range_name = f"'{tab_name}'!{starting_cell}"

            self.sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": values_to_write},
            ).execute()

            rows_written = len(data)
            logger.info(f"Wrote {rows_written} rows to {tab_name}!{starting_cell}")
            return True, rows_written

        except HttpError as e:
            logger.error(f"Failed to write data: {e}")
            return False, 0

    def update_processed_date(
        self,
        spreadsheet_id: str,
        row_index: int,
        processed_col: str = "K",
    ) -> bool:
        """
        Update the processed date for a ToProcess row.

        Args:
            spreadsheet_id: Google Sheet ID
            row_index: Row number in ToProcess tab
            processed_col: Column for processed date

        Returns:
            True if successful
        """
        try:
            timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            range_name = f"'{TOPROCESS_TAB_NAME}'!{processed_col}{row_index}"

            self.sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": [[timestamp]]},
            ).execute()

            return True

        except HttpError as e:
            logger.error(f"Failed to update processed date: {e}")
            return False
