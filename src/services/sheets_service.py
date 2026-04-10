"""Google Sheets service for FinancialSysUpdate.

Delegates core Sheets API operations to the shared SheetsService module.
Project-specific methods (tab duplication, row insertion, config readers,
etc.) remain in this wrapper.

Auth: supports both OAuth (credential_ref) and service account
(pre-authenticated credentials passed to shared module).
"""

import re
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from google.oauth2 import service_account
from googleapiclient.errors import HttpError

from config import GOOGLE_SCOPES, TOPROCESS_TAB_NAME, AUTOPROCESS_TAB_NAME
from settings import (
    get_service_account_path,
    get_google_credentials_path,
    get_google_token_path,
    save_google_token,
)
from logger_setup import get_logger

logger = get_logger()

# ── Import shared SheetsService ──────────────────────────────────────
_SHARED_CONFIG = Path(__file__).parent.parent.parent.parent / "_shared_config"
sys.path.insert(0, str(_SHARED_CONFIG))
from integrations.sheets_service import SheetsService as _SharedSheetsService  # noqa: E402
sys.path.pop(0)


class SheetsService:
    """Service for interacting with Google Sheets.

    Wraps the shared SheetsService for core operations while
    adding FinancialSysUpdate-specific methods.
    """

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
        self._shared: Optional[_SharedSheetsService] = None
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

        creds = service_account.Credentials.from_service_account_file(
            str(sa_path),
            scopes=GOOGLE_SCOPES,
        )
        self._shared = _SharedSheetsService(
            credentials=creds,
            scopes=GOOGLE_SCOPES,
        )
        self._credentials = creds
        logger.info("Authenticated with Google Sheets (service account)")
        return True

    def _authenticate_oauth(self) -> bool:
        """Authenticate using OAuth (browser flow)."""
        # Use shared module with credential_ref for standard OAuth
        # However, FinancialSysUpdate uses per-client credential paths
        # and custom token saving, so we set explicit paths
        credentials_path = get_google_credentials_path(self.client_name)
        token_path = get_google_token_path(self.client_name)

        self._shared = _SharedSheetsService(
            credentials_path=credentials_path,
            token_path=token_path,
            scopes=GOOGLE_SCOPES,
        )

        if not self._shared.authenticate():
            return False

        self._credentials = self._shared.credentials
        logger.info(f"Authenticated with Google Sheets (OAuth - {self.client_name})")
        return True

    def is_authenticated(self) -> bool:
        """Check if already authenticated."""
        return self._shared is not None

    @property
    def sheets(self):
        """Get the Sheets API service."""
        if not self._shared:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return self._shared.service

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
            year_value = self._shared.read_cell(
                TOPROCESS_TAB_NAME, "A1", spreadsheet_id
            )
            try:
                year = int(year_value)
            except (ValueError, TypeError):
                year = datetime.now().year
                logger.warning(f"Could not parse year from A1, using current year: {year}")

            # Get all data from row 2 onwards (row 1 is headers)
            rows = self._shared.read_range(
                f"'{TOPROCESS_TAB_NAME}'!A2:Q100", spreadsheet_id
            )

            # Also get headers
            headers = self._shared.read_range(
                f"'{TOPROCESS_TAB_NAME}'!A1:Q1", spreadsheet_id
            )

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
                "N": 13,  # Date Range
                "O": 14,  # Report Basis
                "P": 15,  # Move (New) Tab to Index
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
                    "date_range": get_val("N", "This Year"),
                    "report_basis": get_val("O", "Accrual"),
                    "tab_index": get_val("P", ""),
                }
                configs.append(config)

            logger.info(f"Loaded {len(configs)} report configurations for year {year}")
            return year, configs

        except HttpError as e:
            logger.error(f"Failed to read ToProcess config: {e}")
            return None, []

    @staticmethod
    def configs_from_master(reports, year: int, master_sheet_id: str) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Convert MasterConfig QBOReportConfig objects to the dict format
        expected by ReportProcessor.

        Args:
            reports: List of QBOReportConfig from MasterConfig
            year: Report year from MasterConfig
            master_sheet_id: MasterConfig sheet ID (for processed date updates)

        Returns:
            Tuple of (year, list of config dicts)
        """
        configs = []
        for idx, r in enumerate(reports):
            if not r.qbo_report:
                continue
            config = {
                "row_index": idx + 3,  # Row 1=headers, 2=year, 3+=data (1-indexed)
                "row_max": r.row_max or "*",
                "col_max": r.column_max or "*",
                "dest_sheet_id": r.google_sheet_id,
                "dest_tab_name": r.google_sheet_name,
                "starting_cell": r.google_sheet_starting_cell or "A1",
                "temp_tab": r.temp,
                "new_tab_name_format": r.new_tab_name,
                "qbo_report": r.qbo_report,
                "report_display": r.report_display or "Monthly",
                "date_range": r.date_range or "This Year",
                "report_basis": r.report_basis or "Accrual",
                "tab_index": r.move_new_tab_to_index or "",
                "verify_last_row": r.verify_last_row.upper().startswith("Y") if r.verify_last_row else False,
                "filter": r.filter or "",
                "sort": r.sort or "",
            }
            configs.append(config)
        return year, configs

    def read_autoprocess_config(
        self,
        spreadsheet_id: str,
    ) -> Tuple[Optional[int], List[Dict[str, Any]]]:
        """
        Read the AutoProcess configuration tab.

        Layout:
            A1 = year
            Row 2+ = data (row 1 is blank/header, skipped)
            Columns: A=qbo_file_id, B=report_name, C=date_range,
                     D=display, E=basis, F=special_options

        Args:
            spreadsheet_id: Google Sheet ID

        Returns:
            Tuple of (year, list of report configs)
        """
        try:
            # Get the year from A1
            year_value = self._shared.read_cell(
                AUTOPROCESS_TAB_NAME, "A1", spreadsheet_id
            )
            try:
                year = int(year_value)
            except (ValueError, TypeError):
                year = datetime.now().year
                logger.warning(f"Could not parse year from A1, using current year: {year}")

            # Get all data from row 3 onwards (row 1 = year, row 2 = headers)
            rows = self._shared.read_range(
                f"'{AUTOPROCESS_TAB_NAME}'!A3:F100", spreadsheet_id
            )

            configs = []
            for row_idx, row in enumerate(rows):
                # Skip empty rows
                if not row or len(row) < 2:
                    continue

                # Safe column access
                def get_col(idx: int, default: str = "") -> str:
                    return row[idx].strip() if idx < len(row) and row[idx] else default

                report_name = get_col(1)
                if not report_name:
                    continue  # Skip rows without a report name

                date_range = get_col(2, "Year")
                display = get_col(3, "Monthly")
                basis = get_col(4, "Accrual")
                special_options = get_col(5)

                # Normalize basis
                if basis.upper() == "CASH":
                    basis = "Cash"
                elif basis.upper() == "ACCRUAL":
                    basis = "Accrual"

                # Backward compat: if display is blank and special_options
                # contains "2 Weeks", treat as Biweekly display
                if not get_col(3) and "2 Weeks" in special_options:
                    display = "Biweekly"

                config = {
                    "row_index": row_idx + 3,  # 1-indexed sheet row (data starts row 3)
                    "qbo_file_id": get_col(0),
                    "report_name": report_name,
                    "date_range": date_range,
                    "display": display,
                    "basis": basis,
                    "special_options": special_options,
                }
                configs.append(config)

            logger.info(f"Loaded {len(configs)} AutoProcess configurations for year {year}")
            return year, configs

        except HttpError as e:
            logger.error(f"Failed to read AutoProcess config: {e}")
            return None, []

    def verify_sheet_access(self, spreadsheet_id: str) -> bool:
        """
        Verify we can access a spreadsheet.

        Args:
            spreadsheet_id: Google Sheet ID

        Returns:
            True if accessible
        """
        success, result = self._shared.test_access(spreadsheet_id)
        if success:
            logger.info(f"Verified access to: {result}")
        else:
            logger.error(f"Cannot access spreadsheet {spreadsheet_id}: {result}")
        return success

    def get_tab_id(self, spreadsheet_id: str, tab_name: str) -> Optional[int]:
        """
        Get the sheet ID for a tab by name.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name

        Returns:
            Sheet ID or None if not found
        """
        return self._shared.get_sheet_id(tab_name, spreadsheet_id)

    def duplicate_tab(
        self,
        spreadsheet_id: str,
        source_tab_name: str,
        new_tab_name: str,
        tab_index: Optional[int] = None,
    ) -> bool:
        """
        Duplicate a tab with a new name.

        If the destination tab already exists, its data is cleared and
        repopulated from the source template. This preserves the tab
        while ensuring clean formatting.

        Args:
            spreadsheet_id: Google Sheet ID
            source_tab_name: Name of tab to duplicate
            new_tab_name: Name for the new tab
            tab_index: Position to place the new tab (0-based). If None, no move.

        Returns:
            True if successful (tab exists and is ready for data)
        """
        try:
            source_id = self.get_tab_id(spreadsheet_id, source_tab_name)
            if source_id is None:
                logger.error(f"Source tab not found: {source_tab_name}")
                return False

            # If destination tab already exists, clear it and copy data over
            existing_id = self.get_tab_id(spreadsheet_id, new_tab_name)
            if existing_id is not None:
                logger.info(f"Tab '{new_tab_name}' exists — clearing and refreshing from template")
                self._shared.clear_range(
                    f"'{new_tab_name}'!A1:ZZ", spreadsheet_id,
                )
                source_data = self._shared.read_range(
                    f"'{source_tab_name}'!A1:ZZ", spreadsheet_id,
                )
                if source_data:
                    self._shared.write_range(
                        f"'{new_tab_name}'!A1", source_data, spreadsheet_id,
                    )
                logger.info(f"Refreshed '{new_tab_name}' from '{source_tab_name}'")
                return True

            # Duplicate from template
            dup_request = {
                "duplicateSheet": {
                    "sourceSheetId": source_id,
                    "newSheetName": new_tab_name,
                }
            }
            if tab_index is not None:
                dup_request["duplicateSheet"]["insertSheetIndex"] = tab_index

            if not self._shared.batch_update([dup_request], spreadsheet_id):
                return False

            # Unhide the new tab (template may be hidden)
            new_sheet_id = self.get_tab_id(spreadsheet_id, new_tab_name)
            if new_sheet_id is not None:
                self._shared.batch_update(
                    [{
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": new_sheet_id,
                                "hidden": False,
                            },
                            "fields": "hidden",
                        }
                    }],
                    spreadsheet_id,
                )

            logger.info(f"Duplicated {source_tab_name} to {new_tab_name}"
                        + (f" at index {tab_index}" if tab_index is not None else ""))
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
            return self._shared.clear_range(range_name, spreadsheet_id)

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

        values_to_write = []
        if include_headers and headers:
            values_to_write.append(headers)
        values_to_write.extend(data)

        range_name = f"'{tab_name}'!{starting_cell}"
        success = self._shared.write_range(range_name, values_to_write, spreadsheet_id)
        return success, len(data) if success else 0

    def apply_category_alignment(
        self,
        spreadsheet_id: str,
        tab_name: str,
        starting_cell: str,
        row_depths: List[int],
        row_labels: List[str],
        include_headers: bool = False,
    ) -> bool:
        """Apply horizontal alignment to the category column based on depth.

        Depth 0-1 and any row containing "Total": LEFT alignment.
        Depth 2+: RIGHT alignment.
        Only touches the horizontalAlignment property of the category column —
        no other formatting (font, color, borders, etc.) is modified.

        Args:
            spreadsheet_id: Google Sheet ID.
            tab_name: Tab name.
            starting_cell: Cell where data starts (e.g. "B8").
            row_depths: List of depth values per data row.
            include_headers: Whether a header row was written before data.
        """
        if not row_depths:
            return True

        service = self._shared.service
        if not service:
            return False

        # Parse starting cell to get column and row
        match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
        if not match:
            return False
        col_letter = match.group(1)
        start_row = int(match.group(2))
        col_index = 0
        for ch in col_letter:
            col_index = col_index * 26 + (ord(ch) - ord("A"))

        # Offset for header row
        data_start_row = start_row + (1 if include_headers else 0)

        # Get the sheet's GID
        try:
            meta = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields="sheets.properties",
            ).execute()
            sheet_id = None
            for sheet in meta.get("sheets", []):
                if sheet["properties"]["title"] == tab_name:
                    sheet_id = sheet["properties"]["sheetId"]
                    break
            if sheet_id is None:
                return False
        except Exception:
            return False

        def _get_alignment(idx: int) -> str:
            """LEFT for depth 0-1 or any Total row; RIGHT for depth 2+."""
            if row_depths[idx] <= 1:
                return "LEFT"
            if idx < len(row_labels) and "total" in row_labels[idx].lower():
                return "LEFT"
            return "RIGHT"

        # Build batch requests — group consecutive rows with same alignment
        requests = []
        i = 0
        while i < len(row_depths):
            align = _get_alignment(i)
            j = i + 1
            while j < len(row_depths):
                if _get_alignment(j) != align:
                    break
                j += 1

            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row + i - 1,
                        "endRowIndex": data_start_row + j - 1,
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": align,
                        }
                    },
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            })
            i = j

        if requests:
            try:
                logger.info(f"Applying alignment to {tab_name}: {len(requests)} ranges")
                for req in requests:
                    r = req["repeatCell"]["range"]
                    a = req["repeatCell"]["cell"]["userEnteredFormat"]["horizontalAlignment"]
                    logger.debug(f"  align {a}: rows {r['startRowIndex']}-{r['endRowIndex']} col {r['startColumnIndex']}")
                result = service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": requests},
                ).execute()
                logger.info(f"Alignment applied to {tab_name}: {len(result.get('replies', []))} replies")
            except Exception as e:
                logger.error(f"Failed to apply alignment to {tab_name}: {e}")
                return False

        return True

    def update_processed_date(
        self,
        spreadsheet_id: str,
        row_index: int,
        processed_col: str = "K",
        tab_name: str = None,
    ) -> bool:
        """
        Update the processed date for a report config row.

        Args:
            spreadsheet_id: Google Sheet ID
            row_index: Row number in the tab
            processed_col: Column for processed date
            tab_name: Tab name (defaults to TOPROCESS_TAB_NAME)

        Returns:
            True if successful
        """
        tab = tab_name or TOPROCESS_TAB_NAME
        timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        range_name = f"'{tab}'!{processed_col}{row_index}"
        return self._shared.write_range(
            range_name, [[timestamp]], spreadsheet_id, value_input_option="RAW"
        )

    def read_cell(
        self,
        spreadsheet_id: str,
        tab_name: str,
        cell: str,
    ) -> Optional[str]:
        """Read a single cell value. Returns None if empty or on error."""
        value = self._shared.read_cell(tab_name, cell, spreadsheet_id)
        if value == "":
            return None
        return str(value) if value is not None else None

    def read_column(
        self,
        spreadsheet_id: str,
        tab_name: str,
        column: str,
        start_row: int = 1,
    ) -> list[str]:
        """Read all values in a column from start_row down. Returns list of strings."""
        range_name = f"'{tab_name}'!{column}{start_row}:{column}"
        rows = self._shared.read_range(range_name, spreadsheet_id)
        return [row[0] if row else "" for row in rows]

    def write_cell(
        self,
        spreadsheet_id: str,
        tab_name: str,
        cell: str,
        value: Any,
    ) -> bool:
        """Write a single value to a specific cell."""
        return self._shared.write_cell(tab_name, cell, value, spreadsheet_id)

    def get_existing_row_count(
        self,
        spreadsheet_id: str,
        tab_name: str,
        starting_cell: str,
    ) -> int:
        """
        Count the total rows of data in a tab from starting_cell to the last
        non-empty row. Includes blank separator rows in the count (financial
        reports have section breaks).

        Returns:
            Total row span from starting_cell to last non-empty row (0 if empty)
        """
        try:
            match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
            if not match:
                return 0

            col = match.group(1)
            start_row = int(match.group(2))

            # Read the first column of data
            range_name = f"'{tab_name}'!{col}{start_row}:{col}"
            values = self._shared.read_range(range_name, spreadsheet_id)
            if not values:
                return 0

            # Find the last non-empty row (index from end)
            last_non_empty = -1
            for i in range(len(values) - 1, -1, -1):
                if values[i] and values[i][0] != "":
                    last_non_empty = i
                    break

            if last_non_empty < 0:
                return 0

            # Total row span = index of last non-empty row + 1
            return last_non_empty + 1

        except HttpError as e:
            logger.error(f"Failed to count rows in {tab_name}: {e}")
            return 0

    def find_label_row(
        self,
        spreadsheet_id: str,
        tab_name: str,
        starting_cell: str,
        label: str,
    ) -> Optional[int]:
        """
        Find which row a label appears on in the sheet.

        Searches the starting column from starting_cell downward for an exact
        match of `label`.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            starting_cell: Cell reference (e.g. "A5", "B8")
            label: The text to search for

        Returns:
            1-based row number where the label was found, or None
        """
        try:
            match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
            if not match:
                return None

            col = match.group(1)
            start_row = int(match.group(2))

            range_name = f"'{tab_name}'!{col}{start_row}:{col}"
            values = self._shared.read_range(range_name, spreadsheet_id)

            for i, row in enumerate(values):
                if row and row[0].strip() == label.strip():
                    return start_row + i

            return None

        except HttpError as e:
            logger.error(f"Failed to search for label in {tab_name}: {e}")
            return None

    def get_tab_sheet_id(
        self,
        spreadsheet_id: str,
        tab_name: str,
    ) -> Optional[int]:
        """Get the internal sheet ID for a tab (needed for insertDimension)."""
        return self._shared.get_sheet_id(tab_name, spreadsheet_id)

    def insert_rows(
        self,
        spreadsheet_id: str,
        tab_name: str,
        row_index: int,
        num_rows: int,
    ) -> bool:
        """
        Insert blank rows at a specific position in a tab.

        This shifts existing rows (and their formulas) down, keeping
        cross-tab references intact.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            row_index: 0-based row index where to insert
            num_rows: Number of blank rows to insert

        Returns:
            True if successful
        """
        try:
            sheet_id = self.get_tab_sheet_id(spreadsheet_id, tab_name)
            if sheet_id is None:
                logger.error(f"Could not find sheet ID for {tab_name}")
                return False

            return self._shared.batch_update(
                [{
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_index,
                            "endIndex": row_index + num_rows,
                        },
                        "inheritFromBefore": True,
                    }
                }],
                spreadsheet_id,
            )

        except HttpError as e:
            logger.error(f"Failed to insert rows in {tab_name}: {e}")
            return False

    def copy_row_down(
        self,
        spreadsheet_id: str,
        tab_name: str,
        source_row: int,
        num_copies: int,
    ) -> bool:
        """Copy a row (formulas, formats, values — everything) N times below it.

        Uses insertDimension to create blank rows, then copyPaste to replicate
        the source row into each new row.

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            source_row: 1-based row number to copy
            num_copies: Number of copies to create below source_row

        Returns:
            True if successful
        """
        try:
            sheet_id = self.get_tab_sheet_id(spreadsheet_id, tab_name)
            if sheet_id is None:
                logger.error(f"Could not find sheet ID for {tab_name}")
                return False

            # Get the column count so we copy the full width
            try:
                meta = self.sheets.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets.properties",
                ).execute()
                col_count = 26  # default
                for sheet in meta.get("sheets", []):
                    if sheet["properties"]["sheetId"] == sheet_id:
                        col_count = sheet["properties"].get("gridProperties", {}).get("columnCount", 26)
                        break
            except HttpError:
                col_count = 26

            # 0-based indices for the source row
            src_start = source_row - 1
            insert_at = source_row  # insert below the source row (0-based)

            return self._shared.batch_update(
                [
                    # Step 1: Insert blank rows below the source
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": insert_at,
                                "endIndex": insert_at + num_copies,
                            },
                            "inheritFromBefore": True,
                        }
                    },
                    # Step 2: Copy the source row into all new rows
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": sheet_id,
                                "startRowIndex": src_start,
                                "endRowIndex": src_start + 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": col_count,
                            },
                            "destination": {
                                "sheetId": sheet_id,
                                "startRowIndex": insert_at,
                                "endRowIndex": insert_at + num_copies,
                                "startColumnIndex": 0,
                                "endColumnIndex": col_count,
                            },
                            "pasteType": "PASTE_NORMAL",
                            "pasteOrientation": "NORMAL",
                        }
                    },
                ],
                spreadsheet_id,
            )

        except HttpError as e:
            logger.error(f"Failed to copy row in {tab_name}: {e}")
            return False

    def read_range(
        self,
        spreadsheet_id: str,
        tab_name: str,
        range_str: str,
    ) -> list[list[str]]:
        """Read a rectangular range. Returns list of rows (each a list of cell values).

        Args:
            spreadsheet_id: Google Sheet ID
            tab_name: Tab name
            range_str: A1-notation range (e.g. "A2:E" or "C2:C")

        Returns:
            List of rows, each row a list of string values. Empty cells are "".
        """
        full_range = f"'{tab_name}'!{range_str}"
        return self._shared.read_range(full_range, spreadsheet_id)
