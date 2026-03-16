"""Report processor for QBO ToProcess.

Two-phase processing:
  Phase 1 (Download): Fetch all reports from QBO into memory
  Phase 2 (Insert):   Write all downloaded data to Google Sheets
"""

from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple

from services.qbo_service import QBOService
from services.sheets_service import SheetsService
from config import QBO_REPORTS
from logger_setup import get_logger

logger = get_logger()

# Report types that use Chart of Accounts (need zero-balance account injection)
COA_REPORT_TYPES = {"ProfitAndLoss", "BalanceSheet"}
# Report types that use Customers or Items (need entity injection)
CUSTOMER_REPORT_TYPES = {"CustomerSales"}
ITEM_REPORT_TYPES = {"ItemSales"}


class DownloadedReport:
    """Holds a downloaded and parsed QBO report ready for insertion."""

    def __init__(
        self,
        config: Dict[str, Any],
        rows: List[List[Any]],
        headers: List[str],
        year: int,
    ):
        self.config = config
        self.rows = rows
        self.headers = headers
        self.year = year
        self.report_name = config.get("qbo_report", "Unknown")
        self.dest_tab = config.get("dest_tab_name", "Unknown")

    @property
    def key(self) -> str:
        return f"{self.report_name} -> {self.dest_tab}"

    @property
    def row_count(self) -> int:
        return len(self.rows)


class ReportProcessor:
    """Processes reports from QBO to Google Sheets based on ToProcess config."""

    def __init__(self, qbo_service: QBOService, sheets_service: SheetsService):
        self.qbo = qbo_service
        self.sheets = sheets_service

    # ── Phase 1: Download all reports from QBO ──

    def download_all_reports(
        self,
        configs: List[Dict[str, Any]],
        year: int,
    ) -> Tuple[List[DownloadedReport], List[Dict[str, Any]]]:
        """
        Download all reports from QBO without writing anything.

        Args:
            configs: Report configurations from ToProcess tab
            year: Year for the reports

        Returns:
            Tuple of (downloaded_reports, download_errors)
        """
        downloaded = []
        errors = []

        logger.info(f"\n{'=' * 40}")
        logger.info(f"PHASE 1: Downloading {len(configs)} reports from QBO")
        logger.info(f"{'=' * 40}")

        # Pre-fetch reference data for entity injection
        coa_accounts = None
        customers = None
        items = None

        report_types_needed = {
            QBO_REPORTS.get(c.get("qbo_report", ""), "") for c in configs
        }

        if report_types_needed & COA_REPORT_TYPES:
            coa_accounts = self.qbo.get_accounts()
            if coa_accounts:
                logger.info(f"Loaded {len(coa_accounts)} accounts from Chart of Accounts")

        if report_types_needed & CUSTOMER_REPORT_TYPES:
            customers = self.qbo.get_customers()
            if customers:
                logger.info(f"Loaded {len(customers)} customers")

        if report_types_needed & ITEM_REPORT_TYPES:
            items = self.qbo.get_items()
            if items:
                logger.info(f"Loaded {len(items)} products/services")

        for config in configs:
            report_name = config.get("qbo_report", "Unknown")
            dest_tab = config.get("dest_tab_name", "Unknown")
            key = f"{report_name} -> {dest_tab}"

            try:
                date_range = config.get("date_range", "This Year")
                # "ALL" = all dates; "Last Year" = prior year; default = this year
                if "all" in date_range.lower():
                    dr = "ALL"
                elif "last" in date_range.lower():
                    dr = "LAST"
                else:
                    dr = "Year"

                report_data = self.qbo.get_report(
                    report_name=report_name,
                    year=year,
                    display=config.get("report_display", "Monthly"),
                    basis=config.get("report_basis", "Accrual"),
                    full_year=True,
                    date_range=dr,
                )

                if not report_data:
                    errors.append({
                        "key": key,
                        "error": "Failed to fetch report from QBO",
                    })
                    logger.error(f"  \u2717 {key}: Failed to fetch from QBO")
                    continue

                # Inject zero-balance rows for missing accounts/customers/items
                qbo_endpoint = QBO_REPORTS.get(report_name, "")
                if coa_accounts and qbo_endpoint in COA_REPORT_TYPES:
                    self.qbo.inject_missing_accounts(report_data, coa_accounts)
                elif customers and qbo_endpoint in CUSTOMER_REPORT_TYPES:
                    self.qbo.inject_missing_entities(report_data, customers, "DisplayName")
                elif items and qbo_endpoint in ITEM_REPORT_TYPES:
                    self.qbo.inject_missing_entities(report_data, items, "Name")

                rows, headers = self.qbo.parse_report_to_rows(
                    report_data,
                    row_max=config.get("row_max", "*"),
                    col_max=config.get("col_max", "*"),
                )

                report = DownloadedReport(config, rows, headers, year)
                downloaded.append(report)

                if rows:
                    logger.info(f"  \u2713 {key}: {len(rows)} rows downloaded")
                else:
                    logger.warning(f"  \u2713 {key}: 0 rows (empty report)")

            except Exception as e:
                errors.append({"key": key, "error": str(e)})
                logger.error(f"  \u2717 {key}: {e}")

        logger.info(f"\nDownload complete: {len(downloaded)} succeeded, {len(errors)} failed")
        return downloaded, errors

    # ── Phase 2: Insert all downloaded reports into Sheets ──

    def insert_all_reports(
        self,
        downloaded: List[DownloadedReport],
        toprocess_sheet_id: str,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Write all downloaded reports to Google Sheets.

        Args:
            downloaded: List of downloaded reports from phase 1
            toprocess_sheet_id: Sheet ID for updating processed dates

        Returns:
            Dict mapping report keys to results
        """
        results = {}

        logger.info(f"\n{'=' * 40}")
        logger.info(f"PHASE 2: Inserting {len(downloaded)} reports into Sheets")
        logger.info(f"{'=' * 40}")

        for report in downloaded:
            config = report.config
            key = report.key

            try:
                dest_sheet_id = config.get("dest_sheet_id", "")
                dest_tab_name = config.get("dest_tab_name", "")
                starting_cell = config.get("starting_cell", "A1")
                temp_tab = config.get("temp_tab", "")
                new_tab_name_format = config.get("new_tab_name_format", "")
                row_index = config.get("row_index", 0)

                # Handle special AR process (create new tab from template)
                if temp_tab and new_tab_name_format:
                    if new_tab_name_format.lower() == "yyyy-mm-dd":
                        # Use config year + today's month/day
                        today = date.today()
                        actual_tab_name = f"{report.year}-{today.strftime('%m-%d')}"
                    else:
                        actual_tab_name = new_tab_name_format

                    # Parse tab index position
                    # Config value is the desired position (e.g. 3 = 3rd tab)
                    # Sheets API index is 0-based, so subtract 1
                    tab_index_str = config.get("tab_index", "")
                    tab_index = None
                    if tab_index_str:
                        try:
                            tab_index = max(0, int(tab_index_str) - 1)
                        except ValueError:
                            pass

                    if not self.sheets.duplicate_tab(
                        dest_sheet_id, temp_tab, actual_tab_name, tab_index=tab_index,
                    ):
                        results[key] = {
                            "status": "error",
                            "rows": 0,
                            "error": f"Failed to duplicate {temp_tab} to {actual_tab_name}",
                        }
                        logger.error(f"  \u2717 {key}: Failed to duplicate template tab")
                        continue

                    dest_tab_name = actual_tab_name

                    # Write report date to A1 (static text, not formula)
                    report_date = date.today().strftime("%m/%d/%Y")
                    self.sheets.write_cell(
                        dest_sheet_id, dest_tab_name, "A1", report_date,
                    )
                    logger.info(f"    Wrote report date {report_date} to A1")

                if not report.rows:
                    results[key] = {"status": "success", "rows": 0, "error": ""}
                    logger.info(f"  \u2713 {key}: 0 rows (empty report, nothing to write)")
                    continue

                # ── Row stability check ──
                # Compare new row count vs existing rows in the sheet.
                # Include header row in the count for both sides.
                new_total_rows = len(report.rows) + (1 if report.headers else 0)
                is_new_tab = bool(temp_tab and new_tab_name_format)

                if not is_new_tab:
                    existing_rows = self.sheets.get_existing_row_count(
                        dest_sheet_id, dest_tab_name, starting_cell,
                    )

                    if existing_rows > 0:
                        if new_total_rows < existing_rows:
                            # FEWER rows than expected — ERROR, do not write
                            results[key] = {
                                "status": "error",
                                "rows": 0,
                                "error": (
                                    f"ROW MISMATCH: new data has {new_total_rows} rows "
                                    f"but sheet has {existing_rows}. "
                                    f"Missing {existing_rows - new_total_rows} rows. "
                                    f"Skipped to protect sheet integrity."
                                ),
                            }
                            logger.error(
                                f"  \u2717 {key}: ROW MISMATCH — "
                                f"new={new_total_rows}, existing={existing_rows}. "
                                f"SKIPPED — check for deleted accounts."
                            )
                            continue

                        elif new_total_rows > existing_rows:
                            # MORE rows — need to insert blank rows to shift formulas
                            extra = new_total_rows - existing_rows
                            logger.warning(
                                f"  \u26a0 {key}: {extra} new row(s) detected "
                                f"(existing={existing_rows}, new={new_total_rows})"
                            )

                            # Parse starting cell to get insertion point
                            import re
                            match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
                            if match:
                                start_row = int(match.group(2))
                                # Insert before the last existing row (0-based index)
                                insert_at = start_row + existing_rows - 1
                                if not self.sheets.insert_rows(
                                    dest_sheet_id, dest_tab_name,
                                    insert_at, extra,
                                ):
                                    results[key] = {
                                        "status": "error",
                                        "rows": 0,
                                        "error": f"Failed to insert {extra} new rows",
                                    }
                                    logger.error(f"  \u2717 {key}: Failed to insert rows")
                                    continue

                            # Flag for post-write verification
                            results[f"_row_change_{key}"] = {
                                "status": "row_change",
                                "rows_added": extra,
                                "tab": dest_tab_name,
                                "sheet_id": dest_sheet_id,
                            }

                # Write data to destination (overwrite in place — do NOT clear,
                # as other columns may contain formulas)
                # For template-based tabs (AR), don't write headers — the
                # template already has formatted labels
                write_headers = not is_new_tab
                success, rows_written = self.sheets.write_data(
                    spreadsheet_id=dest_sheet_id,
                    tab_name=dest_tab_name,
                    starting_cell=starting_cell,
                    data=report.rows,
                    include_headers=write_headers,
                    headers=report.headers,
                )

                if not success:
                    results[key] = {
                        "status": "error",
                        "rows": 0,
                        "error": "Failed to write data to sheet",
                    }
                    logger.error(f"  \u2717 {key}: Failed to write to sheet")
                    continue

                # Update processed date in ToProcess
                if row_index > 0:
                    self.sheets.update_processed_date(toprocess_sheet_id, row_index)

                results[key] = {
                    "status": "success",
                    "rows": rows_written,
                    "error": "",
                }
                logger.info(f"  \u2713 {key}: {rows_written} rows written")

            except Exception as e:
                results[key] = {"status": "error", "rows": 0, "error": str(e)}
                logger.error(f"  \u2717 {key}: {e}")

        return results

    # ── Combined: preflight → download → insert ──

    def process_all_reports(
        self,
        toprocess_sheet_id: str,
        configs: Optional[List[Dict[str, Any]]] = None,
        year: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process all reports using two-phase approach.

        If configs/year are provided (from preflight), uses those directly.
        Otherwise reads ToProcess config from the sheet.

        Args:
            toprocess_sheet_id: Google Sheet ID containing ToProcess tab
            configs: Pre-loaded configs from preflight (optional)
            year: Pre-loaded year from preflight (optional)

        Returns:
            Dict mapping report names to results
        """
        # If not provided by preflight, read config now
        if configs is None or year is None:
            year, configs = self.sheets.read_toprocess_config(toprocess_sheet_id)
            if year is None:
                logger.error("Failed to read ToProcess configuration")
                return {"_error": {"status": "error", "error": "Failed to read configuration"}}

        # Phase 1: Download
        downloaded, download_errors = self.download_all_reports(configs, year)

        if download_errors and not downloaded:
            logger.error("All downloads failed — aborting insert phase")
            results = {}
            for err in download_errors:
                results[err["key"]] = {"status": "error", "rows": 0, "error": err["error"]}
            return results

        if download_errors:
            logger.warning(
                f"{len(download_errors)} download(s) failed — "
                f"proceeding with {len(downloaded)} successful downloads"
            )

        # Phase 2: Insert
        results = self.insert_all_reports(downloaded, toprocess_sheet_id)

        # Add download errors to results
        for err in download_errors:
            results[err["key"]] = {"status": "error", "rows": 0, "error": err["error"]}

        return results
