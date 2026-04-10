"""Report processor for FinancialSysUpdate.

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

import re

logger = get_logger()

# Report types that use Chart of Accounts (need zero-balance account injection)
COA_REPORT_TYPES = {"ProfitAndLoss", "BalanceSheet"}
# Report types that use Customers or Items (need entity injection)
CUSTOMER_REPORT_TYPES = {"CustomerSales"}
ITEM_REPORT_TYPES = {"ItemSales"}


def _resolve_item_filter(qbo: 'QBOService', filter_str: str) -> str:
    """Resolve a filter expression to QBO item IDs.

    Supported formats:
        contains:text     — items whose name contains 'text'
        exact:text        — items whose name exactly matches 'text'

    Returns:
        Comma-separated item IDs for the QBO API, or empty string.
    """
    if ":" not in filter_str:
        logger.warning(f"Invalid filter format '{filter_str}' — expected 'contains:text'")
        return ""

    mode, value = filter_str.split(":", 1)
    mode = mode.strip().lower()
    value = value.strip()

    if not value:
        return ""

    items = qbo.get_items()
    if not items:
        logger.warning("No items returned from QBO for filter resolution")
        return ""

    matched = []
    for item in items:
        name = item.get("Name", "")
        if mode == "contains" and value.lower() in name.lower():
            matched.append(item)
        elif mode == "exact" and name.lower() == value.lower():
            matched.append(item)

    if matched:
        ids = ",".join(i["Id"] for i in matched)
        names = ", ".join(i["Name"] for i in matched)
        logger.info(f"Filter '{filter_str}' matched {len(matched)} items: {names}")
        return ids

    logger.warning(f"Filter '{filter_str}' matched no items")
    return ""


def _sort_rows(
    rows: List[List[Any]],
    row_depths: List[int],
    sort_spec: str,
) -> Tuple[List[List[Any]], List[int]]:
    """Sort data rows based on a sort specification.

    Supported formats:
        Total Desc  — sort by last numeric column, descending
        Total Asc   — sort by last numeric column, ascending
        Name Asc    — sort by column A (name), ascending
        Name Desc   — sort by column A (name), descending

    TOTAL and section header rows are preserved at their positions.

    Returns:
        Sorted (rows, row_depths)
    """
    parts = sort_spec.strip().split()
    if len(parts) < 2:
        return rows, row_depths

    field = parts[0].lower()
    direction = parts[1].lower()
    reverse = direction == "desc"

    def get_total_value(row):
        for cell in reversed(row):
            val = str(cell).strip().replace(",", "")
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
        return 0.0

    def get_name_value(row):
        return str(row[0]).strip().lower() if row else ""

    sort_key = get_total_value if field == "total" else get_name_value

    # Separate TOTAL row from data rows
    data_rows = []
    data_depths = []
    total_row = None
    total_depth = None

    for i, row in enumerate(rows):
        label = str(row[0]).strip() if row else ""
        if label.upper() == "TOTAL":
            total_row = row
            total_depth = row_depths[i] if i < len(row_depths) else 0
        else:
            data_rows.append(row)
            data_depths.append(row_depths[i] if i < len(row_depths) else 0)

    paired = list(zip(data_rows, data_depths))
    paired.sort(key=lambda x: sort_key(x[0]), reverse=reverse)

    sorted_rows = [r for r, _ in paired]
    sorted_depths = [d for _, d in paired]

    if total_row is not None:
        sorted_rows.append(total_row)
        sorted_depths.append(total_depth)

    logger.info(f"Sorted {len(data_rows)} rows by {field} {direction}")
    return sorted_rows, sorted_depths


def _remove_zero_rows(
    rows: List[List[Any]],
    row_depths: List[int],
) -> Tuple[List[List[Any]], List[int]]:
    """Remove rows where all numeric columns are zero or empty.

    Preserves TOTAL and section header rows.

    Returns:
        Filtered (rows, row_depths)
    """
    filtered_rows = []
    filtered_depths = []
    removed = 0

    for i, row in enumerate(rows):
        label = str(row[0]).strip() if row else ""
        depth = row_depths[i] if i < len(row_depths) else 0

        # Always keep TOTAL and section headers
        if label.upper() == "TOTAL" or depth == 0:
            filtered_rows.append(row)
            filtered_depths.append(depth)
            continue

        # Check if any numeric column has a non-zero value
        has_value = False
        for cell in row[1:]:
            val = str(cell).strip().replace(",", "")
            try:
                if float(val) != 0:
                    has_value = True
                    break
            except (ValueError, TypeError):
                continue

        if has_value:
            filtered_rows.append(row)
            filtered_depths.append(depth)
        else:
            removed += 1

    if removed:
        logger.info(f"Removed {removed} zero-revenue rows")
    return filtered_rows, filtered_depths


def _insert_pct_change(
    rows: List[List[Any]],
    headers: List[str],
) -> Tuple[List[List[Any]], List[str]]:
    """Insert % Change columns after each PY column in comparison reports.

    QBO returns [Name, Curr1, PY1, Curr2, PY2, ..., CurrT, PYT]
    We produce [Name, Curr1, PY1, %Chg1, Curr2, PY2, %Chg2, ..., CurrT, PYT, %ChgT]

    The % Change is calculated as (Current - PY) / PY * 100, formatted
    as a percentage string (e.g. "-82.13%").
    """
    if not rows:
        return rows, headers

    # Data columns come in pairs (Current, PY) after the name column
    # Number of periods = (total cols - 1) / 2
    num_data_cols = len(rows[0]) - 1
    if num_data_cols < 2 or num_data_cols % 2 != 0:
        logger.warning(f"Unexpected column count for comparison: {len(rows[0])}")
        return rows, headers

    num_periods = num_data_cols // 2

    new_rows = []
    for row in rows:
        new_row = [row[0]]  # Name column
        for p in range(num_periods):
            curr_idx = 1 + p * 2
            py_idx = 2 + p * 2
            curr_val = row[curr_idx] if curr_idx < len(row) else ""
            py_val = row[py_idx] if py_idx < len(row) else ""

            new_row.append(curr_val)
            new_row.append(py_val)

            # Calculate % Change
            try:
                curr_num = float(str(curr_val).replace(",", "").replace("$", "")) if curr_val else 0
                py_num = float(str(py_val).replace(",", "").replace("$", "")) if py_val else 0
                if py_num != 0:
                    pct = ((curr_num - py_num) / abs(py_num)) * 100
                    new_row.append(f"{pct:.2f}%")
                elif curr_num != 0:
                    new_row.append("")  # Can't calculate % change from zero
                else:
                    new_row.append("")
            except (ValueError, TypeError):
                new_row.append("")

        new_rows.append(new_row)

    # Expand headers: insert "% Change (PY)" after each PY header
    new_headers = [headers[0]] if headers else [""]
    for p in range(num_periods):
        h_idx = 1 + p  # Original headers are just period names
        if h_idx < len(headers):
            new_headers.append(headers[h_idx])
        else:
            new_headers.append("")
        new_headers.append("")  # PY (no separate header in QBO)
        new_headers.append("% Change (PY)")

    logger.info(f"Inserted % Change columns: {len(rows[0])} -> {len(new_rows[0])} cols "
                f"({num_periods} periods)")
    return new_rows, new_headers


class DownloadedReport:
    """Holds a downloaded and parsed QBO report ready for insertion."""

    def __init__(
        self,
        config: Dict[str, Any],
        rows: List[List[Any]],
        headers: List[str],
        year: int,
        row_depths: List[int] = None,
    ):
        self.config = config
        self.rows = rows
        self.headers = headers
        self.year = year
        self.row_depths = row_depths or []
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

        # Pre-fetch Chart of Accounts for P&L/Balance Sheet injection
        coa_accounts = None

        report_types_needed = {
            QBO_REPORTS.get(c.get("qbo_report", ""), "") for c in configs
        }

        if report_types_needed & COA_REPORT_TYPES:
            coa_accounts = self.qbo.get_accounts()
            if coa_accounts:
                logger.info(f"Loaded {len(coa_accounts)} accounts from Chart of Accounts")

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

                # Resolve API-level item filter if configured
                row_filter = config.get("filter", "")
                extra_params = {}
                if row_filter:
                    item_ids = _resolve_item_filter(self.qbo, row_filter)
                    if item_ids:
                        extra_params["item"] = item_ids

                # Comparison reports need prior year sub-columns from QBO
                is_comparison = "comparison" in report_name.lower()
                if is_comparison:
                    extra_params["subcol_py"] = "true"

                report_data = self.qbo.get_report(
                    report_name=report_name,
                    year=year,
                    display=config.get("report_display", "Monthly"),
                    basis=config.get("report_basis", "Accrual"),
                    full_year=True,
                    date_range=dr,
                    extra_params=extra_params,
                )

                if not report_data:
                    errors.append({
                        "key": key,
                        "error": "Failed to fetch report from QBO",
                    })
                    logger.error(f"  \u2717 {key}: Failed to fetch from QBO")
                    continue

                # Inject zero-balance rows for P&L and Balance Sheet only.
                # Sales/Customer/Item reports should show active data only.
                # Skip injection entirely when a filter is active.
                qbo_endpoint = QBO_REPORTS.get(report_name, "")
                if not row_filter and qbo_endpoint in COA_REPORT_TYPES:
                    if coa_accounts:
                        self.qbo.inject_missing_accounts(report_data, coa_accounts)

                rows, headers, row_depths = self.qbo.parse_report_to_rows(
                    report_data,
                    row_max=config.get("row_max", "*"),
                    col_max=config.get("col_max", "*"),
                )

                # Comparison reports: insert calculated % Change column
                # after each PY column. QBO gives [Current, PY] per period,
                # we need [Current, PY, % Change] to match Excel format.
                if is_comparison and rows:
                    rows, headers = _insert_pct_change(rows, headers)

                # For non-P&L/Balance Sheet reports: remove zero-revenue rows
                if qbo_endpoint not in COA_REPORT_TYPES and rows:
                    rows, row_depths = _remove_zero_rows(rows, row_depths)

                # Apply sorting from config, or default Name Asc for
                # non-P&L/Balance Sheet reports without explicit sort
                sort_spec = config.get("sort", "")
                if not sort_spec and qbo_endpoint not in COA_REPORT_TYPES:
                    sort_spec = "Name Asc"
                if sort_spec and rows:
                    rows, row_depths = _sort_rows(rows, row_depths, sort_spec)

                report = DownloadedReport(config, rows, headers, year, row_depths)
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
        reports_tab: str = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Write all downloaded reports to Google Sheets.

        Args:
            downloaded: List of downloaded reports from phase 1
            toprocess_sheet_id: Sheet ID for updating processed dates
            reports_tab: Tab name for processed date updates (MasterConfig)

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
                        today = date.today()
                        actual_tab_name = f"{report.year}-{today.strftime('%m-%d')}"
                    else:
                        actual_tab_name = new_tab_name_format

                    # Check if today's tab already exists
                    tab_exists = self.sheets.get_tab_id(
                        dest_sheet_id, actual_tab_name
                    ) is not None

                    if tab_exists:
                        # Tab already exists — update it with fresh data
                        logger.info(f"    Tab '{actual_tab_name}' already exists — updating")
                    else:
                        # Create new tab from template
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

                    # Write run timestamp to A1
                    run_timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
                    self.sheets.write_cell(
                        dest_sheet_id, dest_tab_name, "A1", run_timestamp,
                    )
                    logger.info(f"    Wrote run timestamp {run_timestamp} to A1")

                    # Write today's date to ARDashboard C — but only if
                    # today's date isn't already there
                    today_str = date.today().strftime("%-m/%-d/%Y")
                    today_str2 = date.today().strftime("%m/%d/%Y")
                    ar_dates = self.sheets.read_range(
                        dest_sheet_id, "ARDashboard", "C8:C",
                    )
                    if ar_dates:
                        # Check if today is already in column C
                        today_already = False
                        last_date_idx = -1
                        for idx, row in enumerate(ar_dates):
                            val = row[0].strip() if row and row[0] else ""
                            if val and val.upper() != "NO TAB":
                                last_date_idx = idx
                                if val == today_str or val == today_str2 or val.startswith(today_str2.split()[0]):
                                    today_already = True

                        if today_already:
                            logger.info(f"    ARDashboard already has today's date — skipping")
                        else:
                            ar_row_num = 8 + last_date_idx + 1
                            self.sheets.write_cell(
                                dest_sheet_id, "ARDashboard",
                                f"C{ar_row_num}", run_timestamp,
                            )
                            logger.info(f"    Wrote AR date to ARDashboard C{ar_row_num}")

                if not report.rows:
                    results[key] = {"status": "success", "rows": 0, "error": ""}
                    logger.info(f"  \u2713 {key}: 0 rows (empty report, nothing to write)")
                    continue

                # ── Row stability check (configurable per report) ──
                # When verify_last_row is enabled:
                # Find the last row label in QBO download, compare where it
                # would land vs where it currently sits in the sheet.
                #   BEFORE current position → ERROR (categories removed)
                #   AFTER current position → NOTICE (new categories, append ok)
                verify_last_row = config.get("verify_last_row", False)
                is_new_tab = bool(temp_tab and new_tab_name_format)

                if verify_last_row and not is_new_tab:
                    import re
                    match = re.match(r"([A-Z]+)(\d+)", starting_cell.upper())
                    start_row = int(match.group(2)) if match else 1

                    # Get the last row label from QBO download
                    last_qbo_label = None
                    for row in reversed(report.rows):
                        if row and str(row[0]).strip():
                            last_qbo_label = str(row[0]).strip()
                            break

                    if last_qbo_label:
                        # Where QBO would write this label
                        header_offset = 1 if report.headers else 0
                        new_last_row = start_row + header_offset + len(report.rows) - 1

                        # Where it currently sits in the sheet
                        existing_last_row = self.sheets.find_label_row(
                            dest_sheet_id, dest_tab_name, starting_cell,
                            last_qbo_label,
                        )

                        if existing_last_row is not None:
                            if new_last_row < existing_last_row:
                                # FEWER categories — ERROR, do not write
                                missing = existing_last_row - new_last_row
                                results[key] = {
                                    "status": "error",
                                    "rows": 0,
                                    "error": (
                                        f"ROW MISMATCH: '{last_qbo_label}' would land on "
                                        f"row {new_last_row} but is currently on row "
                                        f"{existing_last_row}. {missing} category row(s) "
                                        f"missing. Skipped to protect sheet integrity."
                                    ),
                                }
                                logger.error(
                                    f"  \u2717 {key}: ROW MISMATCH — "
                                    f"'{last_qbo_label}' new={new_last_row}, "
                                    f"existing={existing_last_row}. "
                                    f"SKIPPED — {missing} categories missing."
                                )
                                continue

                            elif new_last_row > existing_last_row:
                                # MORE categories — NOTICE, proceed (append)
                                extra = new_last_row - existing_last_row
                                logger.warning(
                                    f"  \u26a0 {key}: {extra} new category row(s) — "
                                    f"'{last_qbo_label}' moves from row "
                                    f"{existing_last_row} to {new_last_row}"
                                )

                                results[f"_row_change_{key}"] = {
                                    "status": "row_change",
                                    "rows_added": extra,
                                    "tab": dest_tab_name,
                                    "sheet_id": dest_sheet_id,
                                }

                # Write run timestamp to A1 for non-template tabs so the
                # sheet always shows when data was last refreshed.
                # (Template tabs already get the timestamp above.)
                if not is_new_tab:
                    run_timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
                    self.sheets.write_cell(
                        dest_sheet_id, dest_tab_name, "A1", run_timestamp,
                    )

                # Write data to destination (overwrite in place — do NOT clear,
                # as other columns may contain formulas)
                # Skip headers when:
                #   - Template tabs (AR) — template has formatted labels
                #   - Display=Total — header is just "Total", not useful
                #   - Sub-columns exist (data cols > header cols) — sheet
                #     already has a multi-row header layout
                display = config.get("report_display", "").lower()
                has_sub_cols = len(report.rows[0]) > len(report.headers) if report.rows and report.headers else False
                write_headers = not is_new_tab and display != "total" and not has_sub_cols
                success, rows_written = self.sheets.write_data(
                    spreadsheet_id=dest_sheet_id,
                    tab_name=dest_tab_name,
                    starting_cell=starting_cell,
                    data=report.rows,
                    include_headers=write_headers,
                    headers=report.headers,
                )

                if not success:
                    error_msg = (f"Failed to write data to "
                                 f"'{dest_tab_name}'!{starting_cell} "
                                 f"in sheet {dest_sheet_id}")
                    results[key] = {
                        "status": "error",
                        "rows": 0,
                        "error": error_msg,
                    }
                    logger.error(f"  \u2717 {key}: {error_msg}")
                    continue

                # Apply category alignment for P&L / Balance Sheet reports
                if report.row_depths:
                    labels = [str(r[0]) if r else "" for r in report.rows]
                    self.sheets.apply_category_alignment(
                        spreadsheet_id=dest_sheet_id,
                        tab_name=dest_tab_name,
                        starting_cell=starting_cell,
                        row_depths=report.row_depths,
                        row_labels=labels,
                        include_headers=write_headers,
                    )

                # Update processed date in ToProcess
                if row_index > 0:
                    self.sheets.update_processed_date(
                        toprocess_sheet_id, row_index, tab_name=reports_tab,
                    )

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
        reports_tab: str = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process all reports using two-phase approach.

        Args:
            toprocess_sheet_id: Google Sheet ID for processed date updates
            configs: Pre-loaded configs (from MasterConfig or preflight)
            year: Report year
            reports_tab: Tab name for processed date updates (MasterConfig)

        Returns:
            Dict mapping report names to results
        """
        if configs is None or year is None:
            logger.error("configs and year are required")
            return {"_error": {"status": "error", "error": "No configs provided"}}

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
        results = self.insert_all_reports(
            downloaded, toprocess_sheet_id, reports_tab=reports_tab,
        )

        # Add download errors to results
        for err in download_errors:
            results[err["key"]] = {"status": "error", "rows": 0, "error": err["error"]}

        return results
