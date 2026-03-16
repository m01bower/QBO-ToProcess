"""Pre-flight checks for QBO ToProcess.

Validates all resources are accessible before any data processing begins.
"""

from typing import Dict, List, Any, Tuple

from services.qbo_service import QBOService
from services.sheets_service import SheetsService
from logger_setup import get_logger

logger = get_logger()


class PreflightResult:
    """Collects preflight check results."""

    def __init__(self):
        self.checks: List[Dict[str, Any]] = []

    def add(self, name: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({"name": name, "passed": passed, "detail": detail})
        symbol = "\u2713" if passed else "\u2717"
        logger.info(f"  {symbol} {name}" + (f" — {detail}" if detail else ""))

    @property
    def all_passed(self) -> bool:
        return all(c["passed"] for c in self.checks)

    @property
    def failures(self) -> List[Dict[str, Any]]:
        return [c for c in self.checks if not c["passed"]]

    def summary(self) -> str:
        total = len(self.checks)
        passed = sum(1 for c in self.checks if c["passed"])
        failed = total - passed
        return f"{passed}/{total} checks passed" + (f", {failed} FAILED" if failed else "")


def run_preflight(
    qbo: QBOService,
    sheets: SheetsService,
    toprocess_sheet_id: str,
) -> Tuple[PreflightResult, List[Dict[str, Any]]]:
    """
    Run all pre-flight checks for a single client.

    Validates:
    1. QBO API connection (token valid, company reachable)
    2. Google Sheets authentication
    3. ToProcess sheet is accessible
    4. ToProcess config can be read and parsed
    5. Every destination sheet ID referenced in ToProcess is accessible
    6. Every destination tab exists in its target sheet
    7. For AR reports: template tab exists

    Args:
        qbo: Initialized QBOService (with loaded token)
        sheets: Initialized SheetsService (already authenticated)
        toprocess_sheet_id: The ToProcess Google Sheet ID

    Returns:
        Tuple of (PreflightResult, configs list).
        configs is only populated if checks passed far enough to read them.
    """
    result = PreflightResult()
    configs = []

    logger.info("Running pre-flight checks...")

    # 1. QBO connection
    qbo_ok = qbo.test_connection()
    result.add("QBO API connection", qbo_ok,
               "Token valid, company reachable" if qbo_ok else "Cannot reach QBO API")
    if not qbo_ok:
        # No point continuing if QBO is down
        return result, configs

    # 2. Google Sheets auth (already done by caller, but verify service works)
    sheets_ok = sheets.is_authenticated()
    result.add("Google Sheets authentication", sheets_ok)
    if not sheets_ok:
        return result, configs

    # 3. ToProcess sheet accessible
    tp_ok = sheets.verify_sheet_access(toprocess_sheet_id)
    result.add("ToProcess sheet accessible", tp_ok,
               toprocess_sheet_id if tp_ok else f"Cannot access {toprocess_sheet_id}")
    if not tp_ok:
        return result, configs

    # 4. Read and parse ToProcess config
    year, configs = sheets.read_toprocess_config(toprocess_sheet_id)
    config_ok = year is not None and len(configs) > 0
    result.add("ToProcess config readable", config_ok,
               f"{len(configs)} reports for year {year}" if config_ok
               else "Failed to read or no report configs found")
    if not config_ok:
        return result, configs

    # 5 & 6. Validate every destination sheet + tab
    # Group by dest_sheet_id to avoid redundant API calls
    # Skip tab check for configs with temp_tab (they create tabs dynamically)
    sheets_to_check: Dict[str, List[str]] = {}
    dynamic_tab_sheets: set = set()
    for cfg in configs:
        sid = cfg.get("dest_sheet_id", "")
        tab = cfg.get("dest_tab_name", "")
        if cfg.get("temp_tab"):
            # This config creates its tab dynamically — only check sheet access
            if sid:
                dynamic_tab_sheets.add(sid)
        elif sid:
            sheets_to_check.setdefault(sid, [])
            if tab and tab not in sheets_to_check[sid]:
                sheets_to_check[sid].append(tab)

    # Also ensure dynamic-tab sheets get access-checked
    for sid in dynamic_tab_sheets:
        if sid not in sheets_to_check:
            sheets_to_check[sid] = []

    for sheet_id, tabs_needed in sheets_to_check.items():
        # Check sheet access
        access_ok = sheets.verify_sheet_access(sheet_id)
        result.add(f"Destination sheet accessible: {sheet_id[:20]}...", access_ok)

        if access_ok and tabs_needed:
            # Get all tab names in this sheet
            existing_tabs = _get_all_tabs(sheets, sheet_id)
            for tab_name in tabs_needed:
                # For AR reports with new_tab_name_format, the tab will be
                # created dynamically — just check the template tab exists
                tab_exists = tab_name in existing_tabs
                result.add(f"  Tab exists: '{tab_name}'", tab_exists)

    # 7. Check template tabs for AR (duplicate-tab) configs
    for cfg in configs:
        temp_tab = cfg.get("temp_tab", "")
        if temp_tab:
            dest_sid = cfg.get("dest_sheet_id", "")
            if dest_sid:
                existing_tabs = _get_all_tabs(sheets, dest_sid)
                template_ok = temp_tab in existing_tabs
                result.add(f"  Template tab exists: '{temp_tab}'", template_ok,
                           f"in sheet {dest_sid[:20]}...")

    logger.info(f"Pre-flight: {result.summary()}")
    return result, configs


def _get_all_tabs(sheets: SheetsService, spreadsheet_id: str) -> set:
    """Get all tab names in a spreadsheet."""
    try:
        meta = sheets.sheets.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.title",
        ).execute()
        return {s["properties"]["title"] for s in meta.get("sheets", [])}
    except Exception as e:
        logger.warning(f"Could not list tabs for {spreadsheet_id}: {e}")
        return set()
