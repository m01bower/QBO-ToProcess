"""Report processor for QBO ToProcess."""

from datetime import datetime, date
from typing import Dict, Any, List, Optional

from services.qbo_service import QBOService
from services.sheets_service import SheetsService
from logger_setup import get_logger

logger = get_logger()


class ReportProcessor:
    """Processes reports from QBO to Google Sheets based on ToProcess config."""

    def __init__(self, qbo_service: QBOService, sheets_service: SheetsService):
        """
        Initialize the report processor.

        Args:
            qbo_service: Authenticated QBO service
            sheets_service: Authenticated Sheets service
        """
        self.qbo = qbo_service
        self.sheets = sheets_service

    def process_report(
        self,
        config: Dict[str, Any],
        year: int,
        toprocess_sheet_id: str,
    ) -> Dict[str, Any]:
        """
        Process a single report based on ToProcess configuration.

        Args:
            config: Report configuration from ToProcess tab
            year: Year for the report
            toprocess_sheet_id: Sheet ID containing ToProcess tab

        Returns:
            Result dict with status, rows, error
        """
        report_name = config.get("qbo_report", "")
        dest_sheet_id = config.get("dest_sheet_id", "")
        dest_tab_name = config.get("dest_tab_name", "")
        starting_cell = config.get("starting_cell", "A1")
        row_max = config.get("row_max", "*")
        col_max = config.get("col_max", "*")
        temp_tab = config.get("temp_tab", "")
        new_tab_name_format = config.get("new_tab_name_format", "")
        report_display = config.get("report_display", "Monthly")
        report_basis = config.get("report_basis", "Accrual")
        row_index = config.get("row_index", 0)

        logger.info(f"Processing: {report_name} -> {dest_tab_name}")

        try:
            # Handle special AR process (create new tab from template)
            if temp_tab and new_tab_name_format:
                # Format the new tab name
                if new_tab_name_format.lower() == "yyyy-mm-dd":
                    actual_tab_name = date.today().strftime("%Y-%m-%d")
                else:
                    actual_tab_name = new_tab_name_format

                # Duplicate the template tab
                if not self.sheets.duplicate_tab(dest_sheet_id, temp_tab, actual_tab_name):
                    return {
                        "status": "error",
                        "rows": 0,
                        "error": f"Failed to duplicate {temp_tab} to {actual_tab_name}",
                    }

                dest_tab_name = actual_tab_name

            # Fetch report from QBO
            report_data = self.qbo.get_report(
                report_name=report_name,
                year=year,
                display=report_display,
                basis=report_basis,
            )

            if not report_data:
                return {
                    "status": "error",
                    "rows": 0,
                    "error": "Failed to fetch report from QBO",
                }

            # Parse report data
            rows, headers = self.qbo.parse_report_to_rows(
                report_data,
                row_max=row_max,
                col_max=col_max,
            )

            if not rows:
                logger.warning(f"No data returned for {report_name}")
                return {
                    "status": "success",
                    "rows": 0,
                    "error": "",
                }

            # Clear existing data in destination
            self.sheets.clear_tab_data(dest_sheet_id, dest_tab_name, starting_cell)

            # Write data to destination
            success, rows_written = self.sheets.write_data(
                spreadsheet_id=dest_sheet_id,
                tab_name=dest_tab_name,
                starting_cell=starting_cell,
                data=rows,
                include_headers=True,
                headers=headers,
            )

            if not success:
                return {
                    "status": "error",
                    "rows": 0,
                    "error": "Failed to write data to sheet",
                }

            # Update processed date in ToProcess
            if row_index > 0:
                self.sheets.update_processed_date(toprocess_sheet_id, row_index)

            return {
                "status": "success",
                "rows": rows_written,
                "error": "",
            }

        except Exception as e:
            logger.error(f"Error processing {report_name}: {e}")
            return {
                "status": "error",
                "rows": 0,
                "error": str(e),
            }

    def process_all_reports(
        self,
        toprocess_sheet_id: str,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process all reports defined in ToProcess tab.

        Args:
            toprocess_sheet_id: Google Sheet ID containing ToProcess tab

        Returns:
            Dict mapping report names to results
        """
        results = {}

        # Read ToProcess configuration
        year, configs = self.sheets.read_toprocess_config(toprocess_sheet_id)

        if year is None:
            logger.error("Failed to read ToProcess configuration")
            return {"_error": {"status": "error", "error": "Failed to read configuration"}}

        logger.info(f"Processing {len(configs)} reports for year {year}")

        for config in configs:
            report_name = config.get("qbo_report", "Unknown")
            dest_tab = config.get("dest_tab_name", "Unknown")
            key = f"{report_name} -> {dest_tab}"

            result = self.process_report(config, year, toprocess_sheet_id)
            results[key] = result

            if result["status"] == "success":
                logger.info(f"  ✓ {key}: {result['rows']} rows")
            else:
                logger.error(f"  ✗ {key}: {result['error']}")

        return results
