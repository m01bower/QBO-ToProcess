"""Post-write verification for FinancialSysUpdate.

Runs after all reports are uploaded. Two phases:

1. Gate check — did ALL reports succeed? If not, report pass/fail per report
   and stop (no timestamps or dashboard checks).

2. If all passed:
   a) Write timestamps to trigger recalculation in dependent sheets
   b) Read "ALL GOOD" cells from each dashboard/planning tab
   c) Compare row labels between P&L Monthly and Monthly Forecast
   d) Produce a summary of all checks (Pass / FAIL)
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

from logger_setup import get_logger

logger = get_logger()


@dataclass
class VerificationCheck:
    """Result of a single verification check."""
    name: str
    passed: bool
    detail: str = ""


@dataclass
class VerificationResult:
    """Aggregated verification results."""
    checks: list[VerificationCheck] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def summary_lines(self) -> list[str]:
        """Return formatted summary lines for console/notification."""
        lines = ["", "VERIFICATION SUMMARY", "=" * 40]
        for check in self.checks:
            icon = "✓" if check.passed else "✗"
            line = f"  {icon} {check.name}"
            if check.detail:
                line += f" — {check.detail}"
            lines.append(line)

        passed = sum(1 for c in self.checks if c.passed)
        failed = sum(1 for c in self.checks if not c.passed)
        lines.append("")
        lines.append(f"{passed} passed, {failed} failed")
        return lines


class VerificationProcessor:
    """Runs post-write verification checks."""

    def __init__(self, sheets_service, sheets_config, year: int):
        """
        Args:
            sheets_service: Authenticated SheetsService instance
            sheets_config: GoogleSheetsConfig from MasterConfig
            year: Report year (e.g. 2026)
        """
        self._sheets = sheets_service
        self._config = sheets_config
        self._year = year

    def run(self, report_results: dict) -> VerificationResult:
        """Run all verification checks.

        Args:
            report_results: Results dict from ReportProcessor.process_all_reports()

        Returns:
            VerificationResult with all check outcomes
        """
        result = VerificationResult()

        # ── Check #2: All reports 100% success (gate check) ──
        report_only = {
            k: v for k, v in report_results.items()
            if not k.startswith("_row_change_")
        }
        all_success = all(
            r.get("status") == "success" for r in report_only.values()
        )

        report_details = []
        for report, res in report_only.items():
            status = res.get("status", "unknown")
            if status == "success":
                report_details.append(f"{report}: GOOD")
            else:
                error = res.get("error", "unknown error")
                report_details.append(f"{report}: FAIL ({error})")

        detail_str = "; ".join(report_details)
        result.checks.append(VerificationCheck(
            name="All Reports Uploaded",
            passed=all_success,
            detail=detail_str,
        ))

        if not all_success:
            logger.error("Verification gate failed — not all reports succeeded")
            return result

        # ── All reports passed — proceed with timestamps and checks ──
        now_str = datetime.now().strftime("%m/%d/%Y %H:%M")
        toprocess_id = self._config.toprocess_sheet_id
        dashboard_id = self._config.financial_dashboard_sheet_id
        ar_id = self._config.ar_sheet_id
        total_cash_id = self._config.total_cash_sheet_id

        # ── Write timestamps ──
        self._write_timestamp(
            result, toprocess_id,
            f"{self._year} P&L Planning", "C1", now_str,
            "P&L Planning timestamp",
        )
        if dashboard_id:
            self._write_timestamp(
                result, dashboard_id,
                f"{self._year} Financial Dashboard", "C3", now_str,
                "Financial Dashboard timestamp",
            )
        if ar_id:
            self._write_timestamp(
                result, ar_id,
                "ARDashboard", "C1", now_str,
                "AR Dashboard timestamp",
            )
        if total_cash_id:
            self._write_timestamp(
                result, total_cash_id,
                f"{self._year} Cash", "B1", now_str,
                "Total Cash timestamp",
            )

        # ── ALL GOOD checks ──
        # P&L Planning: E1, I1, M1
        self._check_all_good(
            result, toprocess_id,
            f"{self._year} P&L Planning", "E1",
            "P&L Planning E1",
        )
        self._check_all_good(
            result, toprocess_id,
            f"{self._year} P&L Planning", "I1",
            "P&L Planning I1",
        )
        self._check_all_good(
            result, toprocess_id,
            f"{self._year} P&L Planning", "M1",
            "P&L Planning M1",
        )

        # Financial Dashboard: D1
        if dashboard_id:
            self._check_all_good(
                result, dashboard_id,
                f"{self._year} Financial Dashboard", "D1",
                "Financial Dashboard D1",
            )

        # AR Dashboard: D1
        if ar_id:
            self._check_all_good(
                result, ar_id,
                "ARDashboard", "D1",
                "AR Dashboard D1",
            )

        # Total Cash: B2
        if total_cash_id:
            self._check_all_good(
                result, total_cash_id,
                f"{self._year} Cash", "B2",
                "Total Cash B2",
            )

        # ── Total Cash: verify today's date in column A ──
        if total_cash_id:
            self._check_date_in_column(
                result, total_cash_id,
                f"{self._year} Cash", "A",
                "Total Cash date in Col A",
            )

        # ── AR Dashboard: verify today's date in column C ──
        if ar_id:
            self._check_date_in_column(
                result, ar_id,
                "ARDashboard", "C",
                "AR Dashboard date in Col C",
            )

        # ── AR Dashboard: auto-extend rows if needed ──
        if ar_id:
            self._ar_auto_extend(result, ar_id)

        # ── AR Dashboard: quarter markers ──
        if ar_id:
            self._ar_quarter_markers(result, ar_id)

        # ── Yearly Analysis: Review and Client Review tabs (same spreadsheet as ToProcess) ──
        self._write_timestamp(
            result, toprocess_id,
            "Review", "A1", now_str,
            "Yearly Analysis Review timestamp",
        )
        self._check_all_good(
            result, toprocess_id,
            "Review", "D1",
            "Yearly Analysis Review D1",
        )
        self._write_timestamp(
            result, toprocess_id,
            "Client Review", "A1", now_str,
            "Yearly Analysis Client Review timestamp",
        )
        self._check_all_good(
            result, toprocess_id,
            "Client Review", "D1",
            "Yearly Analysis Client Review D1",
        )

        # ── Row comparison: P&L Monthly vs Monthly Forecast ──
        self._check_row_match(result, toprocess_id)

        return result

    def _write_timestamp(
        self,
        result: VerificationResult,
        sheet_id: str,
        tab: str,
        cell: str,
        value: str,
        check_name: str,
    ) -> None:
        """Write a timestamp and record pass/fail."""
        ok = self._sheets.write_cell(sheet_id, tab, cell, value)
        result.checks.append(VerificationCheck(
            name=check_name,
            passed=ok,
            detail="written" if ok else "WRITE FAILED",
        ))

    def _check_all_good(
        self,
        result: VerificationResult,
        sheet_id: str,
        tab: str,
        cell: str,
        check_name: str,
    ) -> None:
        """Read a cell and verify it says 'ALL GOOD'."""
        value = self._sheets.read_cell(sheet_id, tab, cell)
        passed = value is not None and value.strip().upper() == "ALL GOOD"
        result.checks.append(VerificationCheck(
            name=check_name,
            passed=passed,
            detail=f"'{value}'" if not passed else "ALL GOOD",
        ))

    def _check_date_in_column(
        self,
        result: VerificationResult,
        sheet_id: str,
        tab: str,
        column: str,
        check_name: str,
    ) -> None:
        """Verify that today's date appears somewhere in the given column."""
        today = date.today()
        values = self._sheets.read_column(sheet_id, tab, column)
        if not values:
            result.checks.append(VerificationCheck(
                name=check_name,
                passed=False,
                detail=f"Could not read {tab} column {column}",
            ))
            return

        # Try common date formats to match today
        today_patterns = [
            today.strftime("%m/%d/%Y"),       # 03/24/2026
            today.strftime("%-m/%-d/%Y"),      # 3/24/2026
            today.strftime("%m/%d/%y"),         # 03/24/26
            today.strftime("%-m/%-d/%y"),       # 3/24/26
            today.strftime("%Y-%m-%d"),         # 2026-03-24
        ]

        found = False
        for val in values:
            cleaned = val.strip()
            # Also handle datetime strings by taking just the date part
            date_part = cleaned.split(" ")[0] if cleaned else ""
            if cleaned in today_patterns or date_part in today_patterns:
                found = True
                break

        result.checks.append(VerificationCheck(
            name=check_name,
            passed=found,
            detail="found" if found else f"ERROR — today's date ({today_patterns[0]}) not found",
        ))
        if not found:
            logger.error(f"{check_name}: today's date not found in {tab}!{column}")

    def _ar_auto_extend(
        self,
        result: VerificationResult,
        ar_id: str,
    ) -> None:
        """Auto-extend ARDashboard rows when few future slots remain.

        A future slot = Column E says 'No Tab' (the row has formulas for
        date/week/tab-name but no actual AR tab exists yet).
        If 5 or fewer future slots remain, copy the last row 90 times
        (formulas auto-increment the dates).
        """
        rows = self._sheets.read_range(ar_id, "ARDashboard", "A2:E")
        if not rows:
            result.checks.append(VerificationCheck(
                name="AR auto-extend",
                passed=True,
                detail="No data to evaluate",
            ))
            return

        # Find the last row with a real value in E (not "No Tab")
        # then count "No Tab" rows after it
        last_real_idx = -1
        for i, row in enumerate(rows):
            col_e = row[4].strip() if len(row) > 4 else ""
            if col_e and col_e.upper() != "NO TAB":
                last_real_idx = i

        no_tab_count = 0
        for row in rows[last_real_idx + 1:]:
            col_e = row[4].strip() if len(row) > 4 else ""
            if col_e.upper() == "NO TAB":
                no_tab_count += 1

        if no_tab_count <= 5:
            # Copy the very last row (its formulas will auto-increment)
            source_row = len(rows) + 1  # 1-based sheet row (data starts row 2)
            ok = self._sheets.copy_row_down(ar_id, "ARDashboard", source_row, 90)
            if ok:
                # Clear column A on all new rows so quarter markers from the
                # source row don't propagate — _ar_quarter_markers will write
                # markers only where a real quarter transition occurs.
                first_new_row = source_row + 1
                clear_range = f"'ARDashboard'!A{first_new_row}:A{first_new_row + 89}"
                self._sheets._shared.service.spreadsheets().values().clear(
                    spreadsheetId=ar_id,
                    range=clear_range,
                    body={},
                ).execute()
                logger.info(f"AR auto-extend: copied row {source_row} x90, cleared A{first_new_row}:A{first_new_row + 89}")
            else:
                logger.error("AR auto-extend: copy failed")
            result.checks.append(VerificationCheck(
                name="AR auto-extend",
                passed=ok,
                detail=f"Copied row {source_row} x90" if ok else "COPY FAILED",
            ))
        else:
            result.checks.append(VerificationCheck(
                name="AR auto-extend",
                passed=True,
                detail=f"{no_tab_count} future slots remaining — no extension needed",
            ))

    def _ar_quarter_markers(
        self,
        result: VerificationResult,
        ar_id: str,
    ) -> None:
        """Write quarter marker (e.g. 'Q1 2026') in Column A when today's date
        crosses a quarter boundary from the row above it."""
        today = date.today()
        today_quarter = (today.month - 1) // 3 + 1

        # Read columns A and C from row 2 down
        rows = self._sheets.read_range(ar_id, "ARDashboard", "A2:C")
        if not rows:
            return

        # Find the row with today's date in Col C (index 2) and check the row above
        today_patterns = [
            today.strftime("%m/%d/%Y"),
            today.strftime("%-m/%-d/%Y"),
            today.strftime("%m/%d/%y"),
            today.strftime("%-m/%-d/%y"),
            today.strftime("%Y-%m-%d"),
        ]

        for i, row in enumerate(rows):
            col_c = row[2].strip() if len(row) > 2 else ""
            date_part = col_c.split(" ")[0] if col_c else ""
            if col_c not in today_patterns and date_part not in today_patterns:
                continue

            # Found today's row — check if it crosses a quarter from the previous row
            if i == 0:
                # First data row — no previous row to compare
                break

            prev_row = rows[i - 1]
            prev_c = prev_row[2].strip() if len(prev_row) > 2 else ""
            prev_date_part = prev_c.split(" ")[0] if prev_c else ""

            # Try to parse the previous date
            prev_date = None
            for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
                try:
                    prev_date = datetime.strptime(prev_date_part, fmt).date()
                    break
                except ValueError:
                    continue

            if prev_date is None:
                break

            prev_quarter = (prev_date.month - 1) // 3 + 1
            prev_year = prev_date.year

            if today_quarter != prev_quarter or today.year != prev_year:
                # Quarter boundary crossed — write marker
                marker = f"Q{today_quarter} {today.year}"
                # Sheet row = i + 2 (data starts at row 2)
                sheet_row = i + 2
                col_a_current = row[0].strip() if len(row) > 0 else ""

                if col_a_current == marker:
                    result.checks.append(VerificationCheck(
                        name="AR quarter marker",
                        passed=True,
                        detail=f"{marker} already in A{sheet_row}",
                    ))
                else:
                    ok = self._sheets.write_cell(ar_id, "ARDashboard", f"A{sheet_row}", marker)
                    result.checks.append(VerificationCheck(
                        name="AR quarter marker",
                        passed=ok,
                        detail=f"{marker} written to A{sheet_row}" if ok else "WRITE FAILED",
                    ))
            else:
                result.checks.append(VerificationCheck(
                    name="AR quarter marker",
                    passed=True,
                    detail="No quarter boundary crossed",
                ))
            return

        # If we didn't find today's date, skip silently (date check already flagged it)

    def _find_anchor(self, labels: list[str], anchor: str = "Income") -> int:
        """Find the index of the anchor label (e.g. 'Income') in a label list.

        Returns -1 if not found.
        """
        for i, label in enumerate(labels):
            if label.strip() == anchor:
                return i
        return -1

    def _check_row_match(
        self,
        result: VerificationResult,
        sheet_id: str,
    ) -> None:
        """Compare row labels (column B) between P&L Monthly and Monthly Forecast.

        Finds "Income" as the anchor row in each tab, then compares all labels
        from that point down. If P&L Monthly has MORE rows, issue a WARNING
        listing the row numbers that don't match.
        """
        pl_tab = f"{self._year} P&L Monthly"
        fc_tab = f"{self._year} Monthly Forecast"

        pl_all = self._sheets.read_column(sheet_id, pl_tab, "B")
        fc_all = self._sheets.read_column(sheet_id, fc_tab, "B")

        if not pl_all:
            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=False,
                detail=f"Could not read {pl_tab} column B",
            ))
            return

        if not fc_all:
            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=False,
                detail=f"Could not read {fc_tab} column B",
            ))
            return

        pl_start = self._find_anchor(pl_all)
        fc_start = self._find_anchor(fc_all)

        if pl_start < 0:
            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=False,
                detail=f"'Income' anchor not found in {pl_tab} column B",
            ))
            return

        if fc_start < 0:
            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=False,
                detail=f"'Income' anchor not found in {fc_tab} column B",
            ))
            return

        # Slice from anchor to "Net Income" (inclusive)
        def _slice_to_net_income(labels, start):
            for i in range(start, len(labels)):
                if labels[i].strip().lower() == "net income":
                    return labels[start:i + 1]
            return labels[start:]

        pl_labels = _slice_to_net_income(pl_all, pl_start)
        fc_labels = _slice_to_net_income(fc_all, fc_start)
        pl_count = len(pl_labels)
        fc_count = len(fc_labels)

        # Sheet row numbers (1-based) for reporting
        pl_row_offset = pl_start + 1  # read_column starts at row 1
        fc_row_offset = fc_start + 1

        if pl_count > fc_count:
            # P&L has more rows — WARNING, list mismatches
            mismatches = []
            for i in range(pl_count):
                pl_val = pl_labels[i].strip()
                fc_val = fc_labels[i].strip() if i < fc_count else "(missing)"
                if pl_val != fc_val:
                    mismatches.append(
                        f"Row {pl_row_offset + i}: P&L='{pl_val}' "
                        f"vs Forecast='{fc_val}'"
                    )

            detail = (
                f"P&L Monthly has {pl_count} rows, Forecast has {fc_count} "
                f"(from 'Income'). Mismatches: {'; '.join(mismatches[:10])}"
            )
            if len(mismatches) > 10:
                detail += f" ...and {len(mismatches) - 10} more"

            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=True,  # WARNING, not failure
                detail=f"WARNING — {detail}",
            ))
            logger.warning(detail)

        elif pl_count < fc_count:
            result.checks.append(VerificationCheck(
                name="Row Count (P&L vs Forecast)",
                passed=False,
                detail=(
                    f"P&L Monthly has FEWER rows ({pl_count}) than "
                    f"Forecast ({fc_count}) from 'Income'"
                ),
            ))

        else:
            # Same count — check for label mismatches
            mismatches = []
            for i in range(pl_count):
                pl_val = pl_labels[i].strip()
                fc_val = fc_labels[i].strip()
                if pl_val != fc_val:
                    mismatches.append(
                        f"Row {pl_row_offset + i}: P&L='{pl_val}' "
                        f"vs Forecast='{fc_val}'"
                    )

            if mismatches:
                detail = (
                    f"Same row count ({pl_count}) but label mismatches: "
                    f"{'; '.join(mismatches[:10])}"
                )
                if len(mismatches) > 10:
                    detail += f" ...and {len(mismatches) - 10} more"
                result.checks.append(VerificationCheck(
                    name="Row Count (P&L vs Forecast)",
                    passed=True,
                    detail=f"WARNING — {detail}",
                ))
            else:
                result.checks.append(VerificationCheck(
                    name="Row Count (P&L vs Forecast)",
                    passed=True,
                    detail=f"{pl_count} rows match",
                ))

