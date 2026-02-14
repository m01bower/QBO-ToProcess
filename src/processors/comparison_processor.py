"""Comparison and filtering processors for AutoProcess reports."""

import re
from typing import List, Any, Tuple, Optional


def interleave_comparison_columns(
    headers: List[str],
    rows: List[List[Any]],
    current_year: int,
) -> Tuple[List[str], List[List[Any]]]:
    """
    Reorder a multi-year P&L report into interleaved comparison format.

    QBO returns chronological columns for a 2-year date range:
        [Account, Q1'25, Q2'25, Q3'25, Q4'25, Q1'26, Q2'26, Q3'26, Q4'26, Total]

    This reorders to interleaved comparison:
        [Account, Q1'26, Q1'25(PY), %Change, Q2'26, Q2'25(PY), %Change, ...,
         Total'26, Total'25(PY), %Change]

    Works for both Quarterly and Monthly display modes.

    Args:
        headers: Column headers from the report
        rows: Data rows from the report
        current_year: The current/primary year

    Returns:
        Tuple of (new_headers, new_rows)
    """
    if not headers or not rows:
        return headers, rows

    prior_year = current_year - 1

    # Parse headers to identify year in each column
    # Headers look like: "Jan 2025", "Q1 2025", "Feb 2026", "TOTAL", etc.
    year_pattern = re.compile(r'\b(20\d{2})\b')

    # Separate columns by year
    # Index 0 is always the account/label column
    label_col = 0
    prior_cols = {}   # period_key -> column_index
    current_cols = {}  # period_key -> column_index
    total_col = None

    for i, header in enumerate(headers):
        if i == 0:
            continue  # Skip label column

        match = year_pattern.search(header)
        if match:
            col_year = int(match.group(1))
            # Extract period part (strip the year to get "Q1", "Jan", etc.)
            period_key = year_pattern.sub("", header).strip().rstrip("'").strip()

            if col_year == prior_year:
                prior_cols[period_key] = i
            elif col_year == current_year:
                current_cols[period_key] = i
        elif "total" in header.lower():
            total_col = i

    # If we couldn't parse years, return unchanged
    if not current_cols and not prior_cols:
        return headers, rows

    # Build ordered period keys from current year columns
    # Preserve the order they appear in the original headers
    ordered_periods = []
    for i, header in enumerate(headers):
        match = year_pattern.search(header)
        if match and int(match.group(1)) == current_year:
            period_key = year_pattern.sub("", header).strip().rstrip("'").strip()
            if period_key not in ordered_periods:
                ordered_periods.append(period_key)

    # Build new column order
    new_headers = [headers[label_col]]
    col_mapping = []  # List of (col_index_or_None, col_index_or_None, is_pct_change)

    for period in ordered_periods:
        curr_idx = current_cols.get(period)
        prior_idx = prior_cols.get(period)

        # Current year column
        if curr_idx is not None:
            new_headers.append(headers[curr_idx])
            col_mapping.append((curr_idx, None, False))

        # Prior year column
        if prior_idx is not None:
            new_headers.append(f"{headers[prior_idx]} (PY)")
            col_mapping.append((prior_idx, None, False))

        # % Change column
        new_headers.append("% Change")
        col_mapping.append((curr_idx, prior_idx, True))

    # Add Total comparison if we have total columns for both years
    # Look for Total columns by year
    prior_total_idx = None
    current_total_idx = None
    for i, header in enumerate(headers):
        if "total" in header.lower():
            match = year_pattern.search(header)
            if match:
                col_year = int(match.group(1))
                if col_year == prior_year:
                    prior_total_idx = i
                elif col_year == current_year:
                    current_total_idx = i
            elif total_col == i:
                # Single "TOTAL" column with no year — skip interleaving for it
                pass

    if current_total_idx is not None:
        new_headers.append(headers[current_total_idx])
        col_mapping.append((current_total_idx, None, False))
    if prior_total_idx is not None:
        new_headers.append(f"{headers[prior_total_idx]} (PY)")
        col_mapping.append((prior_total_idx, None, False))
    if current_total_idx is not None or prior_total_idx is not None:
        new_headers.append("% Change")
        col_mapping.append((current_total_idx, prior_total_idx, True))

    # Reorder rows
    new_rows = []
    for row in rows:
        new_row = [row[label_col] if label_col < len(row) else ""]

        for curr_idx, prior_idx, is_pct in col_mapping:
            if is_pct:
                # Compute % change
                curr_val = _to_float(row[curr_idx] if curr_idx is not None and curr_idx < len(row) else "")
                prior_val = _to_float(row[prior_idx] if prior_idx is not None and prior_idx < len(row) else "")
                pct = _pct_change(curr_val, prior_val)
                new_row.append(pct)
            else:
                idx = curr_idx
                if idx is not None and idx < len(row):
                    new_row.append(row[idx])
                else:
                    new_row.append("")

        new_rows.append(new_row)

    return new_headers, new_rows


def filter_rows_by_products(
    headers: List[str],
    rows: List[List[Any]],
    product_names: List[str],
) -> Tuple[List[str], List[List[Any]]]:
    """
    Filter parsed report rows to keep only rows matching specified product names.

    Case-insensitive matching on the first column (account/item name).
    Keeps rows whose first column matches a product name, as well as
    section headers ("Total ...") for matched products.

    Args:
        headers: Column headers (returned unchanged)
        rows: Data rows from parsed report
        product_names: List of product/item names to keep

    Returns:
        Tuple of (headers, filtered_rows)
    """
    if not product_names or not rows:
        return headers, rows

    # Normalize product names for case-insensitive matching
    normalized = {name.strip().lower() for name in product_names}

    filtered = []
    for row in rows:
        if not row:
            continue

        first_col = str(row[0]).strip()
        first_lower = first_col.lower()

        # Direct match
        if first_lower in normalized:
            filtered.append(row)
            continue

        # Match "Total <product>" lines
        if first_lower.startswith("total "):
            name_part = first_lower[len("total "):].strip()
            if name_part in normalized:
                filtered.append(row)

    return headers, filtered


def _to_float(value: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _pct_change(current: Optional[float], prior: Optional[float]) -> str:
    """
    Compute percentage change: (current - prior) / abs(prior) * 100.

    Returns formatted string or empty string if not computable.
    """
    if current is None and prior is None:
        return ""
    if prior is None or prior == 0:
        if current is not None and current != 0:
            return "N/A"
        return ""
    if current is None:
        current = 0

    pct = (current - prior) / abs(prior) * 100
    return f"{pct:.1f}%"
