"""
V2 evaluation for SpreadsheetBench v2 datasets (Debugging, Financial_Model, Template).

Ported from SpreadsheetBench-2/evaluation/evaluation.py and kept structurally
parallel to it so the two can be diffed. Semantics-bearing functions are
vendored; generic helpers (_transform_value, _generate_cell_names) are reused
from evaluator.py. This module must not be imported at module level by
evaluator.py (evaluator.py uses a function-local import for dispatch) so the
helper imports below stay acyclic.
"""

from typing import Any

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet

from .evaluator import _generate_cell_names, _transform_value

_DISPLAY_EQUIVALENT_ERRORS = {"#DIV/0!", "#N/A"}
# Finance "not meaningful" placeholders: golden #DIV/0! vs output "N/A" (via
# IFERROR) must match.
_NOT_MEANINGFUL = _DISPLAY_EQUIVALENT_ERRORS | {
    "N/A",
    "NA",
    "N.A.",
    "N/M",
    "NM",
    "N.M.",
    "NOT MEANINGFUL",
    "NOT APPLICABLE",
    "NOT AVAILABLE",
    "—",
    "–",
    "-",
    "--",
    "---",
}


def _is_not_meaningful(v: Any) -> bool:
    if isinstance(v, str):
        return v.strip().upper() in _NOT_MEANINGFUL
    return False


def compare_cell_value(v1: Any, v2: Any, tolerance: float = 0.01) -> bool:
    """Tolerant value comparison (upstream compare_cell_value)."""
    # ArrayFormula objects compare by formula text
    if hasattr(v1, "text") and hasattr(v2, "text"):
        return bool(v1.text == v2.text)

    if _is_not_meaningful(v1) and _is_not_meaningful(v2):
        return True

    # Numeric vs numeric: raw values with tolerance only (no rounding, to
    # avoid boundary artifacts like -0.105 vs -0.10500000000000001)
    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
        return _numbers_match(v1, v2, tolerance)

    v1 = _transform_value(v1)
    v2 = _transform_value(v2)
    if (v1 == "" and v2 is None) or (v1 is None and v2 == ""):
        return True
    if (v1 == "" and v2 == "") or (v1 is None and v2 is None):
        return True
    # None is equivalent to 0 (empty cells read as None; rounding can produce
    # 0 from near-zero values)
    if (v1 is None and isinstance(v2, (int, float)) and v2 == 0) or (
        v2 is None and isinstance(v1, (int, float)) and v1 == 0
    ):
        return True
    if type(v1) is not type(v2):
        return False
    if v1 == v2:
        return True
    # Formula strings: Excel function names are case-insensitive
    if isinstance(v1, str) and isinstance(v2, str) and v1.startswith("=") and v2.startswith("="):
        return v1.replace("$", "").upper() == v2.replace("$", "").upper()
    # Numeric-looking strings arrive here as parsed floats
    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
        return _numbers_match(v1, v2, tolerance)
    return False


def _numbers_match(v1: float, v2: float, tolerance: float) -> bool:
    if v1 == v2:
        return True
    if v1 == 0 or v2 == 0:
        return abs(v1 - v2) <= tolerance
    return abs(v1 - v2) / max(abs(v1), abs(v2)) <= tolerance


_EXCEL_ERRORS = {"#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#NULL!", "#N/A", "#NUM!"}


def _has_excel_error(value: Any) -> bool:
    """True if the value is an Excel error string."""
    return isinstance(value, str) and value in _EXCEL_ERRORS


def _find_sheet(wb: openpyxl.Workbook, name: str) -> Worksheet | None:
    """Find a worksheet with whitespace-tolerant, case-insensitive matching."""
    if name in wb.sheetnames:
        return wb[name]
    name_stripped = name.strip().lower()
    for sn in wb.sheetnames:
        if sn.strip().lower() == name_stripped:
            return wb[sn]
    return None


def compare_cell_formula(f1: Any, f2: Any) -> bool:
    """Compare formula-level cell values (upstream compare_cell_formula)."""
    # ArrayFormula objects (CSE array formulas) compare by formula text
    if hasattr(f1, "text") and hasattr(f2, "text"):
        return bool(f1.text == f2.text)

    # Both formulas: normalize $ markers, case, and the legacy =+ prefix
    if isinstance(f1, str) and isinstance(f2, str) and f1.startswith("=") and f2.startswith("="):

        def _normalize(f: str) -> str:
            f = f.replace("$", "").upper()
            if f.startswith("=+"):
                f = "=" + f[2:]
            return f

        return _normalize(f1) == _normalize(f2)

    empty = (None, "")
    if f1 in empty and f2 in empty:
        return True

    return compare_cell_value(f1, f2)
