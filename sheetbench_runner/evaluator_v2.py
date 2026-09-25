"""
V2 evaluation for SpreadsheetBench v2 datasets (Debugging, Financial_Model, Template).

Ported from SpreadsheetBench-2/evaluation/evaluation.py and kept structurally
parallel to it so the two can be diffed. Semantics-bearing functions are
vendored; generic helpers (_transform_value, _generate_cell_names) are reused
from evaluator.py. This module must not be imported at module level by
evaluator.py (evaluator.py uses a function-local import for dispatch) so the
helper imports below stay acyclic.
"""

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import openpyxl
from openpyxl.styles import Font
from openpyxl.styles.colors import Color
from openpyxl.worksheet.worksheet import Worksheet

from .config import NumericToleranceMode
from .entities import EvaluationResult
from .evaluator import _generate_cell_names, _transform_value
from .findings import (
    CellState,
    Classification,
    Criterion,
    GradingDetail,
    GroupCounts,
    Mismatch,
    as_text,
)

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


def compare_cell_value(
    v1: Any,
    v2: Any,
    tolerance: float = 0.01,
    numeric_tolerance_mode: NumericToleranceMode = "relative",
) -> bool:
    """Tolerant value comparison (upstream compare_cell_value)."""
    # ArrayFormula objects compare by formula text
    if hasattr(v1, "text") and hasattr(v2, "text"):
        return bool(v1.text == v2.text)

    if _is_not_meaningful(v1) and _is_not_meaningful(v2):
        return True

    # Numeric vs numeric: raw values with tolerance only (no rounding, to
    # avoid boundary artifacts like -0.105 vs -0.10500000000000001)
    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
        return _numbers_match(v1, v2, tolerance, numeric_tolerance_mode)

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
        return _numbers_match(v1, v2, tolerance, numeric_tolerance_mode)
    return False


def _numbers_match(
    v1: float,
    v2: float,
    tolerance: float,
    numeric_tolerance_mode: NumericToleranceMode,
) -> bool:
    if v1 == v2:
        return True
    if numeric_tolerance_mode == "combined":
        return math.isclose(v1, v2, rel_tol=tolerance, abs_tol=tolerance)
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


def compare_cell_formula(
    f1: Any, f2: Any, numeric_tolerance_mode: NumericToleranceMode = "relative"
) -> bool:
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

    return compare_cell_value(f1, f2, numeric_tolerance_mode=numeric_tolerance_mode)


# Standard Excel theme color map (Office default theme)
_THEME_COLORS = [
    "FFFFFF",  # 0: lt1 (white/light background)
    "000000",  # 1: dk1 (black/dark text)
    "E7E6E6",  # 2: lt2 (light gray)
    "44546A",  # 3: dk2 (dark blue-gray)
    "4472C4",  # 4: accent1
    "ED7D31",  # 5: accent2
    "A5A5A5",  # 6: accent3
    "FFC000",  # 7: accent4
    "5B9BD5",  # 8: accent5
    "70AD47",  # 9: accent6
]


def _get_color_rgb(color: Color | None) -> str:
    """Extract RGB from a color object, resolving theme colors to RGB."""
    if not color:
        return "00000000"
    # openpyxl descriptors return validator objects when the attribute isn't
    # set, so check color.type rather than testing color.theme/color.rgb
    if color.type == "rgb" and isinstance(color.rgb, str):
        return color.rgb
    if color.type == "theme":
        theme_idx = color.theme
        if 0 <= theme_idx < len(_THEME_COLORS):
            base = _THEME_COLORS[theme_idx]
            tint = float(color.tint) if color.tint else 0.0
            if tint != 0:
                r, g, b = int(base[0:2], 16), int(base[2:4], 16), int(base[4:6], 16)
                if tint > 0:
                    r = int(r + (255 - r) * tint)
                    g = int(g + (255 - g) * tint)
                    b = int(b + (255 - b) * tint)
                else:
                    r = int(r * (1 + tint))
                    g = int(g * (1 + tint))
                    b = int(b * (1 + tint))
                r, g, b = min(255, max(0, r)), min(255, max(0, g)), min(255, max(0, b))
                base = f"{r:02X}{g:02X}{b:02X}"
            return "FF" + base
    return "00000000"


def compare_font_color(font_golden: Font, font_output: Font) -> bool:
    """Compare font colors on RGB only, ignoring the alpha channel."""
    rgb1 = _get_color_rgb(font_golden.color)
    rgb2 = _get_color_rgb(font_output.color)
    return rgb1[-6:] == rgb2[-6:]


class _LazyFormulaWorkbooks:
    """
    data_only=False copies of the input/golden/output workbooks, loaded on
    first use. The formula level is only needed when a cell shows an Excel
    error value, so most tasks never pay the extra load.
    """

    def __init__(self, input_path: Path, golden_path: Path, output_path: Path) -> None:
        self._paths: dict[str, Path] = {
            "input": input_path,
            "golden": golden_path,
            "output": output_path,
        }
        self._books: dict[str, openpyxl.Workbook] = {}

    def sheet(
        self, which: Literal["input", "golden", "output"], sheet_name: str
    ) -> Worksheet | None:
        if which not in self._books:
            self._books[which] = openpyxl.load_workbook(
                filename=self._paths[which], data_only=False
            )
        return _find_sheet(self._books[which], sheet_name)

    def close(self) -> None:
        for wb in self._books.values():
            wb.close()


def _compare_cells(
    cell1: Any,
    cell2: Any,
    with_font_color: bool,
    with_formula: bool,
    numeric_tolerance_mode: NumericToleranceMode,
) -> bool:
    if with_formula:
        return compare_cell_formula(cell1.value, cell2.value, numeric_tolerance_mode)
    if with_font_color:
        return compare_cell_value(
            cell1.value,
            cell2.value,
            numeric_tolerance_mode=numeric_tolerance_mode,
        ) and compare_font_color(cell1.font, cell2.font)
    return compare_cell_value(
        cell1.value,
        cell2.value,
        numeric_tolerance_mode=numeric_tolerance_mode,
    )


def classify_cells_by_modification(
    wb_input: openpyxl.Workbook,
    wb_golden: openpyxl.Workbook,
    sheet_name: str,
    cell_range: str,
    with_font_color: bool,
    with_formula: bool,
    formula_books: _LazyFormulaWorkbooks | None,
    numeric_tolerance_mode: NumericToleranceMode = "relative",
) -> tuple[list[str], list[str]]:
    """
    Split the range into regression cells (input == golden, must stay
    untouched) and modification cells (the task's actual work).
    """
    ws_input = _find_sheet(wb_input, sheet_name)
    ws_golden = _find_sheet(wb_golden, sheet_name)
    if ws_input is None or ws_golden is None:
        return [], []

    regression: list[str] = []
    modification: list[str] = []
    for cell_name in _generate_cell_names(cell_range):
        cell_in, cell_gold = ws_input[cell_name], ws_golden[cell_name]
        if (
            not with_formula
            and formula_books is not None
            and (_has_excel_error(cell_in.value) or _has_excel_error(cell_gold.value))
        ):
            ws_in_f = formula_books.sheet("input", sheet_name)
            ws_gold_f = formula_books.sheet("golden", sheet_name)
            assert ws_in_f is not None and ws_gold_f is not None
            is_same = compare_cell_formula(
                ws_in_f[cell_name].value,
                ws_gold_f[cell_name].value,
                numeric_tolerance_mode,
            )
        else:
            is_same = _compare_cells(
                cell_in,
                cell_gold,
                with_font_color,
                with_formula,
                numeric_tolerance_mode,
            )
        (regression if is_same else modification).append(cell_name)

    return regression, modification


_REASONS: dict[Criterion, str] = {
    "value": "the values differ",
    "formula": "the task grades formulas and the formulas differ",
    "formula_on_error": "a side shows an Excel error, so the formulas are compared",
    "font_color": "the value matches and the font color does not",
}


@dataclass(frozen=True)
class SheetGrading:
    """One sheet's tally and the cells its walk rejected."""

    regression_correct: int
    regression_total: int
    modification_correct: int
    modification_total: int
    mismatches: list[Mismatch]
    notes: list[str]


def _decide(
    cell_gold: Any,
    cell_out: Any,
    sheet_name: str,
    name: str,
    with_font_color: bool,
    with_formula: bool,
    formula_books: _LazyFormulaWorkbooks | None,
    numeric_tolerance_mode: NumericToleranceMode,
) -> tuple[Criterion, bool]:
    """
    The same decision _compare_cells makes, naming the comparison that made it. A cell the
    font-color criterion rejects is one whose value matched, so value takes precedence.
    """
    if (
        not with_formula
        and formula_books is not None
        and (_has_excel_error(cell_gold.value) or _has_excel_error(cell_out.value))
    ):
        ws_gold_f = formula_books.sheet("golden", sheet_name)
        ws_out_f = formula_books.sheet("output", sheet_name)
        assert ws_gold_f is not None and ws_out_f is not None
        return "formula_on_error", compare_cell_formula(
            ws_gold_f[name].value, ws_out_f[name].value, numeric_tolerance_mode
        )
    if with_formula:
        return "formula", compare_cell_formula(
            cell_gold.value, cell_out.value, numeric_tolerance_mode
        )
    values_match = compare_cell_value(
        cell_gold.value, cell_out.value, numeric_tolerance_mode=numeric_tolerance_mode
    )
    if not with_font_color:
        return "value", values_match
    if not values_match:
        return "value", False
    return "font_color", compare_font_color(cell_gold.font, cell_out.font)


def _cell_state(
    cell: Any,
    which: Literal["golden", "output"],
    sheet_name: str,
    name: str,
    with_font_color: bool,
    formula_books: _LazyFormulaWorkbooks | None,
) -> CellState:
    """
    A rejected cell as the grader read it. With no formula-level books the value level is
    already the formula level, which is what with_formula grading loads.
    """
    formula = as_text(cell.value)
    if formula_books is not None:
        ws = formula_books.sheet(which, sheet_name)
        formula = None if ws is None else as_text(ws[name].value)
    return CellState(
        value=as_text(cell.value),
        formula=formula,
        font_color=_get_color_rgb(cell.font.color) if with_font_color else None,
    )


def grade_classified_cells(
    wb_golden: openpyxl.Workbook,
    wb_output: openpyxl.Workbook,
    sheet_name: str,
    regression_cells: list[str],
    modification_cells: list[str],
    with_font_color: bool,
    with_formula: bool,
    formula_books: _LazyFormulaWorkbooks | None,
    numeric_tolerance_mode: NumericToleranceMode = "relative",
) -> SheetGrading:
    """
    Compare output vs golden for both cell groups, recording every rejected cell.
    A sheet missing from the output scores all its cells wrong and is a note, not a cell.
    """
    if _find_sheet(wb_output, sheet_name) is None:
        return SheetGrading(
            regression_correct=0,
            regression_total=len(regression_cells),
            modification_correct=0,
            modification_total=len(modification_cells),
            mismatches=[],
            notes=[f"{sheet_name} worksheet not found"],
        )

    ws_golden = _find_sheet(wb_golden, sheet_name)
    ws_output = _find_sheet(wb_output, sheet_name)

    def grade(cells: list[str], classification: Classification) -> tuple[int, list[Mismatch]]:
        correct = 0
        found: list[Mismatch] = []
        for name in cells:
            # Narrowed here (not above) so an empty `cells` list never
            # dereferences a None sheet, matching upstream's lazy access.
            assert ws_golden is not None and ws_output is not None
            cell_gold, cell_out = ws_golden[name], ws_output[name]
            criterion, matched = _decide(
                cell_gold,
                cell_out,
                sheet_name,
                name,
                with_font_color,
                with_formula,
                formula_books,
                numeric_tolerance_mode,
            )
            if matched:
                correct += 1
                continue
            found.append(
                Mismatch(
                    sheet=sheet_name,
                    cell=name,
                    classification=classification,
                    criterion=criterion,
                    reason=_REASONS[criterion],
                    expected=_cell_state(
                        cell_gold, "golden", sheet_name, name, with_font_color, formula_books
                    ),
                    actual=_cell_state(
                        cell_out, "output", sheet_name, name, with_font_color, formula_books
                    ),
                )
            )
        return correct, found

    reg_correct, reg_found = grade(regression_cells, "regression")
    mod_correct, mod_found = grade(modification_cells, "modification")

    return SheetGrading(
        regression_correct=reg_correct,
        regression_total=len(regression_cells),
        modification_correct=mod_correct,
        modification_total=len(modification_cells),
        mismatches=reg_found + mod_found,
        notes=[],
    )


def grading_messages(grading: SheetGrading) -> list[str]:
    """The grader's own error lines, derived from what its walk rejected."""
    return grading.notes + [
        f"{m.classification.capitalize()} error at {m.sheet}!{m.cell}: "
        f"answer={m.expected.value}, output={m.actual.value}"
        for m in grading.mismatches
    ]


def compare_classified_cells(
    wb_golden: openpyxl.Workbook,
    wb_output: openpyxl.Workbook,
    sheet_name: str,
    regression_cells: list[str],
    modification_cells: list[str],
    with_font_color: bool,
    with_formula: bool,
    formula_books: _LazyFormulaWorkbooks | None,
    numeric_tolerance_mode: NumericToleranceMode = "relative",
) -> tuple[int, int, int, int, list[str]]:
    """
    Compare output vs golden for both cell groups.
    Returns (reg_correct, reg_total, mod_correct, mod_total, error_messages).
    """
    grading = grade_classified_cells(
        wb_golden,
        wb_output,
        sheet_name,
        regression_cells,
        modification_cells,
        with_font_color,
        with_formula,
        formula_books,
        numeric_tolerance_mode,
    )
    return (
        grading.regression_correct,
        grading.regression_total,
        grading.modification_correct,
        grading.modification_total,
        grading_messages(grading),
    )


def compare_workbooks(
    input_path: Path,
    golden_path: Path,
    output_path: Path,
    ranges: list[tuple[str, str]],
    with_font_color: bool = False,
    with_formula: bool = False,
    numeric_tolerance_mode: NumericToleranceMode = "relative",
) -> EvaluationResult:
    """
    Grade an output workbook against golden with the v2 regression/
    modification semantics over pre-parsed (sheet_name, cell_range) tuples.

    Pass rule: modification ratio 1.0 AND regression ratio 1.0, where a
    regression ratio >= 0.998 (rounded to 4 decimals) snaps to 1.0. A group
    with zero cells scores 0.0.
    """
    data_only = not with_formula
    wb_input: openpyxl.Workbook | None = None
    wb_golden: openpyxl.Workbook | None = None
    wb_output: openpyxl.Workbook | None = None
    formula_books = (
        None if with_formula else _LazyFormulaWorkbooks(input_path, golden_path, output_path)
    )

    reg_correct = reg_total = mod_correct = mod_total = 0
    errors: list[str] = []
    mismatches: list[Mismatch] = []
    notes: list[str] = []
    try:
        wb_input = openpyxl.load_workbook(filename=input_path, data_only=data_only)
        wb_golden = openpyxl.load_workbook(filename=golden_path, data_only=data_only)
        wb_output = openpyxl.load_workbook(filename=output_path, data_only=data_only)
        for sheet_name, cell_range in ranges:
            regression, modification = classify_cells_by_modification(
                wb_input,
                wb_golden,
                sheet_name,
                cell_range,
                with_font_color,
                with_formula,
                formula_books,
                numeric_tolerance_mode,
            )
            grading = grade_classified_cells(
                wb_golden,
                wb_output,
                sheet_name,
                regression,
                modification,
                with_font_color,
                with_formula,
                formula_books,
                numeric_tolerance_mode,
            )
            reg_correct += grading.regression_correct
            reg_total += grading.regression_total
            mod_correct += grading.modification_correct
            mod_total += grading.modification_total
            errors.extend(grading_messages(grading))
            mismatches.extend(grading.mismatches)
            notes.extend(grading.notes)
    finally:
        if wb_input is not None:
            wb_input.close()
        if wb_golden is not None:
            wb_golden.close()
        if wb_output is not None:
            wb_output.close()
        if formula_books is not None:
            formula_books.close()

    reg_ratio = round(reg_correct / reg_total, 4) if reg_total else 0.0
    mod_ratio = round(mod_correct / mod_total, 4) if mod_total else 0.0
    if reg_ratio >= 0.998:
        reg_ratio = 1.0
    passed = reg_ratio == 1.0 and mod_ratio == 1.0

    if passed:
        message = ""
    else:
        first_error = next((m for m in errors if m), "")
        counts = (
            f"{reg_total - reg_correct}/{reg_total} regression and "
            f"{mod_total - mod_correct}/{mod_total} modification cells wrong"
        )
        message = f"{first_error}; {counts}" if first_error else counts

    return EvaluationResult(
        passed=passed,
        message=message,
        regression_accuracy=reg_ratio,
        modification_accuracy=mod_ratio,
        grading=GradingDetail(
            numeric_tolerance_mode=numeric_tolerance_mode,
            tolerance=0.01,
            with_font_color=with_font_color,
            with_formula=with_formula,
            regression=GroupCounts(
                correct=reg_correct,
                total=reg_total,
                wrong=reg_total - reg_correct,
                accuracy=reg_ratio,
            ),
            modification=GroupCounts(
                correct=mod_correct,
                total=mod_total,
                wrong=mod_total - mod_correct,
                accuracy=mod_ratio,
            ),
            mismatches=mismatches,
            notes=notes,
        ),
    )
