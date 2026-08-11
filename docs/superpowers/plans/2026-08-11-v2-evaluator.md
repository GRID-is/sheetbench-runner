# V2 Evaluation Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Grade SpreadsheetBench v2 tasks (Debugging, Financial_Model, Template) with the upstream v2 evaluator's semantics — tolerant comparison plus a regression/modification split — so results are comparable to published SpreadsheetBench 2 numbers.

**Architecture:** A new module `sheetbench_runner/evaluator_v2.py` vendors the comparison/classification/scoring logic from `SpreadsheetBench-2/evaluation/evaluation.py` (sibling repo, read-only reference). `Evaluator.evaluate` dispatches v2 tasks (marker: `task.golden_response_path is not None`) to it via a function-local import (avoids a circular module dependency: `evaluator_v2` imports helpers from `evaluator` at module level, `evaluator` imports `evaluator_v2` only inside the dispatch method). v1 grading is untouched. New optional `regression_accuracy`/`modification_accuracy` fields flow through `EvaluationResult` → `TaskResult` → `results.json` and into run summaries.

**Tech Stack:** Python 3.12, openpyxl, pytest. Run tests with `uv run pytest`, lint/typecheck with `make lt`.

**Spec:** `docs/superpowers/specs/2026-08-11-v2-evaluator-design.md` — read it first.
**Upstream reference:** `/Users/atli/projects/grid/SpreadsheetBench-2/evaluation/evaluation.py` (DO NOT EDIT; port from it).

**File map:**

| File | Change |
|---|---|
| `sheetbench_runner/evaluator_v2.py` | Create — vendored comparators, classification, scoring |
| `sheetbench_runner/entities.py` | Modify — `EvaluationResult` + `TaskResult` ratio fields |
| `sheetbench_runner/evaluator.py` | Modify — v2 dispatch in `Evaluator.evaluate` |
| `sheetbench_runner/runner.py` | Modify — plumb ratios into results, stats, reevaluate |
| `sheetbench_runner/cli.py` | Modify — summary averages |
| `tests/test_evaluator_v2.py` | Create — unit + end-to-end tests |
| `README.md` | Modify — replace v2 grading caveat |

Conventions: tests are class-grouped (`TestXxx`) like `tests/test_evaluator.py`. Commit after every green task. All work happens on the `support-spreadsheetbench-v2` branch.

**Strict mypy:** `pyproject.toml` sets `[tool.mypy] strict = true` over `sheetbench_runner/` (tests are not type-checked). The vendored code blocks below elide some annotations for diffability with upstream — add full type annotations (worksheet params/returns are `Worksheet | None` via `from openpyxl.worksheet.worksheet import Worksheet`; cell params are `Cell`; list returns like `tuple[list[str], list[str]]`) as each function is written, so `make lt` at Task 7 comes up clean rather than surfacing four tasks' worth of errors at once.

---

### Task 1: Value comparator (`compare_cell_value`)

Port of upstream `compare_cell_value` (evaluation.py:38-103). Reuses `_transform_value` from `evaluator.py` (identical to upstream's `transform_value`).

**Files:**
- Create: `sheetbench_runner/evaluator_v2.py`
- Create: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for v2 (SpreadsheetBench 2) evaluation semantics."""

import openpyxl
import pytest
from openpyxl.styles import Font

from sheetbench_runner.evaluator_v2 import compare_cell_value


class TestCompareCellValue:
    """Port of upstream compare_cell_value semantics."""

    # -- numeric tolerance (1% relative, 0.01 absolute at zero) --

    def test_equal_numbers(self):
        assert compare_cell_value(5, 5) is True

    def test_within_relative_tolerance(self):
        assert compare_cell_value(100.0, 100.9) is True  # 0.9% off

    def test_outside_relative_tolerance(self):
        assert compare_cell_value(100.0, 101.1) is False  # 1.1% off

    def test_zero_uses_absolute_tolerance(self):
        assert compare_cell_value(0, 0.009) is True
        assert compare_cell_value(0, 0.011) is False

    def test_no_rounding_boundary_artifact(self):
        # Raw comparison, not round-to-2: these differ by ~1e-17
        assert compare_cell_value(-0.105, -0.10500000000000001) is True

    # -- "not meaningful" equivalence --

    @pytest.mark.parametrize("golden,output", [
        ("#DIV/0!", "N/A"),
        ("#N/A", "NM"),
        ("#DIV/0!", "n.m."),
        ("#N/A", "—"),
        ("-", "--"),
        ("Not Meaningful", "#DIV/0!"),
    ])
    def test_not_meaningful_pairs_match(self, golden, output):
        assert compare_cell_value(golden, output) is True

    def test_not_meaningful_vs_number_fails(self):
        assert compare_cell_value("#DIV/0!", 5.0) is False

    def test_plain_string_not_in_equivalence_class(self):
        assert compare_cell_value("N/A", "hello") is False

    # -- empty / None / zero equivalences --

    def test_none_equals_empty_string(self):
        assert compare_cell_value(None, "") is True
        assert compare_cell_value("", None) is True

    def test_none_equals_zero(self):
        assert compare_cell_value(None, 0) is True
        assert compare_cell_value(0.0, None) is True

    def test_none_vs_nonzero_fails(self):
        assert compare_cell_value(None, 5) is False

    # -- strings --

    def test_numeric_strings_compared_with_tolerance(self):
        assert compare_cell_value("100.0", "100.5") is True
        assert compare_cell_value("100.0", "102.0") is False

    def test_numeric_string_vs_number(self):
        assert compare_cell_value("3.14159", 3.14) is True

    def test_formula_strings_case_and_dollar_insensitive(self):
        assert compare_cell_value("=sum($A$1:B2)", "=SUM(A1:B2)") is True
        assert compare_cell_value("=SUM(A1:B2)", "=SUM(A1:B3)") is False

    def test_type_mismatch_fails(self):
        assert compare_cell_value("hello", 5.0) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'sheetbench_runner.evaluator_v2'`

- [ ] **Step 3: Write the implementation**

```python
"""
V2 evaluation for SpreadsheetBench v2 datasets (Debugging, Financial_Model, Template).

Ported from SpreadsheetBench-2/evaluation/evaluation.py and kept structurally
parallel to it so the two can be diffed. Semantics-bearing functions are
vendored; generic helpers (_transform_value, _generate_cell_names) are reused
from evaluator.py. This module must not be imported at module level by
evaluator.py (evaluator.py uses a function-local import for dispatch) so the
helper imports below stay acyclic.
"""

from pathlib import Path
from typing import Any

import openpyxl

from .entities import EvaluationResult
from .evaluator import _generate_cell_names, _transform_value

_DISPLAY_EQUIVALENT_ERRORS = {"#DIV/0!", "#N/A"}
# Finance "not meaningful" placeholders: golden #DIV/0! vs output "N/A" (via
# IFERROR) must match.
_NOT_MEANINGFUL = _DISPLAY_EQUIVALENT_ERRORS | {
    "N/A", "NA", "N.A.", "N/M", "NM", "N.M.",
    "NOT MEANINGFUL", "NOT APPLICABLE", "NOT AVAILABLE",
    "—", "–", "-", "--", "---",
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
```

Note: upstream has the numeric comparison inlined twice; `_numbers_match` folds the duplicate. Every branch and its ordering is otherwise verbatim.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator_v2.py tests/test_evaluator_v2.py
git commit -m "Add v2 value comparator ported from SpreadsheetBench 2"
```

---

### Task 2: Formula comparator, error detection, sheet lookup

Port of upstream `compare_cell_formula` (evaluation.py:161-185, taking values instead of cells), `_has_excel_error` (:266-269, value-based), `_find_sheet` (:255-263).

**Files:**
- Modify: `sheetbench_runner/evaluator_v2.py`
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_evaluator_v2.py` (extend the import to include the new names):

```python
from sheetbench_runner.evaluator_v2 import (
    _find_sheet,
    _has_excel_error,
    compare_cell_formula,
    compare_cell_value,
)


class TestCompareCellFormula:
    def test_identical_formulas(self):
        assert compare_cell_formula("=SUM(A1:B2)", "=SUM(A1:B2)") is True

    def test_case_dollar_and_plus_normalized(self):
        assert compare_cell_formula("=+sum($A$1:B2)", "=SUM(A1:B2)") is True

    def test_different_formulas(self):
        assert compare_cell_formula("=SUM(A1:B2)", "=SUM(A1:B3)") is False

    def test_none_and_empty_equivalent(self):
        assert compare_cell_formula(None, "") is True

    def test_non_formula_values_fall_back_to_value_compare(self):
        assert compare_cell_formula(100.0, 100.9) is True
        assert compare_cell_formula(100.0, 102.0) is False


class TestHasExcelError:
    @pytest.mark.parametrize(
        "value", ["#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#NULL!", "#N/A", "#NUM!"]
    )
    def test_error_strings(self, value):
        assert _has_excel_error(value) is True

    def test_non_errors(self):
        assert _has_excel_error("hello") is False
        assert _has_excel_error(5.0) is False
        assert _has_excel_error(None) is False


class TestFindSheet:
    def test_exact_and_fuzzy_match(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "My Sheet "
        assert _find_sheet(wb, "My Sheet ") is ws
        assert _find_sheet(wb, "my sheet") is ws
        assert _find_sheet(wb, "Other") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: new tests FAIL with ImportError

- [ ] **Step 3: Write the implementation**

Add to `sheetbench_runner/evaluator_v2.py`:

```python
_EXCEL_ERRORS = {"#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#NULL!", "#N/A", "#NUM!"}


def _has_excel_error(value: Any) -> bool:
    """True if the value is an Excel error string."""
    return isinstance(value, str) and value in _EXCEL_ERRORS


def _find_sheet(wb: openpyxl.Workbook, name: str):
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator_v2.py tests/test_evaluator_v2.py
git commit -m "Add v2 formula comparator and error/sheet helpers"
```

---

### Task 3: Font-color comparison (Debugging Color mode)

Port of `_get_color_rgb`/`_compare_colors`/`compare_font_color` (evaluation.py:106-158).

**Files:**
- Modify: `sheetbench_runner/evaluator_v2.py`
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing tests**

```python
from openpyxl.styles.colors import Color

from sheetbench_runner.evaluator_v2 import compare_font_color


class TestCompareFontColor:
    def test_same_rgb_matches(self):
        assert compare_font_color(Font(color="FFFF0000"), Font(color="FFFF0000")) is True

    def test_alpha_channel_ignored(self):
        assert compare_font_color(Font(color="00FF0000"), Font(color="FFFF0000")) is True

    def test_different_rgb_fails(self):
        assert compare_font_color(Font(color="FFFF0000"), Font(color="FF00FF00")) is False

    def test_unset_colors_match(self):
        assert compare_font_color(Font(), Font()) is True

    def test_theme_color_resolves_to_rgb(self):
        # Theme 4 (accent1) is 4472C4 in the default Office theme
        themed = Font(color=Color(theme=4, tint=0.0))
        assert compare_font_color(themed, Font(color="FF4472C4")) is True

    def test_theme_color_with_tint(self):
        # Positive tint lightens toward white: 4472C4 at tint 0.5 -> A1B8E1
        themed = Font(color=Color(theme=4, tint=0.5))
        assert compare_font_color(themed, Font(color="FFA1B8E1")) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py::TestCompareFontColor -v`
Expected: FAIL with ImportError

- [ ] **Step 3: Write the implementation**

Add to `sheetbench_runner/evaluator_v2.py` (verbatim port; keep the theme table):

```python
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


def _get_color_rgb(color: Any) -> str:
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


def compare_font_color(font_golden: Any, font_output: Any) -> bool:
    """Compare font colors on RGB only, ignoring the alpha channel."""
    rgb1 = _get_color_rgb(font_golden.color)
    rgb2 = _get_color_rgb(font_output.color)
    return rgb1[-6:] == rgb2[-6:]
```

(Upstream's separate `_compare_colors` is folded into `compare_font_color` — it had a single caller.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: all PASS. If `test_theme_color_with_tint` fails on the expected hex, compute it: `int(0x44 + (255-0x44)*0.5) = 161 = 0xA1`, `int(0x72 + (255-0x72)*0.5) = 184 = 0xB8`, `int(0xC4 + (255-0xC4)*0.5) = 225 = 0xE1` — fix the test constant, not the port.

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator_v2.py tests/test_evaluator_v2.py
git commit -m "Add v2 font-color comparison with theme resolution"
```

---

### Task 4: Classification and counting

Port of `classify_cells_by_modification` (evaluation.py:282-304) and `cell_level_compare_with_classification` (:307-344), plus the lazy formula-workbook loader (spec: formula-level copies load on first Excel-error cell, not eagerly).

**Files:**
- Modify: `sheetbench_runner/evaluator_v2.py`
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing tests**

Add a module-level helper and tests to `tests/test_evaluator_v2.py`:

```python
from sheetbench_runner.evaluator_v2 import (
    _LazyFormulaWorkbooks,
    classify_cells_by_modification,
    compare_classified_cells,
)


def build_workbook(path, sheet_name="Model", cells=None, fonts=None):
    """Write an xlsx with the given {cell: value} and optional {cell: Font}."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for cell, value in (cells or {}).items():
        ws[cell] = value
    for cell, font in (fonts or {}).items():
        ws[cell].font = font
    wb.save(path)
    return path


class TestClassifyCells:
    def test_split_regression_vs_modification(self, temp_dir):
        input_path = build_workbook(temp_dir / "in.xlsx", cells={"A1": 1, "A2": 2, "A3": 3})
        golden_path = build_workbook(temp_dir / "gold.xlsx", cells={"A1": 1, "A2": 99, "A3": 3})
        wb_in = openpyxl.load_workbook(input_path, data_only=True)
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)

        regression, modification = classify_cells_by_modification(
            wb_in, wb_gold, "Model", "A1:A3", False, False, None
        )
        assert regression == ["A1", "A3"]
        assert modification == ["A2"]

    def test_missing_sheet_contributes_no_cells(self, temp_dir):
        input_path = build_workbook(temp_dir / "in.xlsx", sheet_name="Other")
        golden_path = build_workbook(temp_dir / "gold.xlsx")
        wb_in = openpyxl.load_workbook(input_path, data_only=True)
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)

        regression, modification = classify_cells_by_modification(
            wb_in, wb_gold, "Model", "A1:A3", False, False, None
        )
        assert regression == [] and modification == []


class TestCompareClassifiedCells:
    def test_counts_and_error_messages(self, temp_dir):
        golden_path = build_workbook(temp_dir / "gold.xlsx", cells={"A1": 1, "A2": 99, "A3": 3})
        output_path = build_workbook(temp_dir / "out.xlsx", cells={"A1": 1, "A2": 98, "A3": 4})
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)
        wb_out = openpyxl.load_workbook(output_path, data_only=True)

        rc, rt, mc, mt, errors = compare_classified_cells(
            wb_gold, wb_out, "Model", ["A1", "A3"], ["A2"], False, False, None
        )
        assert (rc, rt) == (1, 2)  # A1 right, A3 wrong (3 vs 4)
        assert (mc, mt) == (0, 1)  # A2: 98 vs 99 is 1/99 = 1.01% off, outside 1%
        assert any("Regression error at Model!A3" in e for e in errors)
        assert any("Modification error at Model!A2" in e for e in errors)

    def test_missing_output_sheet_scores_all_wrong(self, temp_dir):
        golden_path = build_workbook(temp_dir / "gold.xlsx", cells={"A1": 1})
        output_path = build_workbook(temp_dir / "out.xlsx", sheet_name="Other")
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)
        wb_out = openpyxl.load_workbook(output_path, data_only=True)

        rc, rt, mc, mt, errors = compare_classified_cells(
            wb_gold, wb_out, "Model", ["A1"], [], False, False, None
        )
        assert (rc, rt, mc, mt) == (0, 1, 0, 0)
        assert errors == ["Model worksheet not found"]

    def test_error_value_falls_back_to_formula_compare(self, temp_dir):
        # Golden shows #DIV/0! from =1/A9; output shows #DIV/0! from the same
        # formula -> formula fallback matches. Literal '=' strings are stored
        # as formulas by openpyxl; data_only=True loads read None for them
        # (no cached value), while the error string is a plain string value.
        golden_path = build_workbook(temp_dir / "gold.xlsx", cells={"A1": "#DIV/0!"})
        output_path = build_workbook(temp_dir / "out.xlsx", cells={"A1": "#DIV/0!"})
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)
        wb_out = openpyxl.load_workbook(output_path, data_only=True)
        books = _LazyFormulaWorkbooks(None, golden_path, output_path)

        rc, rt, mc, mt, errors = compare_classified_cells(
            wb_gold, wb_out, "Model", [], ["A1"], False, False, books
        )
        assert (mc, mt) == (1, 1)

    def test_lazy_books_not_loaded_without_errors(self, temp_dir):
        golden_path = build_workbook(temp_dir / "gold.xlsx", cells={"A1": 1})
        output_path = build_workbook(temp_dir / "out.xlsx", cells={"A1": 1})
        wb_gold = openpyxl.load_workbook(golden_path, data_only=True)
        wb_out = openpyxl.load_workbook(output_path, data_only=True)
        books = _LazyFormulaWorkbooks(None, golden_path, output_path)

        compare_classified_cells(wb_gold, wb_out, "Model", ["A1"], [], False, False, books)
        assert books._books == {}  # nothing loaded
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: FAIL with ImportError

- [ ] **Step 3: Write the implementation**

Add to `sheetbench_runner/evaluator_v2.py`:

```python
class _LazyFormulaWorkbooks:
    """
    data_only=False copies of the input/golden/output workbooks, loaded on
    first use. The formula level is only needed when a cell shows an Excel
    error value, so most tasks never pay the extra load.
    """

    def __init__(self, input_path, golden_path, output_path):
        self._paths = {"input": input_path, "golden": golden_path, "output": output_path}
        self._books: dict[str, openpyxl.Workbook] = {}

    def sheet(self, which: str, sheet_name: str):
        if which not in self._books:
            self._books[which] = openpyxl.load_workbook(
                filename=self._paths[which], data_only=False
            )
        return _find_sheet(self._books[which], sheet_name)

    def close(self) -> None:
        for wb in self._books.values():
            wb.close()


def _compare_cells(cell1, cell2, with_font_color: bool, with_formula: bool) -> bool:
    if with_formula:
        return compare_cell_formula(cell1.value, cell2.value)
    if with_font_color:
        return compare_cell_value(cell1.value, cell2.value) and compare_font_color(
            cell1.font, cell2.font
        )
    return compare_cell_value(cell1.value, cell2.value)


def classify_cells_by_modification(
    wb_input, wb_golden, sheet_name, cell_range, with_font_color, with_formula, formula_books
):
    """
    Split the range into regression cells (input == golden, must stay
    untouched) and modification cells (the task's actual work).
    """
    ws_input = _find_sheet(wb_input, sheet_name)
    ws_golden = _find_sheet(wb_golden, sheet_name)
    if ws_input is None or ws_golden is None:
        return [], []

    regression, modification = [], []
    for cell_name in _generate_cell_names(cell_range):
        cell_in, cell_gold = ws_input[cell_name], ws_golden[cell_name]
        if (
            not with_formula
            and formula_books is not None
            and (_has_excel_error(cell_in.value) or _has_excel_error(cell_gold.value))
        ):
            ws_in_f = formula_books.sheet("input", sheet_name)
            ws_gold_f = formula_books.sheet("golden", sheet_name)
            is_same = compare_cell_formula(ws_in_f[cell_name].value, ws_gold_f[cell_name].value)
        else:
            is_same = _compare_cells(cell_in, cell_gold, with_font_color, with_formula)
        (regression if is_same else modification).append(cell_name)

    return regression, modification


def compare_classified_cells(
    wb_golden,
    wb_output,
    sheet_name,
    regression_cells,
    modification_cells,
    with_font_color,
    with_formula,
    formula_books,
):
    """
    Compare output vs golden for both cell groups.
    Returns (reg_correct, reg_total, mod_correct, mod_total, error_messages).
    A sheet missing from the output scores all its cells wrong.
    """
    if _find_sheet(wb_output, sheet_name) is None:
        return (
            0,
            len(regression_cells),
            0,
            len(modification_cells),
            [f"{sheet_name} worksheet not found"],
        )

    ws_golden = _find_sheet(wb_golden, sheet_name)
    ws_output = _find_sheet(wb_output, sheet_name)

    def count_correct(cells, label):
        correct, errors = 0, []
        for name in cells:
            cell_gold, cell_out = ws_golden[name], ws_output[name]
            if (
                not with_formula
                and formula_books is not None
                and (_has_excel_error(cell_gold.value) or _has_excel_error(cell_out.value))
            ):
                ws_gold_f = formula_books.sheet("golden", sheet_name)
                ws_out_f = formula_books.sheet("output", sheet_name)
                matched = compare_cell_formula(ws_gold_f[name].value, ws_out_f[name].value)
            else:
                matched = _compare_cells(cell_gold, cell_out, with_font_color, with_formula)
            if matched:
                correct += 1
            else:
                gold_val, out_val = cell_gold.value, cell_out.value
                if hasattr(gold_val, "text"):
                    gold_val = gold_val.text
                if hasattr(out_val, "text"):
                    out_val = out_val.text
                errors.append(
                    f"{label} error at {sheet_name}!{name}: answer={gold_val}, output={out_val}"
                )
        return correct, errors

    reg_correct, reg_errors = count_correct(regression_cells, "Regression")
    mod_correct, mod_errors = count_correct(modification_cells, "Modification")

    return (
        reg_correct,
        len(regression_cells),
        mod_correct,
        len(modification_cells),
        reg_errors + mod_errors,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator_v2.py tests/test_evaluator_v2.py
git commit -m "Add v2 cell classification and counting"
```

---

### Task 5: Workbook-level comparison and scoring

Port of `compare_workbooks_with_regression` (evaluation.py:347-405) plus the scoring rules from `process_single_item` (:452-458): ratios rounded to 4dp, zero-total group scores 0.0, regression ≥ 0.998 snaps to 1.0, pass = both ratios 1.0.

**Files:**
- Modify: `sheetbench_runner/evaluator_v2.py`
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing tests**

```python
from sheetbench_runner.evaluator_v2 import compare_workbooks


class TestCompareWorkbooks:
    def make_files(self, temp_dir, input_cells, golden_cells, output_cells):
        return (
            build_workbook(temp_dir / "in.xlsx", cells=input_cells),
            build_workbook(temp_dir / "gold.xlsx", cells=golden_cells),
            build_workbook(temp_dir / "out.xlsx", cells=output_cells),
        )

    def test_output_equals_golden_passes(self, temp_dir):
        cells = {"A1": 1, "A2": 2, "A3": 3}
        golden = {"A1": 1, "A2": 99, "A3": 3}
        inp, gold, out = self.make_files(temp_dir, cells, golden, dict(golden))
        result = compare_workbooks(inp, gold, out, [("Model", "A1:A3")])
        assert result.passed is True
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 1.0
        assert result.message == ""

    def test_output_equals_input_fails_modification(self, temp_dir):
        cells = {"A1": 1, "A2": 2, "A3": 3}
        golden = {"A1": 1, "A2": 99, "A3": 3}
        inp, gold, out = self.make_files(temp_dir, cells, golden, dict(cells))
        result = compare_workbooks(inp, gold, out, [("Model", "A1:A3")])
        assert result.passed is False
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 0.0
        assert "Modification error at Model!A2" in result.message
        assert "0/2 regression and 1/1 modification cells wrong" in result.message

    def test_regression_snap_at_998(self, temp_dir):
        # 500 regression cells, 1 wrong -> 499/500 = 0.998 -> snaps to 1.0
        n = 500
        cells = {f"A{i}": i for i in range(1, n + 1)}
        golden = dict(cells)
        golden["B1"] = 42  # one modification cell
        output = dict(golden)
        output["A1"] = -1  # one regression cell wrong
        inp, gold, out = self.make_files(temp_dir, cells, golden, output)
        result = compare_workbooks(inp, gold, out, [("Model", f"A1:B{n}")])
        # B2..B500 are empty in input and golden -> regression (None == None)
        # regression total = 500 A-cells + 499 empty B-cells = 999; 1 wrong
        # 998/999 = 0.9990 -> >= 0.998 -> snaps to 1.0 -> passes
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 1.0
        assert result.passed is True

    def test_regression_below_snap_fails(self, temp_dir):
        # 3 of 100 regression cells wrong -> 0.97 -> no snap
        cells = {f"A{i}": i for i in range(1, 101)}
        golden = dict(cells)
        output = dict(cells)
        for i in (1, 2, 3):
            output[f"A{i}"] = -i
        inp, gold, out = self.make_files(temp_dir, cells, golden, output)
        result = compare_workbooks(inp, gold, out, [("Model", "A1:A100")])
        assert result.passed is False
        assert result.regression_accuracy == 0.97

    def test_zero_total_modification_group_scores_zero(self, temp_dir):
        # Input already equals golden: no modification cells -> mod ratio 0.0,
        # task can never pass (upstream behavior, ported verbatim)
        cells = {"A1": 1}
        inp, gold, out = self.make_files(temp_dir, cells, dict(cells), dict(cells))
        result = compare_workbooks(inp, gold, out, [("Model", "A1")])
        assert result.passed is False
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 0.0

    def test_multiple_ranges_aggregate(self, temp_dir):
        inp = build_workbook(temp_dir / "in.xlsx", cells={"A1": 1, "B1": 2})
        gold = build_workbook(temp_dir / "gold.xlsx", cells={"A1": 10, "B1": 20})
        out = build_workbook(temp_dir / "out.xlsx", cells={"A1": 10, "B1": 2})
        result = compare_workbooks(inp, gold, out, [("Model", "A1"), ("Model", "B1")])
        assert result.modification_accuracy == 0.5
        assert result.passed is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py::TestCompareWorkbooks -v`
Expected: FAIL with ImportError

- [ ] **Step 3: Write the implementation**

Add to `sheetbench_runner/evaluator_v2.py`:

```python
def compare_workbooks(
    input_path: Path,
    golden_path: Path,
    output_path: Path,
    ranges: list[tuple[str, str]],
    with_font_color: bool = False,
    with_formula: bool = False,
) -> EvaluationResult:
    """
    Grade an output workbook against golden with the v2 regression/
    modification semantics over pre-parsed (sheet_name, cell_range) tuples.

    Pass rule: modification ratio 1.0 AND regression ratio 1.0, where a
    regression ratio >= 0.998 (rounded to 4 decimals) snaps to 1.0. A group
    with zero cells scores 0.0.
    """
    data_only = not with_formula
    wb_input = openpyxl.load_workbook(filename=input_path, data_only=data_only)
    wb_golden = openpyxl.load_workbook(filename=golden_path, data_only=data_only)
    wb_output = openpyxl.load_workbook(filename=output_path, data_only=data_only)
    formula_books = (
        None if with_formula else _LazyFormulaWorkbooks(input_path, golden_path, output_path)
    )

    reg_correct = reg_total = mod_correct = mod_total = 0
    errors: list[str] = []
    try:
        for sheet_name, cell_range in ranges:
            regression, modification = classify_cells_by_modification(
                wb_input, wb_golden, sheet_name, cell_range,
                with_font_color, with_formula, formula_books,
            )
            rc, rt, mc, mt, msgs = compare_classified_cells(
                wb_golden, wb_output, sheet_name, regression, modification,
                with_font_color, with_formula, formula_books,
            )
            reg_correct += rc
            reg_total += rt
            mod_correct += mc
            mod_total += mt
            errors.extend(msgs)
    finally:
        wb_input.close()
        wb_golden.close()
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
    )
```

This needs the `EvaluationResult` fields from Task 6 — implement Task 6 Step 3a (the entities change) together with this step if running strictly in order, or reorder: do the entities change first. **Recommended order: apply the Task 6 entities change (Step 3a) before running these tests.**

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_evaluator_v2.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator_v2.py tests/test_evaluator_v2.py sheetbench_runner/entities.py
git commit -m "Add v2 workbook comparison and scoring"
```

---

### Task 6: `EvaluationResult` fields and `Evaluator` dispatch

**Files:**
- Modify: `sheetbench_runner/entities.py:109-114` (`EvaluationResult`)
- Modify: `sheetbench_runner/evaluator.py:316-341` (`Evaluator.evaluate`, new `_evaluate_v2`)
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 3a (may be pulled into Task 5): Extend `EvaluationResult`**

In `sheetbench_runner/entities.py` replace:

```python
@dataclass(frozen=True)
class EvaluationResult:
    """Result of evaluating a task output against the golden file."""

    passed: bool
    message: str = ""
```

with:

```python
@dataclass(frozen=True)
class EvaluationResult:
    """Result of evaluating a task output against the golden file.

    regression_accuracy/modification_accuracy are set only by the v2 grader
    (None on the v1 path).
    """

    passed: bool
    message: str = ""
    regression_accuracy: float | None = None
    modification_accuracy: float | None = None
```

- [ ] **Step 1: Write the failing tests**

```python
from sheetbench_runner.entities import Task
from sheetbench_runner.evaluator import Evaluator


def make_v2_task(dataset_dir, input_cells, golden_cells, answer_position="'Model'!A1:A3",
                 input_name="01_01_input.xlsx"):
    """Build a v2 dataset dir with one task's files and return the Task."""
    sheets_dir = dataset_dir / "spreadsheet" / "01_proj"
    sheets_dir.mkdir(parents=True, exist_ok=True)
    build_workbook(sheets_dir / input_name, cells=input_cells)
    build_workbook(sheets_dir / "01_golden.xlsx", cells=golden_cells)
    return Task(
        id="01_01",
        instruction="test",
        spreadsheet_path=f"spreadsheet/01_proj/{input_name}",
        answer_position=answer_position,
        golden_response_path="spreadsheet/01_proj/01_golden.xlsx",
    )


class TestEvaluatorV2Dispatch:
    def test_v2_task_graded_with_v2_semantics(self, temp_dir):
        # Output differs from golden within 1% -> v2 passes where v1 would fail
        task = make_v2_task(
            temp_dir,
            input_cells={"A1": 1, "A2": 2, "A3": 3},
            golden_cells={"A1": 1, "A2": 100.0, "A3": 3},
        )
        output = build_workbook(temp_dir / "out.xlsx", cells={"A1": 1, "A2": 100.5, "A3": 3})

        result = Evaluator(temp_dir).evaluate(task, output)
        assert result.passed is True
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 1.0

    def test_v1_task_untouched(self, temp_dir):
        # v1 tasks still route to strict grading and carry no ratios
        task_dir = temp_dir / "spreadsheet" / "13-1"
        task_dir.mkdir(parents=True)
        build_workbook(task_dir / "1_13-1_golden.xlsx", sheet_name="Sheet1",
                       cells={"C1": 100.0})
        output = build_workbook(temp_dir / "out.xlsx", sheet_name="Sheet1",
                                cells={"C1": 100.5})

        result = Evaluator(temp_dir).evaluate(
            Task(id="13-1", instruction="t", spreadsheet_path="spreadsheet/13-1",
                 answer_position="C1", answer_sheet="Sheet1"),
            output,
        )
        assert result.passed is False  # 100.5 != 100.0 under strict grading
        assert result.regression_accuracy is None
        assert result.modification_accuracy is None

    def test_unreadable_input_scores_zero(self, temp_dir):
        task = make_v2_task(
            temp_dir,
            input_cells={"A1": 1},
            golden_cells={"A1": 2},
        )
        # Corrupt the input file (mirrors the DigiMark malformed-XML files)
        (temp_dir / "spreadsheet" / "01_proj" / "01_01_input.xlsx").write_bytes(b"not a zip")
        output = build_workbook(temp_dir / "out.xlsx", cells={"A1": 2})

        result = Evaluator(temp_dir).evaluate(task, output)
        assert result.passed is False
        assert result.regression_accuracy == 0.0
        assert result.modification_accuracy == 0.0
        assert "Evaluation error" in result.message

    def test_debugging_embedded_uses_formula_mode(self, temp_dir):
        # Dataset dir named *Debugging* + 'Embedded' in spreadsheet_path.
        # Range spans A1:A2 so the regression group is non-empty: A2 is empty
        # everywhere (compare_cell_formula(None, None) -> regression, correct),
        # while A1 is the modification cell. A single-cell range would leave
        # the regression group empty, which scores 0.0 and can never pass.
        dataset_dir = temp_dir / "Debugging"
        dataset_dir.mkdir()
        sheets_dir = dataset_dir / "spreadsheet" / "Embedded_case"
        sheets_dir.mkdir(parents=True)
        build_workbook(sheets_dir / "x_input.xlsx", cells={"A1": "=SUM(B1:B2)"})
        build_workbook(sheets_dir / "x_golden.xlsx", cells={"A1": "=SUM(B1:B3)"})
        output = build_workbook(temp_dir / "out.xlsx", cells={"A1": "=sum($B$1:B3)"})

        task = Task(
            id="e1", instruction="t",
            spreadsheet_path="spreadsheet/Embedded_case/x_input.xlsx",
            answer_position="'Model'!A1:A2",
            golden_response_path="spreadsheet/Embedded_case/x_golden.xlsx",
        )
        result = Evaluator(dataset_dir).evaluate(task, output)
        # Formula mode: normalized output formula matches golden -> modification correct
        assert result.passed is True
        assert result.regression_accuracy == 1.0
        assert result.modification_accuracy == 1.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_evaluator_v2.py::TestEvaluatorV2Dispatch -v`
Expected: `test_v2_task_graded_with_v2_semantics` FAILS (strict grader rejects 100.5 vs 100.0); `test_v1_task_untouched` may already pass.

- [ ] **Step 3: Write the dispatch**

In `sheetbench_runner/evaluator.py`, replace the body of `Evaluator.evaluate` (lines 316-337) and add `_evaluate_v2`:

```python
    def evaluate(self, task: Task, output_path: Path) -> EvaluationResult:
        """
        Evaluate a task output against its golden file.

        v1 tasks use strict exact-match grading; v2 tasks (marked by
        golden_response_path) use the SpreadsheetBench 2 regression/
        modification semantics.
        """
        if not output_path.exists():
            return EvaluationResult(passed=False, message="Output file not found")

        is_v2 = task.golden_response_path is not None
        golden_path = self._get_golden_path(task)
        if not golden_path.exists():
            return EvaluationResult(
                passed=False,
                message=f"Golden file not found: {golden_path}",
                # v2 load-error tasks score 0.0 so they count in the averages,
                # matching upstream's summary math
                regression_accuracy=0.0 if is_v2 else None,
                modification_accuracy=0.0 if is_v2 else None,
            )

        if is_v2:
            return self._evaluate_v2(task, golden_path, output_path)

        try:
            return self._compare_workbooks(task, golden_path, output_path)
        except Exception as e:
            return EvaluationResult(passed=False, message=f"Evaluation error: {e}")

    def _evaluate_v2(self, task: Task, golden_path: Path, output_path: Path) -> EvaluationResult:
        """Grade a v2 task with the upstream SpreadsheetBench 2 semantics."""
        # Local import: evaluator_v2 imports helpers from this module at load
        # time, so importing it at module level here would create a cycle.
        from . import evaluator_v2

        with_font_color = with_formula = False
        if "Debugging" in self.dataset_path.name:
            if "Color" in task.spreadsheet_path:
                with_font_color = True
            if "Embedded" in task.spreadsheet_path:
                with_formula = True

        try:
            ranges = _parse_sheet_cell_ranges(task.answer_position, task.answer_sheet)
            return evaluator_v2.compare_workbooks(
                self.dataset_path / task.input_relpath,
                golden_path,
                output_path,
                ranges,
                with_font_color=with_font_color,
                with_formula=with_formula,
            )
        except Exception as e:
            # Load-error tasks score 0.0, matching upstream's summary math
            return EvaluationResult(
                passed=False,
                message=f"Evaluation error: {e}",
                regression_accuracy=0.0,
                modification_accuracy=0.0,
            )
```

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -v`
Expected: all PASS, including all pre-existing `tests/test_evaluator.py` tests (proves v1 untouched).

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/evaluator.py sheetbench_runner/entities.py tests/test_evaluator_v2.py
git commit -m "Dispatch v2 tasks to the v2 grader"
```

---

### Task 7: Results plumbing — `results.json`, stats, reevaluate, CLI summary

**Files:**
- Modify: `sheetbench_runner/entities.py:116-160` (`TaskResult`)
- Modify: `sheetbench_runner/runner.py:29-45` (`RunStats`), `:190-198` (skipped-task stats), `:230-240` (`_task_completed`), `:310-321` (eval recording), `:413-427` (reevaluate)
- Modify: `sheetbench_runner/cli.py:163-176` (summary)
- Modify: `tests/test_evaluator_v2.py`

- [ ] **Step 1: Write the failing test for `TaskResult`**

```python
from sheetbench_runner.entities import TaskResult


class TestTaskResultRatios:
    def test_ratios_in_results_dict_when_set(self):
        r = TaskResult(task_id="01_01", result="fail",
                       regression_accuracy=0.9987, modification_accuracy=0.5)
        d = r.to_results_dict()
        assert d["regression_accuracy"] == 0.9987
        assert d["modification_accuracy"] == 0.5

    def test_ratios_omitted_when_none(self):
        d = TaskResult(task_id="13-1", result="pass").to_results_dict()
        assert "regression_accuracy" not in d
        assert "modification_accuracy" not in d
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_evaluator_v2.py::TestTaskResultRatios -v`
Expected: FAIL — `TaskResult.__init__() got an unexpected keyword argument`

- [ ] **Step 3: Implement the plumbing**

In `sheetbench_runner/entities.py`, add to `TaskResult` after `message: str = ""`:

```python
    regression_accuracy: float | None = None
    modification_accuracy: float | None = None
```

and in `to_results_dict`, after the `if self.message:` block:

```python
        if self.regression_accuracy is not None:
            d["regression_accuracy"] = self.regression_accuracy
        if self.modification_accuracy is not None:
            d["modification_accuracy"] = self.modification_accuracy
```

In `sheetbench_runner/runner.py`:

1. `RunStats` — add fields (spec: averages exclude missing-output tasks, which are never recorded; load-error tasks arrive as 0.0 from the evaluator):

```python
    regression_accuracies: list[float] = field(default_factory=list)
    modification_accuracies: list[float] = field(default_factory=list)
```

2. `_task_completed` — accept and accumulate ratios:

```python
    def _task_completed(
        self,
        task_id: str,
        passed: bool | None,
        regression_accuracy: float | None = None,
        modification_accuracy: float | None = None,
    ) -> None:
        """Update stats and display when a task completes."""
        self._stats.running_tasks.discard(task_id)
        if passed is True:
            self._stats.passed += 1
        elif passed is False:
            self._stats.failed += 1
        if regression_accuracy is not None:
            self._stats.regression_accuracies.append(regression_accuracy)
        if modification_accuracy is not None:
            self._stats.modification_accuracies.append(modification_accuracy)
        # Update progress bar
        if self._progress and self._progress_task is not None:
            self._progress.advance(self._progress_task)
        self._update_display()
```

3. In `_run_task` after `eval_result = self._evaluator.evaluate(...)` (line 311), carry the ratios and enrich the log line:

```python
                eval_result = self._evaluator.evaluate(task, self._run_dir.path / output_file)
                result.result = "pass" if eval_result.passed else "fail"
                result.message = eval_result.message
                result.regression_accuracy = eval_result.regression_accuracy
                result.modification_accuracy = eval_result.modification_accuracy
                result.status = TaskStatus.EVALUATED

                status_str = "PASS" if eval_result.passed else "FAIL"
                ratios = ""
                if eval_result.regression_accuracy is not None:
                    ratios = (
                        f", reg={eval_result.regression_accuracy:.4f}"
                        f", mod={eval_result.modification_accuracy:.4f}"
                    )
                logger.debug(f"Task {task.id}: {status_str} ({duration:.1f}s{ratios})")

                # Record result - only for actually evaluated tasks
                self._run_dir.record_result(result)
                self._task_completed(
                    task.id,
                    passed=eval_result.passed,
                    regression_accuracy=eval_result.regression_accuracy,
                    modification_accuracy=eval_result.modification_accuracy,
                )
                return result
```

4. In `run_all`'s skipped-task accumulation (lines 190-197), also collect stored ratios:

```python
        for task in tasks:
            if task.id in skipped_task_ids:
                result = self._run_dir.get_result(task.id)
                if result is not None:
                    if result.get("result") == "pass":
                        self._stats.passed += 1
                    elif result.get("result") == "fail":
                        self._stats.failed += 1
                    if result.get("regression_accuracy") is not None:
                        self._stats.regression_accuracies.append(result["regression_accuracy"])
                    if result.get("modification_accuracy") is not None:
                        self._stats.modification_accuracies.append(result["modification_accuracy"])
```

5. In the `reevaluate` block (after line 426 `existing["message"] = ...`):

```python
            existing["result"] = new_result
            existing["message"] = eval_result.message
            if eval_result.regression_accuracy is not None:
                existing["regression_accuracy"] = eval_result.regression_accuracy
            if eval_result.modification_accuracy is not None:
                existing["modification_accuracy"] = eval_result.modification_accuracy
```

In `sheetbench_runner/cli.py`, after the passed/failed lines in the summary (line 174):

```python
    if stats.regression_accuracies:
        avg_reg = sum(stats.regression_accuracies) / len(stats.regression_accuracies)
        avg_mod = sum(stats.modification_accuracies) / len(stats.modification_accuracies)
        print(f"  Avg regression accuracy:   {avg_reg:.4f}")
        print(f"  Avg modification accuracy: {avg_mod:.4f}")
```

- [ ] **Step 4: Run the full suite and lint**

Run: `uv run pytest -v && make lt`
Expected: all PASS, lint/typecheck clean

- [ ] **Step 5: Commit**

```bash
git add sheetbench_runner/entities.py sheetbench_runner/runner.py sheetbench_runner/cli.py tests/test_evaluator_v2.py
git commit -m "Plumb v2 accuracy ratios through results and summaries"
```

---

### Task 8: README, full verification, real-data smoke test

**Files:**
- Modify: `README.md:65-82` (v2 section caveats)

- [ ] **Step 1: Update README**

In the "SpreadsheetBench v2 datasets" section, replace the sentence "The prompt for v2 tasks omits the `instruction_type` section accordingly; grading is the same exact-match cell comparison as v1." and the second caveat bullet ("`Debugging` and `Financial_Model` answer ranges span entire workbooks ... first mismatching cell.") with:

```markdown
The prompt for v2 tasks omits the `instruction_type` section accordingly. Grading
uses the upstream SpreadsheetBench 2 semantics: cells in `answer_position` are
classified by comparing input to golden — unchanged cells are *regression* cells,
changed cells are *modification* cells — and the output is compared to golden with
1% numeric tolerance, "not meaningful" equivalence (`#DIV/0!`/`#N/A` vs `N/A`,
`NM`, dashes), and a formula-level fallback for error values. A task passes when
all modification cells match and ≥ 99.8% of regression cells are intact. Each
v2 entry in `results.json` records `regression_accuracy` and
`modification_accuracy`, and the run summary reports their averages.
```

Keep the Visualization caveat and the DigiMark caveat bullets (the DigiMark inputs now *do* affect grading — reword that bullet):

```markdown
- The five `Financial_Model/spreadsheet/06_Project DigiMark/*_input.xlsx` files
  cannot be opened by openpyxl (malformed XML namespace). These tasks fail
  evaluation with a load error and score 0.0 on both accuracy ratios — the same
  outcome as the upstream evaluator, which also reads inputs with openpyxl.
```

- [ ] **Step 2: Full verification**

Run: `make test && make lt`
Expected: all tests pass with coverage, lint/typecheck clean

- [ ] **Step 3: Real-data smoke test**

An existing Financial_Model run lives at `data/runs/2026-08-10-finance` (verify with `ls data/runs/`). The dataset lives in the sibling repo checkout: `../SpreadsheetBench/data/spreadsheetbench-v2/Financial_Model`. Regrade it:

```bash
uv run sheetbench-runner \
  --dataset ../SpreadsheetBench/data/spreadsheetbench-v2/Financial_Model \
  --run-dir data/runs/2026-08-10-finance \
  --reevaluate
```

Expected: per-task `regression_accuracy`/`modification_accuracy` appear in `results.json` for tasks with output files; some `fail -> pass` transitions are plausible; no evaluation errors except tasks with unreadable DigiMark inputs. Spot-check one regraded task's message against the upstream evaluator's output for the same file if in doubt. If the run dir or dataset is missing, skip — unit tests cover the semantics.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "Document v2 grading semantics in README"
```
