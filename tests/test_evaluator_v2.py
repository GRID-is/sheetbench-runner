"""Tests for v2 (SpreadsheetBench 2) evaluation semantics."""

import openpyxl
import pytest
from openpyxl.styles import Font
from openpyxl.styles.colors import Color

from sheetbench_runner.evaluator_v2 import (
    _find_sheet,
    _has_excel_error,
    _LazyFormulaWorkbooks,
    classify_cells_by_modification,
    compare_cell_formula,
    compare_cell_value,
    compare_classified_cells,
    compare_font_color,
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

    @pytest.mark.parametrize(
        "golden,output",
        [
            ("#DIV/0!", "N/A"),
            ("#N/A", "NM"),
            ("#DIV/0!", "n.m."),
            ("#N/A", "—"),
            ("-", "--"),
            ("Not Meaningful", "#DIV/0!"),
        ],
    )
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

    def test_theme_color_with_negative_tint(self):
        # Negative tint darkens: 4472C4 at tint -0.5 -> 223962
        # int(0x44*0.5)=0x22, int(0x72*0.5)=0x39, int(0xC4*0.5)=0x62
        themed = Font(color=Color(theme=4, tint=-0.5))
        assert compare_font_color(themed, Font(color="FF223962")) is True


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
        # Golden and output both show #DIV/0! stored as literal strings; the
        # error triggers the formula-level fallback which compares equal.
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
