"""Tests for v2 (SpreadsheetBench 2) evaluation semantics."""

import openpyxl
import pytest
from openpyxl.styles import Font
from openpyxl.styles.colors import Color

from sheetbench_runner.evaluator_v2 import (
    _find_sheet,
    _has_excel_error,
    compare_cell_formula,
    compare_cell_value,
    compare_font_color,
)


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
