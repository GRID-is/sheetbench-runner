"""Tests for the cell-level grading findings the evaluation walk records."""

import json

import openpyxl
import pytest
from openpyxl.styles import Font
from openpyxl.styles.colors import Color

from sheetbench_runner.dataset import Dataset
from sheetbench_runner.entities import TaskResult, TaskStatus
from sheetbench_runner.evaluator_v2 import compare_workbooks
from sheetbench_runner.findings import GRADER_VERSION, task_findings
from sheetbench_runner.run_directory import RunDirectory
from sheetbench_runner.runner import run as run_tasks


def build_workbook(path, sheet_name="Model", cells=None, fonts=None):
    """Write an xlsx with the given {cell: value} and optional {cell: Font}."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for cell, value in (cells or {}).items():
        ws[cell] = value
    for cell, font in (fonts or {}).items():
        ws[cell].font = font
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


@pytest.fixture
def books(temp_dir):
    """input, golden and output paths in a temporary directory."""
    return (
        temp_dir / "input.xlsx",
        temp_dir / "golden.xlsx",
        temp_dir / "output.xlsx",
    )


class TestGradingDetail:
    """compare_workbooks records what it rejected, alongside the score."""

    def test_records_the_rejected_cell_with_its_classification_and_values(self, books):
        # Arrange: B1 is the task's work and wrong; B2 must stay put and does.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "B2": 7})
        build_workbook(golden_path, cells={"B1": 2, "B2": 7})
        build_workbook(output_path, cells={"B1": 3, "B2": 7})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B2")])

        # Assert
        assert result.grading is not None
        assert result.grading.regression.model_dump() == {
            "correct": 1,
            "total": 1,
            "wrong": 0,
            "accuracy": 1.0,
        }
        assert result.grading.modification.model_dump() == {
            "correct": 0,
            "total": 1,
            "wrong": 1,
            "accuracy": 0.0,
        }
        (mismatch,) = result.grading.mismatches
        assert mismatch.sheet == "Model"
        assert mismatch.cell == "B1"
        assert mismatch.classification == "modification"
        assert mismatch.criterion == "value"
        assert mismatch.expected.value == "2"
        assert mismatch.actual.value == "3"

    def test_records_a_regression_cell_as_a_regression_cell(self, books):
        # Arrange: B1 is untouched work, and the output breaks it.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 7})
        build_workbook(golden_path, cells={"B1": 7})
        build_workbook(output_path, cells={"B1": 9})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])

        # Assert
        (mismatch,) = result.grading.mismatches
        assert mismatch.classification == "regression"
        assert result.grading.regression.wrong == 1
        assert result.grading.modification.total == 0

    def test_counts_agree_with_the_message_the_grader_writes(self, books):
        # Arrange: two wrong modification cells out of three.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "B2": 1, "B3": 1})
        build_workbook(golden_path, cells={"B1": 2, "B2": 2, "B3": 2})
        build_workbook(output_path, cells={"B1": 9, "B2": 9, "B3": 2})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B3")])

        # Assert: one walk, so the message's counts are the findings' counts.
        detail = result.grading
        assert (
            f"{detail.modification.wrong}/{detail.modification.total} modification"
            in result.message
        )
        assert f"{detail.regression.wrong}/{detail.regression.total} regression" in result.message
        assert len(detail.mismatches) == detail.regression.wrong + detail.modification.wrong

    def test_records_the_accuracies_the_result_reports(self, books):
        # Arrange
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "B2": 1})
        build_workbook(golden_path, cells={"B1": 2, "B2": 2})
        build_workbook(output_path, cells={"B1": 2, "B2": 9})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B2")])

        # Assert
        assert result.modification_accuracy == result.grading.modification.accuracy
        assert result.grading.modification.accuracy == 0.5

    def test_records_the_grading_configuration(self, books):
        # Arrange
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1})
        build_workbook(golden_path, cells={"B1": 2})
        build_workbook(output_path, cells={"B1": 2})

        # Act
        result = compare_workbooks(
            input_path,
            golden_path,
            output_path,
            [("Model", "B1:B1")],
            numeric_tolerance_mode="combined",
        )

        # Assert
        assert result.grading.numeric_tolerance_mode == "combined"
        assert result.grading.tolerance == 0.01
        assert result.grading.with_font_color is False
        assert result.grading.with_formula is False
        assert result.grading.grader_version == GRADER_VERSION

    def test_finds_nothing_when_every_cell_matches(self, books):
        # Arrange: one modification cell the output gets right, one untouched cell.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "B2": 7})
        build_workbook(golden_path, cells={"B1": 2, "B2": 7})
        build_workbook(output_path, cells={"B1": 2, "B2": 7})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B2")])

        # Assert
        assert result.passed is True
        assert result.grading.mismatches == []
        assert result.grading.notes == []

    def test_the_tolerance_mode_decides_whether_a_near_zero_cell_is_rejected(self, books):
        # Arrange: Cashflow!E31 of Financial_Model 20_02.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 0.5})
        build_workbook(golden_path, cells={"B1": -2.6193447411060333e-10})
        build_workbook(output_path, cells={"B1": -1.80007191374898e-8})

        # Act
        relative = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])
        combined = compare_workbooks(
            input_path,
            golden_path,
            output_path,
            [("Model", "B1:B1")],
            numeric_tolerance_mode="combined",
        )

        # Assert
        assert [m.cell for m in relative.grading.mismatches] == ["B1"]
        assert combined.grading.mismatches == []

    def test_the_tolerance_mode_decides_the_classification_totals(self, books):
        # Arrange: input and golden differ only by near-zero noise.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": -2.6193447411060333e-10})
        build_workbook(golden_path, cells={"B1": -1.80007191374898e-8})
        build_workbook(output_path, cells={"B1": -1.80007191374898e-8})

        # Act
        relative = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])
        combined = compare_workbooks(
            input_path,
            golden_path,
            output_path,
            [("Model", "B1:B1")],
            numeric_tolerance_mode="combined",
        )

        # Assert
        assert (relative.grading.modification.total, relative.grading.regression.total) == (1, 0)
        assert (combined.grading.modification.total, combined.grading.regression.total) == (0, 1)


class TestCriteria:
    """The criterion names the comparison that rejected the cell."""

    def test_font_color_when_the_value_matches_and_the_color_does_not(self, books):
        # Arrange: the golden recolors B1; the output has the value but not the color.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1})
        build_workbook(
            golden_path, cells={"B1": 1}, fonts={"B1": Font(color=Color(rgb="FFFF0000"))}
        )
        build_workbook(output_path, cells={"B1": 1})

        # Act
        result = compare_workbooks(
            input_path, golden_path, output_path, [("Model", "B1:B1")], with_font_color=True
        )

        # Assert
        (mismatch,) = result.grading.mismatches
        assert mismatch.criterion == "font_color"
        assert mismatch.expected.font_color == "FFFF0000"
        assert mismatch.actual.font_color == "FF000000"
        assert result.grading.with_font_color is True

    def test_value_when_both_the_value_and_the_color_differ(self, books):
        # Arrange
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1})
        build_workbook(
            golden_path, cells={"B1": 2}, fonts={"B1": Font(color=Color(rgb="FFFF0000"))}
        )
        build_workbook(output_path, cells={"B1": 9})

        # Act
        result = compare_workbooks(
            input_path, golden_path, output_path, [("Model", "B1:B1")], with_font_color=True
        )

        # Assert
        (mismatch,) = result.grading.mismatches
        assert mismatch.criterion == "value"

    def test_formula_when_the_task_grades_formulas(self, books):
        # Arrange
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "C1": "=B1"})
        build_workbook(golden_path, cells={"B1": 1, "C1": "=B1*2"})
        build_workbook(output_path, cells={"B1": 1, "C1": "=B1*3"})

        # Act
        result = compare_workbooks(
            input_path, golden_path, output_path, [("Model", "C1:C1")], with_formula=True
        )

        # Assert
        (mismatch,) = result.grading.mismatches
        assert mismatch.criterion == "formula"
        assert mismatch.expected.formula == "=B1*2"
        assert mismatch.actual.formula == "=B1*3"
        assert result.grading.with_formula is True

    def test_formula_on_error_when_a_cell_shows_an_excel_error(self, books):
        # Arrange: the output leaves an error where the golden has a number.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1})
        build_workbook(golden_path, cells={"B1": 2})
        build_workbook(output_path, cells={"B1": "#DIV/0!"})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])

        # Assert
        (mismatch,) = result.grading.mismatches
        assert mismatch.criterion == "formula_on_error"
        assert mismatch.actual.value == "#DIV/0!"

    def test_records_the_formula_behind_a_value_mismatch(self, books):
        # Arrange: openpyxl writes the formula, so the cached value is absent; the grader
        # reads None on both sides at the value level and rejects on the formula fallback.
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "C1": 5})
        build_workbook(golden_path, cells={"B1": 2, "C1": 5})
        build_workbook(output_path, cells={"B1": 9, "C1": 5})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])

        # Assert: no formula on either side, so both formula fields are the literal.
        (mismatch,) = result.grading.mismatches
        assert mismatch.expected.formula == "2"
        assert mismatch.actual.formula == "9"


class TestSheetLevelFindings:
    """A sheet the output does not have is not a cell-level finding."""

    def test_a_missing_sheet_is_a_note_and_scores_every_cell_wrong(self, books):
        # Arrange
        input_path, golden_path, output_path = books
        build_workbook(input_path, sheet_name="Model", cells={"B1": 1})
        build_workbook(golden_path, sheet_name="Model", cells={"B1": 2})
        build_workbook(output_path, sheet_name="Elsewhere", cells={"B1": 2})

        # Act
        result = compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B1")])

        # Assert
        assert result.grading.notes == ["Model worksheet not found"]
        assert result.grading.mismatches == []
        assert result.grading.modification.wrong == 1
        assert "Model worksheet not found" in result.message


class TestPersistence:
    """A run directory holds findings as a per-task artifact, like the transcript."""

    def _graded(self, books) -> object:
        input_path, golden_path, output_path = books
        build_workbook(input_path, cells={"B1": 1, "B2": 7})
        build_workbook(golden_path, cells={"B1": 2, "B2": 7})
        build_workbook(output_path, cells={"B1": 3, "B2": 7})
        return compare_workbooks(input_path, golden_path, output_path, [("Model", "B1:B2")])

    def test_records_the_artifact_and_names_it_in_results(self, temp_dir, books):
        # Arrange
        result = self._graded(books)
        run_dir = RunDirectory(temp_dir / "run")
        run_dir.path.mkdir()
        run_dir.load()
        task_result = TaskResult(
            task_id="20_02",
            status=TaskStatus.EVALUATED,
            result="fail",
            findings=task_findings("20_02", result.grading, result.passed),
        )

        # Act
        run_dir.record_result(task_result)

        # Assert
        assert (run_dir.path / "20_02-findings.json").exists()
        recorded = json.loads(run_dir.results_path.read_text())
        assert recorded[0]["findings_file"] == "20_02-findings.json"

    def test_the_artifact_carries_the_counts_the_grader_reported(self, temp_dir, books):
        # Arrange
        result = self._graded(books)
        run_dir = RunDirectory(temp_dir / "run")
        run_dir.path.mkdir()
        run_dir.load()

        # Act
        run_dir.write_findings(task_findings("20_02", result.grading, result.passed))
        written = run_dir.read_findings("20_02")

        # Assert
        assert written.schema_version == 1
        assert written.modification.wrong == result.grading.modification.wrong
        assert written.modification.total == result.grading.modification.total
        assert written.regression.wrong == result.grading.regression.wrong
        assert [m.cell for m in written.mismatches] == [m.cell for m in result.grading.mismatches]
        assert written.grading.numeric_tolerance_mode == "relative"

    def test_a_result_without_findings_names_none(self, temp_dir):
        # Arrange: the v1 grader and load errors produce no cell-level grading.
        run_dir = RunDirectory(temp_dir / "run")
        run_dir.path.mkdir()
        run_dir.load()

        # Act
        run_dir.record_result(
            TaskResult(task_id="13-1", status=TaskStatus.EVALUATED, result="pass")
        )

        # Assert
        recorded = json.loads(run_dir.results_path.read_text())
        assert "findings_file" not in recorded[0]
        assert run_dir.read_findings("13-1") is None

    def test_reading_findings_of_a_run_that_has_none_is_none(self, temp_dir):
        run_dir = RunDirectory(temp_dir / "run")
        run_dir.path.mkdir()

        assert run_dir.read_findings("20_02") is None


class TestReevaluate:
    """
    Re-evaluating a run grades its saved outputs again and records the findings. It is the
    only way an old run gets them, and it updates the scores as any grading does.
    """

    def _run_directory(self, temp_dir, mode="combined", golden=None, output=None):
        """A v2 dataset and a run directory holding a saved output and transcript."""
        dataset = temp_dir / "dataset"
        build_workbook(dataset / "in.xlsx", cells={"B1": 1, "B2": 7})
        build_workbook(dataset / "golden.xlsx", cells=golden or {"B1": 2, "B2": 7})
        (dataset / "dataset.json").write_text(
            json.dumps(
                [
                    {
                        "id": "20_02",
                        "instruction": "Fill B1",
                        "spreadsheet_path": "in.xlsx",
                        "golden_response_path": "golden.xlsx",
                        "answer_position": "'Model'!B1:B2",
                    }
                ]
            )
        )
        run = temp_dir / "run"
        build_workbook(run / "20_02-output.xlsx", cells=output or {"B1": 3, "B2": 7})
        (run / "20_02-transcript.json").write_text('{"turns": []}')
        (run / "run.json").write_text(
            json.dumps(
                {
                    "model": "claude-sonnet-5",
                    "git_hash": "abc1234",
                    "dataset_path": str(dataset.resolve()),
                    "created_at": "2026-09-10T18:24:02.285903",
                    "numeric_tolerance_mode": mode,
                }
            )
        )
        (run / "results.json").write_text(
            json.dumps(
                [
                    {
                        "task_id": "20_02",
                        "duration_seconds": 12.5,
                        "turns": 4,
                        "output_file": "20_02-output.xlsx",
                        "transcript_file": "20_02-transcript.json",
                        "result": "fail",
                        "message": "stale message from the original grading",
                        "regression_accuracy": 1.0,
                        "modification_accuracy": 0.0,
                    }
                ],
                indent=2,
            )
        )
        return dataset, run

    async def _reevaluate(self, dataset, run, mode="combined"):
        return await run_tasks(
            dataset_path=dataset,
            run_dir_path=run,
            solve_server_url="http://localhost:3000",
            solve_profile_path=None,
            tasks=Dataset(dataset).all_tasks,
            reevaluate=True,
            numeric_tolerance_mode=mode,
        )

    async def test_records_the_findings_and_names_them_in_results(self, temp_dir):
        # Arrange
        dataset, run = self._run_directory(temp_dir)

        # Act
        await self._reevaluate(dataset, run)

        # Assert
        assert (run / "20_02-findings.json").exists()
        recorded = json.loads((run / "results.json").read_text())[0]
        assert recorded["findings_file"] == "20_02-findings.json"

    async def test_the_findings_hold_the_cells_the_grader_rejected(self, temp_dir):
        # Arrange
        dataset, run = self._run_directory(temp_dir)

        # Act
        await self._reevaluate(dataset, run)

        # Assert
        findings = RunDirectory(run).read_findings("20_02")
        assert [(m.sheet, m.cell, m.classification) for m in findings.mismatches] == [
            ("Model", "B1", "modification")
        ]
        assert findings.modification.model_dump() == {
            "correct": 0,
            "total": 1,
            "wrong": 1,
            "accuracy": 0.0,
        }
        assert findings.regression.wrong == 0

    async def test_the_findings_record_the_grading_that_was_applied(self, temp_dir):
        # Arrange: the operator names the mode, and the artifact records the one used.
        dataset, run = self._run_directory(temp_dir, mode="relative")

        # Act
        await self._reevaluate(dataset, run, mode="relative")

        # Assert
        findings = RunDirectory(run).read_findings("20_02")
        assert findings.grading.numeric_tolerance_mode == "relative"
        assert findings.grading.grader_version == GRADER_VERSION
        assert findings.grading.grader == "evaluator_v2"
        assert json.loads((run / "run.json").read_text())["numeric_tolerance_mode"] == "relative"

    async def test_updates_the_recorded_score_and_message(self, temp_dir):
        # Arrange: an output that is in fact right, recorded as a failure.
        dataset, run = self._run_directory(temp_dir, output={"B1": 2, "B2": 7})

        # Act
        await self._reevaluate(dataset, run)

        # Assert
        recorded = json.loads((run / "results.json").read_text())[0]
        assert recorded["result"] == "pass"
        assert recorded["message"] == ""
        assert recorded["modification_accuracy"] == 1.0
        assert RunDirectory(run).read_findings("20_02").mismatches == []

    async def test_leaves_the_saved_output_and_transcript_alone(self, temp_dir):
        # Arrange
        dataset, run = self._run_directory(temp_dir)
        output_before = (run / "20_02-output.xlsx").read_bytes()
        transcript_before = (run / "20_02-transcript.json").read_bytes()

        # Act
        await self._reevaluate(dataset, run)

        # Assert
        assert (run / "20_02-output.xlsx").read_bytes() == output_before
        assert (run / "20_02-transcript.json").read_bytes() == transcript_before
        recorded = json.loads((run / "results.json").read_text())[0]
        assert recorded["output_file"] == "20_02-output.xlsx"
        assert recorded["transcript_file"] == "20_02-transcript.json"
        assert (recorded["turns"], recorded["duration_seconds"]) == (4, 12.5)

    async def test_the_tolerance_mode_decides_the_counts_it_records(self, temp_dir):
        # Arrange: near-zero noise, which only combined tolerance accepts.
        dataset, run = self._run_directory(
            temp_dir,
            golden={"B1": -2.6193447411060333e-10, "B2": 7},
            output={"B1": -1.80007191374898e-8, "B2": 7},
        )

        # Act
        await self._reevaluate(dataset, run)

        # Assert
        findings = RunDirectory(run).read_findings("20_02")
        assert findings.mismatches == []
        assert findings.grading.numeric_tolerance_mode == "combined"
