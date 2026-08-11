# V2 evaluation semantics for SpreadsheetBench v2 tasks

**Date:** 2026-08-11
**Status:** Approved (design), pending implementation

## Problem

sheetbench-runner grades SpreadsheetBench v2 tasks (`Debugging`, `Financial_Model`,
`Template`) with the v1 grader: strict cell-by-cell exact match (numbers rounded to
2 decimals) over the entire `answer_position`, binary pass/fail. The upstream v2
evaluator (`SpreadsheetBench-2/evaluation/evaluation.py`) grades differently:
tolerant value comparison plus a regression/modification split with a lenient pass
rule. Because v2 answer ranges span whole workbooks (4k–295k cells) and inputs
differ from goldens in 1–22% of cells, our strict grading yields pass rates near
zero and results that cannot be compared with published SpreadsheetBench 2 numbers.

## Goal

For v2 tasks, produce per-task results comparable to the upstream v2 evaluator,
without changing v1 grading in any way (historical `spreadsheetbench_verified_400`
runs must stay comparable).

Non-goals: the `Visualization` category (rubric/VLM-graded, still unsupported);
LibreOffice recalculation (our outputs are already fully calculated); upstream's
`.traj` interaction-turn extraction (we already track turns).

## Approach

Vendor the upstream comparison/classification/scoring logic into a new module and
dispatch to it for v2 tasks. Alternatives rejected:

- *Retrofit the existing evaluator with mode flags*: the two semantics conflict
  (type-check ordering, tolerance, short-circuit vs full-count); interleaving risks
  perturbing v1 grading and makes upstream-equivalence hard to verify.
- *Invoke the upstream script directly*: it is a CLI in a separate repository with
  a "DO NOT EDIT" header, hardcoded `DATA_ROOT`, and output-naming assumptions.

## Design

### New module `sheetbench_runner/evaluator_v2.py`

A close port of the upstream functions, kept structurally parallel to
`SpreadsheetBench-2/evaluation/evaluation.py` so the two can be diffed:

- `compare_cell_value(v1, v2, tolerance=0.01)` — ArrayFormula text comparison;
  "not meaningful" equivalence class (`#DIV/0!`/`#N/A` ≍ `N/A`, `NA`, `N.M.`,
  `NM`, `NOT MEANINGFUL`, dashes, etc.); numeric comparison with 1% relative
  tolerance (0.01 absolute when one side is 0); `transform_value` normalization
  (round 2dp, datetime→Excel serial, numeric strings parsed); `None` ≍ `""` and
  `None` ≍ `0`; case-insensitive `$`-stripped formula-string comparison.
- `compare_cell_formula(cell1, cell2)` — formula comparison normalizing case,
  `$`, and the legacy `=+` prefix; falls back to `compare_cell_value` for
  non-formula values.
- `_find_sheet(wb, name)` — whitespace-tolerant, case-insensitive sheet lookup.
- `_has_excel_error(cell)` — detects Excel error strings (`#REF!`, `#VALUE!`, …).
- `classify_cells_by_modification(...)` — compares *input* vs *golden* per cell in
  each answer range: matching cells are **regression** (must stay untouched),
  differing cells are **modification** (the task's actual work). Sheets missing
  from input or golden contribute no cells (upstream behavior).
- Cell counting — compares *output* vs *golden* for both groups. When either cell
  holds an Excel error value (and not in formula mode), comparison falls back to
  the formula-level workbooks. A sheet missing from the output scores all its
  cells wrong. No short-circuiting: every cell is counted so ratios are exact.
- Font-color support for Debugging: `_get_color_rgb` (theme→RGB resolution with
  tint), `_compare_colors` (RGB-only, alpha ignored), `compare_font_color`.

Range parsing reuses the existing `_parse_sheet_cell_ranges` from `evaluator.py`
(a superset of upstream's parser; v2 positions are always sheet-qualified, and
both parsers handle quoted sheet names containing commas identically). Cell-name
expansion reuses the existing `_generate_cell_names` with the sheets' max row as
the extent; all v2 answer ranges are fully bounded (verified across all three
datasets), so this is equivalent to upstream's `generate_cell_names`.

### Dispatch and comparison modes

`Evaluator.evaluate` routes a task to the v2 path when `task.golden_response_path`
is set (the existing v2 marker in `entities.py`). The v2 path additionally loads
the *input* workbook via `task.input_relpath`.

Comparison mode mirrors upstream's `process_single_item`: value mode for all
categories; when the dataset directory name contains `Debugging`, tasks whose
`spreadsheet_path` contains `Color` use font-color mode and those containing
`Embedded` use formula mode (workbooks loaded `data_only=False`).

Formula-level (`data_only=False`) copies of input/golden/output are needed only
for the Excel-error fallback; they are loaded lazily on the first error cell
encountered rather than eagerly. Same semantics as upstream, roughly half the
load time on clean workbooks.

### Scoring and pass rule

Per task, aggregate regression and modification `correct/total` across all answer
ranges, then compute ratios rounded to 4 decimals (upstream rounding). A group
with zero cells scores 0.0, as upstream does — a task with an empty modification
group can never pass; ported verbatim for comparability. A regression ratio
≥ 0.998 snaps to 1.0; the modification ratio gets no slack.
`passed = (regression == 1.0 and modification == 1.0)`.

### Result plumbing

- `EvaluationResult` gains optional `regression_accuracy: float | None` and
  `modification_accuracy: float | None` (both `None` on the v1 path).
- `TaskResult` carries the two fields through; `to_results_dict` includes them in
  `results.json` when present.
- The failure `message` stays the first mismatch, in upstream's format
  (`Modification error at sheet!cell: answer=X, output=Y`) plus mismatch counts
  per group, e.g. `"…; 3/1200 regression and 41/85 modification cells wrong"`.
- The runner's per-task log line and end-of-run summary include average
  regression/modification accuracy for v2 tasks. To stay comparable with
  upstream's summary numbers, the averages exclude missing-output tasks but
  include load-error tasks as 0.0.
- `--reevaluate` needs no changes and is the validation path: regrade an existing
  Financial_Model run dir and compare per-task results against upstream's
  evaluator run on the same outputs.

### Error handling

- Unreadable workbooks (notably the five `06_Project DigiMark/*_input.xlsx` files
  with malformed XML namespaces) fail evaluation with the load error as message —
  identical outcome to upstream, which also loads inputs with openpyxl.
- Missing output file keeps the existing "Output file not found" failure.

### Testing

Unit tests with small openpyxl-built fixtures (following existing `conftest.py`
patterns):

- Comparator cases: tolerance boundaries (1% relative, absolute-at-zero),
  not-meaningful pairs, `None`/`""`/`0` equivalences, datetime serials,
  formula normalization (`$`, case, `=+`), error-value formula fallback.
- Classification: regression vs modification assignment; missing sheets.
- Scoring: the 0.998 snap threshold on regression; modification gets no snap;
  message contents.
- End-to-end: output=golden passes; output=input yields regression 1.0 /
  modification 0.0; a v1 task still takes the strict path unchanged.
- Font-color and formula modes for Debugging: theme-color resolution and a
  Color/Embedded mode-selection test.

### Documentation

Update `README.md`: replace the "expect pass rates near zero" caveat with a
description of v2 grading (regression/modification split, tolerance, pass rule)
and the new `results.json` fields.
