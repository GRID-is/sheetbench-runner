# sheetbench-runner

Parallel inference runner for [SpreadsheetBench](https://github.com/RUCKBReasoning/SpreadsheetBench) that uses GRID's spreadsheet agent API to solve tasks and evaluate results inline.

## How it differs from the default SpreadsheetBench pipeline

SpreadsheetBench's built-in inference scripts ask the LLM to generate Python code (using openpyxl) to manipulate spreadsheets. The generated code runs in a Jupyter kernel to produce output `.xlsx` files. Because openpyxl cannot evaluate Excel formulas, the pipeline then opens each output file in Excel or LibreOffice to recalculate formulas before evaluation can compare cell values.

GRID's agent operates directly on a live spreadsheet engine, so output files are fully calculated `.xlsx` workbooks. This eliminates two steps from the pipeline:

1. No Python/openpyxl code generation — the agent manipulates spreadsheets directly.
2. No Excel/LibreOffice recalculation pass — output files already contain computed values.

The evaluation logic (cell-by-cell comparison with type coercion) is ported from `SpreadsheetBench/evaluation/evaluation_verified.py` to ensure compatible results.

## Install

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
make install
```

Or run directly without installing:

```bash
uv run sheetbench-runner --help
```

## Usage

Point the runner at a SpreadsheetBench dataset directory and an output directory:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench_verified_400/ \
  --run-dir data/runs/2026-02-05-my-run \
  --concurrency 10
```

To run a specific subset of tasks, use a task file:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench_verified_400/ \
  --run-dir data/runs/2026-02-05-my-run \
  --task-file task-sets/all_verified_tasks.txt \
  --concurrency 10
```

Runs are **resumable** — if interrupted, re-running the same command skips already-completed tasks and retries any that failed due to transient errors (5xx, timeouts).

### SpreadsheetBench v2 datasets

The v2 test set is split into category directories, each with its own `dataset.json`.
Point `--dataset` at one category per run:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench-v2/Template \
  --run-dir data/runs/2026-08-10-template \
  --concurrency 10
```

`Debugging`, `Financial_Model` and `Template` are supported. v2 entries reference
their input workbook (`spreadsheet_path`) and golden file (`golden_response_path`)
directly, have no `instruction_type`, and always sheet-qualify `answer_position`.
The prompt for v2 tasks omits the `instruction_type` section accordingly. Grading
uses the upstream SpreadsheetBench 2 semantics: cells in `answer_position` are
classified by comparing input to golden — unchanged cells are *regression* cells,
changed cells are *modification* cells — and the output is compared to golden with
1% numeric tolerance, "not meaningful" equivalence (`#DIV/0!`/`#N/A` vs `N/A`,
`NM`, dashes), and a formula-level fallback for error values. A task passes when
all modification cells match and ≥ 99.8% of regression cells are intact. Each
v2 entry in `results.json` records `regression_accuracy` and
`modification_accuracy`, and the run summary reports their averages.

Caveats:

- `Visualization` is **not** supported — its tasks are graded against a rubric
  (`criteria`), not cell ranges, and need a different evaluator.
- The five `Financial_Model/spreadsheet/06_Project DigiMark/*_input.xlsx` files
  cannot be opened by openpyxl (malformed XML namespace). These tasks fail
  evaluation with a load error and score 0.0 on both accuracy ratios — the same
  outcome as the upstream evaluator, which also reads inputs with openpyxl.

### Re-evaluation

If the evaluation logic changes (e.g. a parser fix for edge-case Excel references), you can re-evaluate existing output files without re-running inference:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench_verified_400/ \
  --run-dir data/runs/2026-02-05-my-run \
  --reevaluate
```

### Upstream-parity grading (LibreOffice pass)

The official SpreadsheetBench 2 protocol recalculates every output with
LibreOffice before evaluation. GRID outputs are already calculated, so
native grading skips this — the honest measure of the engine, but it
systematically differs from published numbers (LibreOffice-normalized
goldens contain artifacts such as literal `=#N/A` formulas, and stale or
uncalculated cells in outputs are recomputed rather than penalized). To
produce an upstream-comparable number:

```bash
make lo-parity RUN=data/runs/<run-dir> [DATASET=<dataset-dir>]
```

(equivalent to `docker build -t lo-recalc docker/lo-recalc/` followed by
`scripts/lo_parity.sh <run-dir> [dataset-dir]`)

Run it from the repo root; the regrade uses `uv run` so it always grades
with the current evaluator source (not the installed CLI snapshot). The
script clones the run dir to `<run-dir>-parity`, applies upstream's own
`open_spreadsheet.py` in a Linux container (macOS cannot run it natively —
and the container runs as root because LibreOffice cannot create its user
profile under an unmapped UID), regrades the copies, and prints the
native-vs-parity comparison. Report both numbers; `lo-version.txt` in the
parity dir records the LibreOffice version used. Reference point: on the
2026-08-10 Financial_Model run the pass moved 26/100 -> 40/100 with zero
downward flips.

### All options

```
Usage: sheetbench-runner [OPTIONS]

Options:
  --dataset PATH         Path to SpreadsheetBench dataset directory
                         (containing dataset.json)  [required]
  --run-dir PATH         Directory to store results (creates if missing,
                         resumes if exists)  [required]
  --task-ids TEXT        Comma-separated list of specific task IDs to run
  --task-file PATH       File with task IDs to run (one per line)
  --config PATH          Path to config.toml file
  --infuser-url TEXT     Override infuser URL from config
  --concurrency INTEGER  Number of parallel tasks (default: 4)
  --timeout INTEGER      Timeout per task in seconds (default: 3600)
  -v, --verbose          Enable verbose logging
  --reevaluate           Re-evaluate all tasks that have output files (useful
                         after parser fixes)
  --help                 Show this message and exit.
```

## Configuration

Copy `config.example.toml` to `config.toml` and adjust as needed:

```toml
[infuser]
url = "http://localhost:3000"

[runner]
concurrency = 4
timeout_seconds = 3600
```

CLI options (`--infuser-url`, `--concurrency`, `--timeout`) override their config file equivalents.

## Output

A run directory contains:

```
run-dir/
├── run.json                  # Run metadata (model, config, timestamp)
├── results.json              # Task results sorted by task_id
├── run.log                   # Execution log
├── 13-1-output.xlsx          # Output workbook for task 13-1
├── 13-1-transcript.json      # Agent transcript for task 13-1
├── 203-15-output.xlsx
├── 203-15-transcript.json
└── ...
```

Each entry in `results.json` records the task outcome, timing, and token usage:

```json
{
  "task_id": "13-1",
  "duration_seconds": 45.2,
  "turns": 5,
  "tool_calls": 8,
  "input_tokens": 12500,
  "output_tokens": 3200,
  "output_file": "13-1-output.xlsx",
  "transcript_file": "13-1-transcript.json",
  "result": "pass",
  "message": ""
}
```

## Development

```bash
make test   # run tests with coverage
make lt     # lint + typecheck
```
