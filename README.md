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
  --solve-profile solve-profile.json \
  --concurrency 10
```

To run a specific subset of tasks, use a task file:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench_verified_400/ \
  --run-dir data/runs/2026-02-05-my-run \
  --task-file task-sets/all_verified_tasks.txt \
  --solve-profile solve-profile.json \
  --concurrency 10
```

Runs are **resumable** — if interrupted, re-running the same command skips already-completed tasks and retries any that failed due to transient errors (5xx, timeouts).

### Re-evaluation

If the evaluation logic changes (e.g. a parser fix for edge-case Excel references), you can re-evaluate existing output files without re-running inference:

```bash
sheetbench-runner \
  --dataset data/spreadsheetbench_verified_400/ \
  --run-dir data/runs/2026-02-05-my-run \
  --reevaluate
```

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
  --solve-profile PATH   Non-secret solve profile JSON file
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

[solve]
profile = "solve-profile.json"

[runner]
concurrency = 4
timeout_seconds = 3600
```

CLI options (`--infuser-url`, `--solve-profile`, `--concurrency`, `--timeout`)
override their config file equivalents.

The solve profile is JSON containing `models`, `modelRoles`, and optional
`ttlSeconds`. It is non-secret and must not contain API keys or legacy credential
fields. Each model's required `apiKeyEnv` directly names the environment variable
containing its API key and must match the portable syntax
`[A-Za-z_][A-Za-z0-9_]*` (maximum 64 characters). Models may share an environment
variable or name different variables. The runner resolves each key before HTTP and
submits it inline as that model's `apiKey`; no top-level credentials object is sent.
See `solve-profile.example.json` for the shape.

For every non-reevaluation invocation, the runner creates one short-lived solve
context before running tasks, uses it for all workbook uploads and solves, and
deletes it on exit. API key values and the context ID remain in process memory
and are never written to run artifacts. Run metadata stores only the server's
sanitized configuration (`transport`, `model`, and optional `options` per model),
so neither `apiKey` nor `apiKeyEnv` is persisted. A pure `--reevaluate` run makes
no server requests and does not require a solve profile.

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
