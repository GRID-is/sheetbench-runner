# sheetbench-runner

Parallel inference runner for SpreadsheetBench with inline evaluation.

## Install

```bash
make install
```

## Usage

```
$ sheetbench-runner --help
Usage: sheetbench-runner [OPTIONS]

  Parallel inference runner for SpreadsheetBench with inline evaluation.

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
  --help                 Show this message and exit.
```

## Development

```bash
make test   # run tests
make lt     # lint + typecheck
```
