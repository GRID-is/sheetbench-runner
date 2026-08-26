#!/usr/bin/env bash
# Run all gradable v2 categories, each into its own run directory, then print
# a combined summary. One run dir per category is deliberate: task ids
# overlap across categories (Financial_Model, Debugging and Template all use
# NN_NN ids), so mixing them in one directory would collide.
#
# Usage (from the repo root):
#   scripts/bench_all.sh <run-prefix> [extra sheetbench-runner args...]
# Example:
#   scripts/bench_all.sh data/runs/2026-08-21 --concurrency 2
set -euo pipefail

PREFIX=${1:?usage: bench_all.sh <run-prefix> [extra runner args...]}
shift || true
DATASET_ROOT=${DATASET_ROOT:-../SpreadsheetBench/data/spreadsheetbench-v2}
CATEGORIES=(Financial_Model Debugging Template)

slug() { echo "$1" | tr '[:upper:]_' '[:lower:]-'; }

for cat in "${CATEGORIES[@]}"; do
  run_dir="${PREFIX}-$(slug "$cat")"
  echo "==> $cat -> $run_dir"
  uv run sheetbench-runner \
    --dataset "$DATASET_ROOT/$cat" \
    --run-dir "$run_dir" \
    "$@"
done

echo "==> combined summary"
python3 - "$PREFIX" "${CATEGORIES[@]}" <<'EOF'
import json, sys
prefix = sys.argv[1]
total_pass = total_done = 0
for cat in sys.argv[2:]:
    slug = cat.lower().replace('_', '-')
    path = f"{prefix}-{slug}/results.json"
    try:
        res = json.load(open(path))
    except FileNotFoundError:
        print(f"{cat:16} no results ({path})")
        continue
    p = sum(1 for r in res if r.get('result') == 'pass')
    mods = [r['modification_accuracy'] for r in res if r.get('modification_accuracy') is not None]
    avg = sum(mods) / len(mods) if mods else 0.0
    print(f"{cat:16} {p}/{len(res)} passes, avg modification {avg:.4f}")
    total_pass += p
    total_done += len(res)
if total_done:
    print(f"{'TOTAL':16} {total_pass}/{total_done} passes ({100*total_pass/total_done:.1f}%)")
EOF
