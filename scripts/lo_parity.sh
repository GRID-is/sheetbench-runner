#!/usr/bin/env bash
# LO-parity grading: run a sheetbench run directory's outputs through the
# official SpreadsheetBench 2 LibreOffice preprocessing (Dockerized), then
# regrade the copies and compare against the native grading.
#
# Usage (from the repo root): scripts/lo_parity.sh <run-dir> [dataset-dir]
#
# The container runs as root; LibreOffice cannot create its profile under an
# arbitrary --user UID (no passwd entry). On Docker Desktop (macOS/Windows)
# bind-mount ownership is mapped to the host user anyway; on a Linux host the
# parity copies will come out root-owned.
set -euo pipefail

RUN_DIR=${1:?usage: lo_parity.sh <run-dir> [dataset-dir]}
DATASET=${2:-../SpreadsheetBench/data/spreadsheetbench-v2/Financial_Model}
RUN_DIR=${RUN_DIR%/}
PARITY_DIR="${RUN_DIR}-parity"
IMAGE=lo-recalc

command -v docker >/dev/null || { echo "docker not found in PATH" >&2; exit 1; }
docker image inspect "$IMAGE" >/dev/null 2>&1 \
  || { echo "image '$IMAGE' not built; run: docker build -t $IMAGE docker/lo-recalc/" >&2; exit 1; }
[ -f "$RUN_DIR/results.json" ] || { echo "$RUN_DIR/results.json not found" >&2; exit 1; }

echo "==> cloning $RUN_DIR -> $PARITY_DIR"
rm -rf "$PARITY_DIR"
mkdir -p "$PARITY_DIR"
cp "$RUN_DIR/run.json" "$RUN_DIR/results.json" "$PARITY_DIR/"
find "$RUN_DIR" -maxdepth 1 -name '*-output.xlsx' ! -name '~$*' \
  -exec cp {} "$PARITY_DIR/" \;
COPY_STAMP="$PARITY_DIR/.copy-stamp"
touch "$COPY_STAMP"

echo "==> LibreOffice pass (official open_spreadsheet.py)"
docker run --rm -v "$(cd "$PARITY_DIR" && pwd)":/data "$IMAGE" | tee "$PARITY_DIR/lo-pass.log"

{ docker run --rm "$IMAGE" soffice --version; date; } > "$PARITY_DIR/lo-version.txt"

# The upstream script exits 0 even if the LO service never started and no
# file was touched; fail loudly rather than regrade un-recalculated copies.
if grep -q "Cannot start LibreOffice service" "$PARITY_DIR/lo-pass.log"; then
  echo "ERROR: LibreOffice service failed to start inside the container" >&2
  exit 1
fi
if [ -z "$(find "$PARITY_DIR" -maxdepth 1 -name '*-output.xlsx' -newer "$COPY_STAMP" -print -quit)" ]; then
  echo "ERROR: no output file was rewritten by the LibreOffice pass" >&2
  exit 1
fi
rm -f "$COPY_STAMP"

echo "==> regrading parity copies"
TASK_IDS=$(python3 -c "import json; print(','.join(r['task_id'] for r in json.load(open('$PARITY_DIR/results.json'))))")
# uv run grades with the repo's CURRENT evaluator source; the bare
# `sheetbench-runner` command is a uv-tool snapshot that goes stale until
# `make install` (a known gotcha in this repo).
uv run sheetbench-runner --dataset "$DATASET" --run-dir "$PARITY_DIR" \
  --reevaluate --task-ids "$TASK_IDS"

echo "==> native vs parity"
python3 - "$RUN_DIR/results.json" "$PARITY_DIR/results.json" <<'EOF'
import json, sys
native = {r['task_id']: r for r in json.load(open(sys.argv[1]))}
parity = {r['task_id']: r for r in json.load(open(sys.argv[2]))}
def stats(d):
    p = sum(1 for r in d.values() if r.get('result') == 'pass')
    def avg(key):
        vals = [r[key] for r in d.values() if r.get(key) is not None]
        return sum(vals) / len(vals) if vals else 0.0
    return p, avg('regression_accuracy'), avg('modification_accuracy')
np, nr, nm = stats(native)
pp, pr, pm = stats(parity)
print(f"native: {np}/{len(native)} passes, avg regression {nr:.4f}, avg modification {nm:.4f}")
print(f"parity: {pp}/{len(parity)} passes, avg regression {pr:.4f}, avg modification {pm:.4f}")
for tid in sorted(native):
    a, b = native[tid].get('result'), parity.get(tid, {}).get('result')
    if a != b:
        print(f"  {tid}: {a} -> {b} (mod {native[tid].get('modification_accuracy')} -> {parity[tid].get('modification_accuracy')})")
EOF
