.PHONY: test lint lt install lo-image lo-parity bench-all parity-all

test:
	uv run pytest tests/ -v --cov=sheetbench_runner --cov-report=term-missing

lint:
	uv run ruff format sheetbench_runner/ tests/
	uv run ruff check sheetbench_runner/ tests/

lt: lint typecheck

typecheck:
	uv run mypy sheetbench_runner/

install:
	uv build
	uv tool install --force dist/*.whl

test_%:
	uv run pytest --tb=short -vs -k $@ tests/

lo-image:
	docker build -t lo-recalc docker/lo-recalc/

# Usage: make lo-parity RUN=data/runs/<run-dir> [DATASET=<dataset-dir>]
# DATASET defaults to the dataset recorded in the run's run.json.
lo-parity: lo-image
	@test -n "$(RUN)" || { echo "usage: make lo-parity RUN=data/runs/<run-dir> [DATASET=<dataset-dir>]"; exit 1; }
	scripts/lo_parity.sh "$(RUN)" $(if $(DATASET),"$(DATASET)")

# Run all gradable v2 categories into <PREFIX>-<category> run dirs + summary.
# Usage: make bench-all PREFIX=data/runs/<date> [ARGS="--concurrency 2"]
bench-all:
	@test -n "$(PREFIX)" || { echo "usage: make bench-all PREFIX=data/runs/<date> [ARGS=...]"; exit 1; }
	scripts/bench_all.sh "$(PREFIX)" $(ARGS)

# LO-parity pass over all three category run dirs of a bench-all prefix.
# Usage: make parity-all PREFIX=data/runs/<date>
parity-all: lo-image
	@test -n "$(PREFIX)" || { echo "usage: make parity-all PREFIX=data/runs/<date>"; exit 1; }
	for d in financial-model debugging template; do \
	  test -f "$(PREFIX)-$$d/results.json" && scripts/lo_parity.sh "$(PREFIX)-$$d" || echo "skipping $(PREFIX)-$$d (no results)"; \
	done
