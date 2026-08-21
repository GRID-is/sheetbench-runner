.PHONY: test lint lt install lo-image lo-parity

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
lo-parity: lo-image
	@test -n "$(RUN)" || { echo "usage: make lo-parity RUN=data/runs/<run-dir> [DATASET=<dataset-dir>]"; exit 1; }
	scripts/lo_parity.sh "$(RUN)" $(if $(DATASET),"$(DATASET)")
