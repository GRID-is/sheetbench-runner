.PHONY: test lint lt install

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
