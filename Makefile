.PHONY: test lint lt install

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check sheetbench_runner/ tests/

lt:
	uv run ruff check sheetbench_runner/ tests/
	uv run mypy sheetbench_runner/

install:
	uv build
	pip install --user --force-reinstall dist/*.whl
