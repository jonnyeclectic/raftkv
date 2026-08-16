.PHONY: install test lint
install:
	uv sync --all-extras
test:
	uv run pytest
lint:
	uv run ruff check .
