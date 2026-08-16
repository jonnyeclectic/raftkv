.PHONY: install test lint run-local
install:
	uv sync --all-extras
test:
	uv run pytest
lint:
	uv run ruff check .
run-local:
	./scripts/run_local.sh
