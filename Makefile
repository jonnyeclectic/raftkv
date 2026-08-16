.PHONY: install test lint demo down smoke clean-start-check run-local
install:
	uv sync --all-extras
test:
	uv run pytest
lint:
	uv run ruff check .
demo:
	docker compose up -d --wait --build
	@echo "dashboard: http://localhost:8001/"
down:
	docker compose down --volumes
smoke:
	./scripts/smoke.sh
clean-start-check:
	./scripts/clean_start_check.sh
run-local:
	./scripts/run_local.sh
