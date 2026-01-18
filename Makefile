PYTHON_VERSION := 3.12
SWEBENCH_PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
SWEBENCH_PORT ?= 8001

.PHONY: help
help:
	@echo "Makefile for agentic-harness"
	@echo ""
	@echo "Setup:"
	@echo "  make install             Install root workspace dependencies"
	@echo "  make tracker-install     Install tracker service (separate venv)"
	@echo "  make swebench-install    Install swebench service (separate venv)"
	@echo ""
	@echo "Development:"
	@echo "  make style               Lint & Format"
	@echo "  make style-check         Check style"
	@echo "  make typecheck           Typecheck"
	@echo "  make validate-workspace  Check all workspace packages are in sync"
	@echo ""
	@echo "Testing:"
	@echo "  make test                Run all tests (unit + integration)"
	@echo "  make test-unit           Run unit tests only"
	@echo "  make test-integration    Run integration tests only"
	@echo ""
	@echo "Services (development mode):"
	@echo "  make tracker-service     Start tracker service docker container"
	@echo "  make tracker-dev         Start tracker service on port $(TRACKER_PORT)"
	@echo "  make swebench-dev        Start swebench service on port $(SWEBENCH_PORT)"

.PHONY: install
install:
	uv venv --python $(PYTHON_VERSION)
	uv cache clean model-library valsai
	uv sync --group dev
	uv pip install -e services/tracker
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

.PHONY: update-submodules
update-submodules:
	git submodule update --remote --merge
	uv sync

.PHONY: venv_check
venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi

.PHONY: format
format: venv_check
	@uv run ruff format .

.PHONY: lint
lint: venv_check
	@uv run ruff check --fix .

.PHONY: style
style: format lint

.PHONY: style-check
style-check: venv_check
	@uv run ruff format --check .
	@uv run ruff check .

.PHONY: typecheck
typecheck: venv_check
	@uv run basedpyright

# Test commands
.PHONY: test
test: venv_check
	@uv run pytest

.PHONY: test-unit
test-unit: venv_check
	@uv run pytest -m "not integration"

.PHONY: test-integration
test-integration: venv_check
	@uv run pytest -m integration

# Tracker service commands
.PHONY: tracker-service
tracker-service:
	cd services/tracker && make tracker-service

.PHONY: tracker-install
tracker-install:
	@echo "Installing tracker service (separate venv)..."
	@cd services/tracker && uv venv --python $(TRACKER_PYTHON_VERSION)
	@cd services/tracker && uv sync
	@echo "✓ Tracker service installed at services/tracker/.venv"

.PHONY: tracker-dev
tracker-dev:
	@echo "Starting tracker service (development mode on port $(TRACKER_PORT))..."
	@cd services/tracker && uv run fastapi dev main.py --port $(TRACKER_PORT)

# SWEbench service commands
.PHONY: swebench-install
swebench-install:
	@echo "Installing swebench service (separate venv)..."
	@cd services/benchmarks/swebench && uv venv --python $(SWEBENCH_PYTHON_VERSION)
	@cd services/benchmarks/swebench && uv sync
	@echo "✓ SWE-bench service installed at services/benchmarks/swebench/.venv"

.PHONY: swebench-dev
swebench-dev:
	@echo "Starting swebench service (development mode on port $(SWEBENCH_PORT))..."
	@cd services/benchmarks/swebench && uv run fastapi dev main.py --port $(SWEBENCH_PORT)
