.PHONY: help install test test-unit test-integration test-all style style-check typecheck \
	tracker-install tracker-dev tracker-test tracker-test-unit tracker-test-integration \
	swebench-install swebench-dev \
	validate-workspace tracker-service

PYTHON_VERSION := 3.12
SWEBENCH_PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
SWEBENCH_PORT ?= 8001

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
	@echo "  make tracker-test        Run tracker service tests"
	@echo "  make tracker-test-unit   Run tracker unit tests"
	@echo "  make tracker-test-integration  Run tracker integration tests"
	@echo ""
	@echo "Services (development mode):"
	@echo "  make tracker-dev         Start tracker service on port $(TRACKER_PORT)"
	@echo "  make swebench-dev        Start swebench service on port $(SWEBENCH_PORT)"

install:
	uv venv --python $(PYTHON_VERSION)
	uv cache clean model-library valsai
	uv sync --group dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

update-submodules:
	git submodule update --remote --merge
	uv sync

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi

format: venv_check
	@uv run ruff format .

lint: venv_check
	@uv run ruff check --fix .

style: format lint

style-check: venv_check
	@uv run ruff format --check .
	@uv run ruff check .

typecheck: venv_check
	@uv run basedpyright

validate-workspace:
	@echo "Validating workspace is in sync..."
	@uv sync --all-packages --dry-run > /dev/null 2>&1 && echo "✓ All workspace packages are synced" || (echo "❌ Workspace out of sync! Run 'uv sync --all-packages'" && exit 1)

# Test commands
test: venv_check
	@uv run pytest

test-unit: venv_check
	@uv run pytest -m "not integration"

test-integration: venv_check
	@uv run pytest -m integration

# Tracker service commands
tracker-service:
	cd services/tracker && make tracker-service

tracker-test:
	@echo "Running tracker service tests..."
	@cd services/tracker && uv run pytest

tracker-test-unit:
	@echo "Running tracker unit tests..."
	@cd services/tracker && uv run pytest tests/unit

tracker-test-integration:
	@echo "Running tracker integration tests..."
	@cd services/tracker && uv run pytest tests/integration

# SWEbench service commands
swebench-install:
	@echo "Installing swebench service (separate venv)..."
	@cd services/benchmarks/swebench && uv venv --python $(SWEBENCH_PYTHON_VERSION)
	@cd services/benchmarks/swebench && uv sync
	@echo "✓ SWE-bench service installed at services/benchmarks/swebench/.venv"

swebench-dev:
	@echo "Starting swebench service (development mode on port $(SWEBENCH_PORT))..."
	# TODO: figure out this certifi thing
	@cd services/benchmarks/swebench && SSL_CERT_FILE=$$(uv run python -c "import certifi; print(certifi.where())") uv run fastapi dev main.py --port $(SWEBENCH_PORT)

swebench-test:
	@echo "Running tracker service tests..."
	@cd services/benchmarks/swebench && uv run pytest

swebench-test-unit:
	@echo "Running tracker unit tests..."
	@cd services/benchmarks/swebench && uv run pytest tests/unit

swebench-test-integration:
	@echo "Running tracker integration tests..."
	@cd services/benchmarks/swebench && uv run pytest tests/integration
