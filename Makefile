.PHONY: help install test test-unit test-integration test-all style style-check typecheck \
	tracker-install tracker-dev tracker-service tracker-test tracker-test-unit tracker-test-integration \
	swebench-install swebench-dev swebench-test swebench-test-unit swebench-test-integration \
	validate-workspace format lint update-submodules venv_check tool-install

PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
SWEBENCH_PORT ?= 8001

help:
	@echo "Makefile for agentic-harness"
	@echo ""
	@echo "Setup:"
	@echo "  make install             			Install root workspace dependencies"
	@echo "  make tracker-install     			Install tracker service (separate venv)"
	@echo "  make swebench-install    			Install swebench service (separate venv)"
	@echo ""
	@echo "Development:"
	@echo "  make style               			Lint & Format"
	@echo "  make style-check         			Check style"
	@echo "  make typecheck           			Typecheck"
	@echo "  make validate-workspace  			Check all workspace packages are in sync"
	@echo ""
	@echo "Testing:"
	@echo "  make test                			Run all tests (unit + integration)"
	@echo "  make test-unit           			Run unit tests only"
	@echo "  make test-integration    			Run integration tests only"
	@echo "  make tracker-test        			Run tracker service tests"
	@echo "  make tracker-test-unit   			Run tracker unit tests"
	@echo "  make tracker-test-integration  	Run tracker integration tests"
	@echo "  make swebench-test       			Run swebench service tests"
	@echo "  make swebench-test-unit  			Run swebench unit tests"
	@echo "  make swebench-test-integration 	Run swebench integration tests"
	@echo ""
	@echo "Services (development mode):"
	@echo "  make tracker-service     			Start tracker service docker container"
	@echo "  make swebench-dev        			Start swebench service on port $(SWEBENCH_PORT)"

install:
	uv venv --python $(PYTHON_VERSION)
	uv cache clean model-library valsai
	uv sync --group dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

tool-install:
	uv tool install -e .

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

typecheck: venv_check
	@uv run basedpyright

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

tracker-install:
	@cd services/tracker && make install

tracker-test:
	@echo "Running tracker service tests..."
	@cd services/tracker && make test

tracker-test-unit:
	@echo "Running tracker unit tests..."
	@cd services/tracker && make test-unit

tracker-test-integration:
	@echo "Running tracker integration tests..."
	@cd services/tracker && make test-integration

# SWEbench service commands
swebench-install:
	@echo "Installing swebench service (separate venv)..."
	@cd services/benchmarks/swebench && make install
	@echo "✓ SWE-bench service installed at services/benchmarks/swebench/.venv"

swebench-dev:
	@echo "Starting swebench service (development mode on port $(SWEBENCH_PORT))..."
	@cd services/benchmarks/swebench && uv run fastapi dev main.py --port $(SWEBENCH_PORT)

swebench-test:
	@echo "Running swebench service tests..."
	@cd services/benchmarks/swebench && uv run pytest

swebench-test-unit:
	@echo "Running swebench unit tests..."
	@cd services/benchmarks/swebench && uv run pytest tests/unit

swebench-test-integration:
	@echo "Running swebench integration tests..."
	@cd services/benchmarks/swebench && uv run pytest tests/integration
