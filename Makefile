.PHONY: help install test test-unit test-integration test-all style style-check typecheck \
	tracker-install tracker-dev tracker-service tracker-test tracker-test-unit tracker-test-integration \
	swebench-install swebench-dev swebench-test swebench-test-unit swebench-test-integration \
	validate-workspace format lint update-submodules venv_check tool-install build

PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
SWEBENCH_PORT ?= 8001

help:
	@echo "Makefile for agentic-harness"
	@echo ""
	@echo "Setup:"
	@echo "  make install             			Install cli dependencies
	@echo "  make tracker-install     			Install tracker service (separate venv)"
	@echo "  make swebench-install    			Install swebench service (separate venv)"
	@echo ""
	@echo "Development:"
	@echo "  make style               			Lint & Format"
	@echo "  make typecheck           			Typecheck"
	@echo ""
	@echo "Testing:"
	@echo "  make test                			Run all tests (unit + integration)"
	@echo ""
	@echo "Build:"
	@echo "  make build               			Build harness CLI binary to dist/"
	@echo ""
	@echo "Services (development mode):"
	@echo "  make tracker-service     			Start tracker service docker container"
	@echo "  make swebench-dev        			Start swebench service on port $(SWEBENCH_PORT)"

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi
env-check:
	@if [ ! -f .env ]; then \
		echo "❌ Env not found! Create a .env file first."; \
		exit 1; \
	fi

install:
	uv venv --python $(PYTHON_VERSION)
	@echo "Installing cli dependencies..."
	uv cache clean model-library valsai
	uv sync --dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

tool-install:
	uv tool install -e .

build: venv_check
	@echo "Building harness CLI binary..."
	@uv run pyinstaller \
		--onefile \
		--name harness \
		--distpath dist \
		--workpath build \
		--specpath build \
		--clean \
		src/agentic_harness/cli/main.py
	@echo "✓ Binary built at dist/harness"

update-submodules:
	git submodule update --init --recursive
	git submodule update --remote --merge
	uv sync

format: venv_check
	@uv run ruff format .
lint: venv_check
	@uv run ruff check --fix .
style: format lint

typecheck: venv_check
	@uv run basedpyright

# --- Tracker Service ---
tracker-install:
	@cd services/tracker && make install
tracker-service:
	cd services/tracker && make tracker-service

# --- SWEBench service---
swebench-install:
	@cd services/benchmarks/swebench && make install
swebench-service:
	@cd services/benchmarks/swebench && make benchmark-service-local
