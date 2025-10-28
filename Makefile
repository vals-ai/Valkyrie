.PHONY: help install test test-integration test-all style style-check typecheck

help:
	@echo "Makefile for agentic-harness"
	@echo "Usage:"
	@echo "  make install          Install dependencies"
	@echo "  make test             Run unit tests"
	@echo "  make test-integration Run integration tests (requires API keys)"
	@echo "  make test-all         Run all tests (unit + integration)"
	@echo "  make style            Lint & Format"
	@echo "  make style-check      Check style"
	@echo "  make typecheck        Typecheck"

install:
	uv venv
	uv sync --group dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi

test: venv_check
	@echo "Running unit tests..."
	@uv run pytest -m "not integration"

test-integration: venv_check
	@echo "Running integration tests (requires API keys)..."
	@uv run pytest -m "not unit"

test-all: venv_check
	@echo "Running all tests..."
	@uv run pytest

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

