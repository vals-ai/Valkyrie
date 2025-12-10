.PHONY: help install test test-integration test-all style style-check typecheck
PYTHON_VERSION := 3.11

help:
	@echo "Makefile for agentic-harness"
	@echo "Usage:"
	@echo "  make install          Install dependencies"
	@echo "  make style            Lint & Format"
	@echo "  make style-check      Check style"
	@echo "  make typecheck        Typecheck"

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

