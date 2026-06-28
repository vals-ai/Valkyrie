.PHONY: help install style format format-check lint typecheck \
	tracker-service venv_check tool-install build unit-test

PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
help:
	@echo "Makefile for Valkyrie"
	@echo ""
	@echo "Setup:"
	@echo "  make install             Install cli dependencies"
	@echo "  make tool-install        Install valkyrie as a global executable"
	@echo ""
	@echo "Development:"
	@echo "  make unit-test           Run CLI unit tests with coverage"
	@echo "  make style               Lint & Format"
	@echo "  make typecheck           Typecheck"
	@echo ""
	@echo "Build:"
	@echo "  make build               Build valkyrie CLI binary to dist/"
	@echo ""
	@echo "Services:"
	@echo "  make tracker-service     Start tracker service docker container"

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
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
	@echo "Building valkyrie CLI binary..."
	@uv run pyinstaller \
		--onefile \
		--name valkyrie \
		--distpath dist \
		--workpath build \
		--specpath build \
		--clean \
		src/valkyrie/cli/main.py
	@echo "✓ Binary built at dist/valkyrie"

format: venv_check
	@uv run ruff format .

format-check: venv_check
	@uv run ruff format --check .

lint: venv_check
	@uv run ruff check --fix .

unit-test: venv_check
	@uv run pytest tests/unit --cov=src/valkyrie --cov-report=xml --cov-report=term-missing

style: format lint

typecheck: venv_check
	@uv run basedpyright

tracker-service:
	cd services/tracker && make tracker-service

