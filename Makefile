.PHONY: help install style format lint typecheck \
	tracker-service update-submodules venv_check tool-install build

PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
help:
	@echo "Makefile for agentic-harness"
	@echo ""
	@echo "Setup:"
	@echo "  make install             Install cli dependencies"
	@echo "  make tool-install        Install harness as a global executable"
	@echo "  make update-submodules   Init and update git submodules"
	@echo ""
	@echo "Development:"
	@echo "  make style               Lint & Format"
	@echo "  make typecheck           Typecheck"
	@echo ""
	@echo "Build:"
	@echo "  make build               Build harness CLI binary to dist/"
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

tracker-service:
	cd services/tracker && make tracker-service

