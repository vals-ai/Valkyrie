.PHONY: help build run stop clean logs tracker-service \
        test test-integration-local test-integration-live test-unit test-coverage test-alembic \
        migrate-gen venv-check

PYTHON_VERSION := 3.12
PYTEST := uv run pytest
COMPOSE := docker compose

help:
	@echo "Available commands:"
	@echo "  make install         - Install local development environment and dependencies"
	@echo "  make build           - Build Docker images"
	@echo "  make run             - Start all services with docker-compose"
	@echo "  make stop            - Stop all services"
	@echo "  make clean           - Stop services and remove images"
	@echo "  make logs            - View container logs"
	@echo "  make tracker-service - Build, run, and view logs for the tracker service"
	@echo "  make migrate-gen     - Generate a new migration from model changes"

# --- Setup ---
venv-check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi
install:
	uv venv --python $(PYTHON_VERSION)
	@echo "Installing tracker service dependencies..."
	uv cache clean agentic-harness
	uv sync --dev

# --- Docker ---
build:
	@echo "Building Docker images..."
	@$(COMPOSE) build
run:
	@echo "Starting services with docker-compose..."
	@$(COMPOSE) up -d
	@echo "Tracker service is running on http://localhost:8000"
	@echo "Health check: http://localhost:8000/health"
stop:
	@echo "Stopping services..."
	@$(COMPOSE) down
clean:
	@echo "Stopping services and removing images..."
	@$(COMPOSE) down --rmi local
logs:
	@$(COMPOSE) logs -f tracker worker
tracker-service: build run logs

# --- Tests ---
test: test-coverage
test-unit: venv-check
	$(PYTEST) tests/unit --test-alembic --cov=src/tracker --cov=main --cov-report=term-missing --cov-report=xml
test-coverage: venv-check
	uv run coverage erase
	$(PYTEST) tests/unit --test-alembic --cov=src/tracker --cov=main --cov-report= --cov-fail-under=0
	$(PYTEST) tests/integration/local/api -n auto --cov=src/tracker --cov=main --cov-append --cov-report= --cov-fail-under=0
	$(PYTEST) tests/integration/local/database --cov=src/tracker --cov=main --cov-append --cov-report=term-missing --cov-report=xml --cov-fail-under=85
test-alembic: venv-check
	$(PYTEST) --test-alembic -m alembic
test-integration-local: venv-check
	uv run coverage erase
	$(PYTEST) tests/integration/local/api -n auto --cov=src/tracker --cov=main --cov-report= --cov-fail-under=0
	$(PYTEST) tests/integration/local/database --cov=src/tracker --cov=main --cov-append --cov-report=term-missing --cov-report=xml --cov-fail-under=0
test-integration-live: venv-check
	uv run coverage erase
	$(PYTEST) tests/integration/live -n auto --cov=src/tracker --cov=main --cov-report=term-missing --cov-report=xml --cov-fail-under=0

# --- Migrations ---
migrate-gen: venv-check
	@read -p "Migration message: " msg; \
	uv run alembic revision --autogenerate -m "$$msg"
