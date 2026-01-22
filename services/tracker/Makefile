.PHONY: help setup build run stop clean logs tracker-service test test-integration test-unit test-alembic migrate

help:
	@echo "Available commands:"
	@echo "  make setup           - Install local development environment and dependencies"
	@echo "  make build           - Build Docker images"
	@echo "  make run             - Start all services with docker-compose"
	@echo "  make stop            - Stop all services"
	@echo "  make clean           - Stop services and remove images"
	@echo "  make logs            - View container logs"
	@echo "  make tracker-service - Build, run, and view logs for the tracker service"
	@echo "  make migrate         - Run database migrations"

setup:
	@echo "Installing tracker service (separate venv)..."
	uv venv --python 3.12
	uv cache clean agentic-harness
	uv sync --group dev
	@echo "Creating database..."
	make migrate
	@echo "Database created and migrations applied."

build:
	@echo "Building Docker images..."
	@docker-compose build

run:
	@echo "Starting services with docker-compose..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found!"; \
		exit 1; \
	fi
	@docker-compose up -d
	@echo "Services started successfully!"
	@echo "Tracker service is running on http://localhost:8000"
	@echo "Health check: http://localhost:8000/health"

stop:
	@echo "Stopping services..."
	@docker-compose down
	@echo "Services stopped."

clean:
	@echo "Stopping services and removing images..."
	@docker-compose down --rmi local
	@echo "Cleanup complete."

logs:
	@docker-compose logs -f tracker


tracker-service:
	@echo "Building tracker service..."
	@make clean
	@make build
	@make run
	@make logs

test:
	uv run pytest

test-integration:
	uv run pytest tests/integration

test-unit:
	uv run pytest tests/unit --test-alembic

test-alembic:
	uv run pytest --test-alembic -m alembic

migrate:
	@echo "Running database migrations..."
	uv run alembic upgrade head