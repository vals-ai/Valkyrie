.PHONY: help install build run stop clean logs test tracker-service test test-integration test-unit test-alembic

IMAGE_NAME := tracker-service
CONTAINER_NAME := tracker-service-container

help:
	@echo "Available commands:"
	@echo "  make install - Install local development environment"
	@echo "  make build  - Build the Docker image"
	@echo "  make run    - Run the Docker container with environment variables from .env"
	@echo "  make stop   - Stop and remove the Docker container"
	@echo "  make clean  - Remove the Docker image and container"
	@echo "  make logs   - View container logs"
	@echo "  make tracker-service - Build, run, and view logs for the tracker service"

install:
	@echo "Installing tracker service (separate venv)..."
	uv venv --python 3.12
	uv cache clean agentic-harness
	uv sync --group dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME) .

run:
	@echo "Starting Redis container..."
	@docker run -d \
		--name tracker-redis \
		-p 6379:6379 \
		redis:7-alpine || echo "Redis container already running"
	@echo "Starting tracker service container..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found!"; \
		exit 1; \
	fi
	@docker run -d \
		--name $(CONTAINER_NAME) \
		-p 8000:8000 \
		--env-file .env \
		-e REDIS_URL=redis://host.docker.internal:6379 \
		-v ${HOME}/.aws:/root/.aws:ro \
		$(IMAGE_NAME)
	@echo "Containers started successfully!"
	@echo "Tracker service is running on http://localhost:8000"
	@echo "Health check: http://localhost:8000/health"

stop:
	@echo "Stopping and removing containers..."
	@docker stop $(CONTAINER_NAME) 2>/dev/null || true
	@docker rm $(CONTAINER_NAME) 2>/dev/null || true
	@docker stop tracker-redis 2>/dev/null || true
	@docker rm tracker-redis 2>/dev/null || true
	@echo "Containers stopped and removed."

clean: stop
	@echo "Removing Docker image..."
	@docker rmi $(IMAGE_NAME) 2>/dev/null || true
	@echo "Cleanup complete."

logs:
	@docker logs -f $(CONTAINER_NAME)


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