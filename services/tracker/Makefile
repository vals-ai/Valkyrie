PYTHON_VERSION := 3.12
PORT ?= 8000
IMAGE_NAME := tracker-service
IMAGE_TAG := latest

# Local Python environment targets
install:
	uv venv --python $(PYTHON_VERSION)
	uv sync

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

# Docker targets
build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

dev:
	docker run --rm \
		--env-file .env \
		-v $$(pwd):/app \
		-p $(PORT):8000 \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		uv run fastapi dev main.py --host 0.0.0.0 --port 8000

prod:
	docker run -d \
		-p $(PORT):8000 \
		--name tracker-prod \
		$(IMAGE_NAME):$(IMAGE_TAG)
