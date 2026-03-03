# Tracker Service

FastAPI backend that orchestrates benchmark runs, manages task lifecycle, stores artifacts in S3, and interfaces with Daytona for sandbox provisioning.

## Architecture

The service runs as two separate containers:

- **tracker** — FastAPI API server (uvicorn)
- **worker** — Task queue worker (taskiq) that processes benchmark runs

Redis and PostgreSQL run as shared infrastructure. The worker can continue processing benchmarks independently of the tracker API, allowing the tracker to be restarted without interrupting running benchmarks.

## Running

```bash
make tracker-service
```

Builds, starts, and tails logs for all services. The tracker API is available at `http://localhost:8000`.

Individual commands:

```bash
make build    # Build Docker images
make run      # Start all services
make stop     # Stop all services
make clean    # Stop and remove images
make logs     # Tail container logs
```

No `.env` file is required for local development

## Tests

```bash
make test-unit          # Unit tests + Alembic migration tests
make test-alembic       # Alembic migration tests only
make test-integration   # Integration tests
```

## Migrations

```bash
make migrate-gen        # Generate a new migration from model changes
```

See the dedicated [database README](src/tracker/database/README.md) for the full migration guide.
