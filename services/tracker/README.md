# Tracker Service

FastAPI backend that orchestrates benchmark runs, manages task lifecycle, stores artifacts in S3, and interfaces with Daytona for sandbox provisioning.

## Architecture

The service runs as two separate containers:

- **tracker** — FastAPI API server (uvicorn)
- **worker** — Task queue worker (taskiq) that processes benchmark runs

Redis and PostgreSQL run as shared infrastructure. The worker can continue processing benchmarks independently of the tracker API, allowing the tracker to be restarted without interrupting running benchmarks.

## Eval-only retry

Some benchmark services can persist enough evaluation state to retry evaluation without rerunning agent generation.

During fresh evaluation, the tracker still creates a Daytona sandbox, runs the agent, and calls `evaluate_instance`. If the benchmark service emits an `eval_resume_state` stream chunk, the tracker stores that opaque object on the task row.

On retry, tasks with stored `eval_resume_state` start in `EVALUATING`. The worker skips sandbox creation and calls `evaluate_response` with the saved state. The benchmark service owns the state shape and decides how to resume. For example, the state can point at uploaded artifacts, an external eval job, or completed partial checks.

Use retry-from-scratch when generation itself must be rerun; eval-only retry is only for reusing existing generated output.

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
