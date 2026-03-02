# Tracker Service

FastAPI backend that orchestrates benchmark runs, manages task lifecycle, stores artifacts in S3, and interfaces with Daytona for sandbox provisioning.

## Environment

Create a `.env` file in `services/tracker/`:

```env
BENCHMARK_SERVICE_URL=http://host.docker.internal:8001
```

Database (PostgreSQL), Redis, and AWS credentials are configured automatically by docker-compose. AWS credentials are mounted from `~/.aws`.

## Running

```bash
make tracker-service
```

Builds, starts, and tails logs for the tracker service (with Postgres and Redis sidecars). Available at `http://localhost:8000`.

Individual commands:

```bash
make build    # Build Docker images
make run      # Start all services
make stop     # Stop all services
make clean    # Stop and remove images
make logs     # Tail container logs
```

## Tests

```bash
make test-unit          # Unit tests + Alembic migration tests
make test-alembic       # Alembic migration tests only
make test-integration   # Integration tests (requires .env)
```

## Migrations

```bash
make migrate-gen        # Generate a new migration from model changes
```

See the dedicated [database README](src/tracker/database/README.md) for the full migration guide.
