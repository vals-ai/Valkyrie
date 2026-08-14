# Tracker Service

FastAPI backend that orchestrates benchmark runs, manages task lifecycle, stores artifacts in S3, and interfaces with Daytona for sandbox provisioning.

## Architecture

Tracker is the FastAPI control plane. It records release-bound dispatches in PostgreSQL and publishes them through Redis. Deployed ExecutorHost services consume those dispatches and run the selected immutable executor artifact.

Local Docker Compose starts only Tracker, PostgreSQL, and Redis. It does not run ExecutorHost, create an active executor release, or execute benchmarks. Use a deployed release environment for benchmark execution.

### Benchmark-service authentication

Tracker automatically forwards its inbound Descope API key only when the effective benchmark-service origin exactly matches the hosted origin derived from the benchmark name and Tracker configuration. Custom benchmark-service origins do not receive the Tracker key. They can still use explicit service-owned headers or secret-backed service authentication.

## Sandbox scheduling

`SANDBOX_QUEUE_ENABLED` is `false` by default. In that mode, runs create sandboxes directly and requests with an
explicit priority are rejected. When the flag is `true`, Tracker queues runs only when the selected sandbox provider
has a managed admission pool. Unmanaged providers continue to use direct execution and also reject explicit priority.

Managed shared pools order waiting tasks by run priority, enqueue time, and task ID. Priority ranges from P0 (highest)
to P4 (lowest), and admitted runs default to P3 when no priority is supplied. Per-run `concurrency` still limits how
many tasks from that run may be active.

Authenticated clients can inspect their organization's scheduler state with:

```text
GET /scheduler/overview?waiting_limit=100&active_limit=100
```

The endpoint uses the request's resolved organization and never returns rows from another organization. Its summary
contains total waiting, building, in-progress, and evaluating counts. It also returns per-pool waiting counts plus
bounded waiting and active entries. Each entry limit defaults to 100, accepts 1–200, and has a corresponding
`waiting_capped` or `active_capped` flag when more rows exist. Active entries use `BUILDING`, `IN_PROGRESS`, or
`EVALUATING` status.

## Running

```bash
make tracker-service
```

Builds and starts the local API and infrastructure, then tails Tracker logs. The Tracker API is available at `http://localhost:8000`.

Individual commands:

```bash
make build    # Build the Tracker image
make run      # Start Tracker, PostgreSQL, and Redis
make stop     # Stop the local stack
make clean    # Stop the stack and remove local images
make logs     # Tail Tracker logs
```

No `.env` file is required for local API development (Docker Compose reads AWS credentials from your shell environment).

Optional catalog config for local tracker-service use:

```env
BENCHMARK_CATALOG_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com
```

Tracker-service reads this when running `valkyrie config service list` to show the catalog of benchmarks hosted at that endpoint.

## Tests

```bash
make test                    # Unit + local integration tests with 85% total coverage
make test-unit               # Unit tests + Alembic migrations
make test-alembic            # Alembic migration tests only
make test-integration-local  # Local API + Postgres tests
make test-integration-live   # AWS + benchmark service + sandbox tests
```

`tests/integration/live` requires a `.env` file at `services/tracker/.env`:

```env
# AWS — used to fetch sandbox provider credentials from Secrets Manager and write to S3/CloudWatch
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
AWS_SESSION_TOKEN=              # Optional, required when using temporary credentials

# Test infrastructure
TEST_AWS_S3_BUCKET=             # S3 bucket for agent artifacts (e.g. agentic-harness)
TEST_LOG_GROUP=                 # CloudWatch log group (e.g. valkyrie-test-log-group)
TEST_DAYTONA_SECRET_NAME=       # AWS Secrets Manager secret containing Daytona provider config (e.g. YourSandboxProviderSecret)

# Benchmark service
BENCHMARK_SERVICE_BASE_URL=     # Use the domain of the benchmark service
BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE=local  # Cloud Map namespace fallback when no base URL is set
BENCHMARK_SERVICE_AUTH_KEY=     # Access key for authenticating with a benchmark service
```

## Migrations

```bash
make migrate-gen        # Generate a new migration from model changes
```

See the dedicated [database README](src/tracker/database/README.md) for the full migration guide.
