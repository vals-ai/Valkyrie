# Tracker service

The FastAPI backend that the CLI and SDK call. It keeps run and task state in PostgreSQL,
schedules tasks onto sandboxes, and stores artifacts.

## Run it locally

From `services/tracker`:

```bash
make install

# Build and start Tracker and PostgreSQL with Docker Compose, then tail the logs
make tracker-service
```

- The API serves at `http://localhost:8000`. Point the CLI at it with
  `export VALKYRIE_URL=http://localhost:8000`.
- Docker Compose reads AWS credentials from your shell. Runs still need a benchmark
  service and AWS access. To run benchmarks without AWS, use `make local-tracker`; see
  [Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker).
- Rebuild the image to run changed code. Every dispatch, including a retry or resume, runs
  the code in the current image.

```bash
# Build the image
make build

# Start Tracker and PostgreSQL
make run

# Tail the Tracker logs
make logs

# Stop the stack
make stop

# Stop the stack and remove its images
make clean
```

## Tests

```bash
# Unit and local integration tests
make test

# Unit tests and Alembic migrations
make test-unit

# Alembic migration tests only
make test-alembic

# Local API and PostgreSQL tests
make test-integration-local

# AWS, benchmark service and sandbox tests
make test-integration-live
```

The Docker runtime check needs a Docker daemon. Use your Docker context's socket when it
differs:

```bash
DOCKER_HOST=unix:///var/run/docker.sock uv run pytest tests/integration/docker
```

`tests/integration/live` uses the AWS SDK default credential chain, including an AWS SSO
profile, and reads these settings from the shell or `services/tracker/.env`:

```env
# Region for Secrets Manager, S3 and CloudWatch
AWS_DEFAULT_REGION=us-east-1

# S3 bucket for agent artifacts
TEST_AWS_S3_BUCKET=

# CloudWatch log group
TEST_LOG_GROUP=

# Secrets Manager secret holding the Daytona provider settings
TEST_DAYTONA_SECRET_NAME=

# Domain of the benchmark service
BENCHMARK_SERVICE_BASE_URL=

# Cloud Map namespace used when no base URL is set
BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE=local

# Access key for the benchmark service
BENCHMARK_SERVICE_AUTH_KEY=
```

## Migrations

See [Database and migrations](src/tracker/database/README.md).
