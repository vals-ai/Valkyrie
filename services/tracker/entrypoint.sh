#!/bin/bash
set -e

# Only run migrations when starting the API server.
if echo "$@" | grep -q -e "tracker.serve" -e "uvicorn"; then
    echo "Running database migrations..."
    uv run --no-sync alembic upgrade head

    echo "Checking for uncommitted model changes..."
    if ! uv run --no-sync alembic check; then
        echo "WARNING: Model changes detected that may need a new migration."
    fi
fi

echo "Starting application..."
exec "$@"
