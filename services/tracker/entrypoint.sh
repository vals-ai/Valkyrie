#!/bin/bash
set -e

echo "Running database migrations..."
uv run alembic upgrade head

echo "Checking for uncommitted model changes..."
if ! uv run alembic check; then
    echo "WARNING: Model changes detected that may need a new migration."
fi

echo "Starting application..."
exec "$@"