#!/bin/bash
set -e

echo "Waiting for database..."
dockerize -wait tcp://${DB_HOST}:${DB_PORT} -timeout 120s

echo "Running database migrations..."
uv run alembic upgrade head

echo "Checking for uncommitted model changes..."
if ! uv run alembic check; then
    echo "WARNING: Model changes detected that may need a new migration."
fi

echo "Starting application..."
exec "$@"
