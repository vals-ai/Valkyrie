#!/bin/bash
set -e

echo "Checking for pending model changes..."
if ! uv run alembic check; then
    echo "ERROR: Alembic check failed. Database schema may be out of sync."
    exit 1
fi

echo "Running database migrations..."
uv run alembic upgrade head

echo "Starting application..."
exec "$@"
