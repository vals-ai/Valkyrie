FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY services/tracker/pyproject.toml services/tracker/uv.lock services/tracker/alembic.ini ./

COPY services/tracker/src ./src
COPY services/tracker/main.py ./
COPY services/tracker/entrypoint.sh /entrypoint.sh

RUN uv sync --frozen --no-dev
RUN chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
