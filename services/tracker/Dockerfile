FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    supervisor \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY src ./src
COPY main.py ./
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN uv run python src/tracker/database/session.py

EXPOSE 8000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
