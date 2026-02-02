FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    supervisor \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

COPY pyproject.toml uv.lock ./

COPY src ./src
COPY main.py ./
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN uv sync --frozen --no-dev

EXPOSE 8000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
