FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    supervisor \
    gcc \
    curl \
    wget \
    && wget -qO- https://github.com/jwilder/dockerize/releases/download/v0.7.0/dockerize-linux-amd64-v0.7.0.tar.gz | tar xz -C /usr/local/bin \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

COPY pyproject.toml uv.lock alembic.ini ./

COPY src ./src
COPY main.py ./
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh

RUN uv sync --frozen --no-dev && chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
