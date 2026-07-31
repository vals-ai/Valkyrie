FROM python:3.12-slim

WORKDIR /app

COPY services/executor_host/requirements.txt ./
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

COPY services/executor_host/supervisor.py ./
COPY services/tracker/src/executor_protocol.py ./
RUN useradd --create-home --uid 10001 executor \
    && mkdir -p /var/cache/valkyrie-executors \
    && chown -R executor:executor /app /var/cache/valkyrie-executors

ENV PYTHONDONTWRITEBYTECODE=1
ENV EXECUTOR_CACHE_DIR=/var/cache/valkyrie-executors
ENV STABLE_QUEUE_NAME=valkyrie-stable

USER executor

CMD ["python", "-m", "taskiq", "worker", "--workers", "1", "--no-configure-logging", "supervisor:broker", "supervisor"]
