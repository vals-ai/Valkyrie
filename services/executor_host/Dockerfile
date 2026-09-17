FROM python:3.12-slim

WORKDIR /app

COPY services/executor_host/requirements.txt ./
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

COPY services/executor_host/__init__.py services/executor_host/observability.py services/executor_host/supervisor.py ./services/executor_host/
COPY services/executor_host/local/ ./services/executor_host/local/
COPY services/tracker/src/executor_protocol.py ./
COPY services/tracker/src/tracker/__init__.py ./tracker/
COPY services/tracker/src/tracker/aws/__init__.py services/tracker/src/tracker/aws/executor_artifacts.py ./tracker/aws/
COPY services/tracker/src/tracker/local/__init__.py services/tracker/src/tracker/local/executor_artifacts.py services/tracker/src/tracker/local/secret_pipe.py ./tracker/local/
COPY services/tracker/src/tracker/runtime/__init__.py services/tracker/src/tracker/runtime/executor_artifacts.py services/tracker/src/tracker/runtime/lifecycle.py ./tracker/runtime/
RUN useradd --create-home --uid 10001 executor \
    && mkdir -p /var/cache/valkyrie-executors \
    && chown -R executor:executor /app /var/cache/valkyrie-executors

ENV PYTHONDONTWRITEBYTECODE=1
ENV EXECUTOR_CACHE_DIR=/var/cache/valkyrie-executors
ENV STABLE_QUEUE_NAME=valkyrie-stable

USER executor

CMD ["python", "-m", "taskiq", "worker", "--workers", "1", "--no-configure-logging", "services.executor_host.supervisor:broker", "services.executor_host.supervisor"]
