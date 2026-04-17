"""Logfire tracing configuration for the tracker service.

Configures Pydantic Logfire (built on OpenTelemetry) for distributed tracing
and correlated structured logs. Called once per process at startup.
"""

import os

import logfire


def configure_logfire(service_name: str) -> None:
    """Configure Logfire tracing for the given service.

    Must be called before any instrumentation hooks (instrument_fastapi,
    instrument_sqlalchemy, etc.) since those depend on an active
    TracerProvider being configured.

    Auto-instruments httpx and Redis here since both tracker and worker
    use them. FastAPI and SQLAlchemy are instrumented at their respective
    setup sites since they need the app/engine instance.

    Args:
        service_name: Identifies this process in Logfire UI.
                      Use "valkyrie-tracker" or "valkyrie-worker".
    """
    environment = os.environ.get("ENVIRONMENT", "development")

    logfire.configure(
        service_name=service_name,
        environment=environment,
        send_to_logfire="if-token-present",
    )

    # Auto-instrument outbound HTTP (covers Daytona, benchmark service, Slack webhook calls)
    logfire.instrument_httpx()

    # Auto-instrument Redis commands (covers Taskiq broker + result backend)
    logfire.instrument_redis()
