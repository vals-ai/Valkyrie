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

    Auto-instruments httpx here since both tracker and worker make outbound
    HTTP calls. FastAPI and SQLAlchemy are instrumented at their respective
    setup sites since they need the app/engine instance.

    Redis is intentionally NOT instrumented: all Redis usage is Taskiq broker
    plumbing (XREADGROUP polling, XAUTOCLAIM, result backend GET/SET), which
    produces high-volume, low-signal spans. Application-level tracing of
    benchmark execution is captured by manual spans on process_benchmark
    and process_task, plus trace context propagated via Taskiq message labels.

    Args:
        service_name: Identifies this process in Logfire UI.
                      Use "valkyrie-tracker" or "valkyrie-worker".
    """
    environment = os.environ.get("ENVIRONMENT", "development")

    logfire.configure(
        service_name=service_name,
        environment=environment,
        send_to_logfire="if-token-present",
        # We intentionally propagate trace context from the API into Taskiq
        # worker tasks via TracingContextMiddleware. Opt in so Logfire doesn't
        # warn on every extracted context.
        distributed_tracing=True,
    )

    # Auto-instrument outbound HTTP (covers Daytona, benchmark service, Slack webhook calls)
    logfire.instrument_httpx()
