"""Exercise application sampling configuration in isolated SDK processes."""

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest


@pytest.mark.parametrize(
    ("overrides", "expected_roots"),
    [
        ({}, 10),
        ({"LOGFIRE_TRACE_SAMPLE_RATE": "1"}, 100),
        ({"OTEL_TRACES_SAMPLER_ARG": "1"}, 100),
        ({"LOGFIRE_TRACE_SAMPLE_RATE": "0.2", "OTEL_TRACES_SAMPLER_ARG": "1"}, 20),
    ],
)
def test_root_sampling_and_parent_decisions(overrides: dict[str, str], expected_roots: int) -> None:
    # SDK providers and HTTP instrumentation are process-global; isolate each configuration.
    env = os.environ.copy()
    for key in ("LOGFIRE_TRACE_SAMPLE_RATE", "OTEL_TRACES_SAMPLER_ARG", "OTEL_TRACES_SAMPLER"):
        env.pop(key, None)
    env.update(overrides)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            dedent(
                """
                import json
                from functools import partial
                from unittest.mock import patch

                import logfire
                from opentelemetry import trace
                from opentelemetry.context import Context
                from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
                from tracker.observability.tracing import configure_tracing

                class UniformTraceIds(RandomIdGenerator):
                    def __init__(self):
                        self.ids = iter((1 << 64) | (i * (1 << 64) // 100 + 1) for i in range(100))

                    def generate_trace_id(self):
                        return next(self.ids)

                configure = partial(
                    logfire.configure,
                    advanced=logfire.AdvancedOptions(id_generator=UniformTraceIds()),
                )
                with patch.object(logfire, "configure", configure):
                    configure_tracing("sampling-test", "test")

                tracer = trace.get_tracer("sampling-test")
                roots = 0
                for _ in range(100):
                    with tracer.start_as_current_span("root", context=Context()) as span:
                        roots += span.is_recording()

                parents = []
                for remote in (False, True):
                    for sampled in (False, True):
                        # Choose IDs that would make the opposite decision if resampled.
                        context = trace.SpanContext(
                            trace_id=(1 << 128) - 1 if sampled else 1,
                            span_id=1,
                            is_remote=remote,
                            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED if sampled else 0),
                        )
                        parent = trace.set_span_in_context(trace.NonRecordingSpan(context), Context())
                        with tracer.start_as_current_span("child", context=parent) as span:
                            parents.append(span.is_recording())
                print(json.dumps({"roots": roots, "parents": parents}))
                """
            ),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert json.loads(result.stdout) == {"roots": expected_roots, "parents": [False, True, False, True]}
