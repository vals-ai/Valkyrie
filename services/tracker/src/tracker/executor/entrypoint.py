"""Entrypoint packaged into immutable executor PEX artifacts."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path
from typing import Any, cast

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.utils.run_orchestration import process_benchmark


async def _run_executor(payload: dict[str, Any]) -> None:
    task = asyncio.create_task(
        process_benchmark(
            start_benchmark_request_json=payload.get("start_benchmark_request_json"),
            benchmark_id_str=payload.get("benchmark_id_str"),
            verified_task_ids=payload.get("verified_task_ids"),
            execution_context_json=payload.get("execution_context_json"),
            executor_dispatch_id=payload["executor_dispatch_id"],
        )
    )
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        return
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("payload", nargs="?", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        print(json.dumps({"protocol_version": SUPPORTED_PROTOCOL_VERSION}))
        return
    if args.payload is None:
        parser.error("payload is required")

    decoded: object = json.loads(args.payload.read_text())
    if not isinstance(decoded, dict):
        raise SystemExit("Invalid executor payload: expected object")
    payload = cast(dict[str, Any], decoded)
    asyncio.run(_run_executor(payload))


if __name__ == "__main__":
    main()
