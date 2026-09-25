"""Run the real host receiver in one process so the E2E test can signal its drain."""

import asyncio
import signal

from services.executor_host.draining import DrainingReceiver
from services.executor_host.supervisor import broker


async def main() -> None:
    broker.is_worker_process = True
    finish_event = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, finish_event.set)
    await broker.startup()
    try:
        await DrainingReceiver(broker, max_prefetch=1).listen(finish_event)
    finally:
        await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
