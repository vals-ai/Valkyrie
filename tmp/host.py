"""Adapt only the receiver startup to the source revision being tested."""

import asyncio
import importlib.util
import signal

from taskiq.receiver import Receiver
from services.executor_host.supervisor import broker


async def main() -> None:
    broker.is_worker_process = True
    receiver_type = Receiver
    if importlib.util.find_spec("services.executor_host.draining") is not None:
        from services.executor_host.draining import DrainingReceiver

        receiver_type = DrainingReceiver
    finish = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, finish.set)
    await broker.startup()
    try:
        await receiver_type(broker, max_prefetch=1).listen(finish)
    finally:
        await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
