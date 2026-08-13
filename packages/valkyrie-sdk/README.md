# Valkyrie SDK

Async Python client for running benchmarks and inspecting hosted Valkyrie resources.

```bash
pip install valkyrie-sdk
```

```python
import asyncio

from valkyrie.sdk import ValkyrieClient


async def main() -> None:
    async with ValkyrieClient.from_config() as client:
        runs = await client.runs.list()
        print(runs)


asyncio.run(main())
```

See the [SDK quickstart](https://github.com/vals-ai/Valkyrie/blob/prod/docs/sdk/quickstart.mdx) and
[examples](https://github.com/vals-ai/Valkyrie/tree/prod/docs/sdk/examples).
