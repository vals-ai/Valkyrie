# Valkyrie SDK

Async Python client for running benchmarks and inspecting hosted Valkyrie resources.

With uv installed and a Python 3.12+ virtual environment activated:

```bash
uv pip install --exclude-newer "7 days" --exclude-newer-package certifi=false valkyrie-sdk
```

```python
from valkyrie.sdk import ValkyrieClient
```

See the [SDK guide](https://github.com/vals-ai/Valkyrie/tree/prod/docs/sdk) and
[examples](https://github.com/vals-ai/Valkyrie/tree/prod/docs/sdk/examples).
