In order to run a benchmark you must setup a config. Currently the config would look something like this

```yaml
agent_config:
  model: grok/grok-4-1-fast-non-reasoning
  extra:
    max_turns: 20

benchmark: fab
agent: edgar_agent
dataset:
  name: fab
  suite_id: e5c0ea52-3791-404b-8bfc-6325973b2faa
  project_id: testing-sfrbyo
```

the required fields are

- `benchamrk: str` - source the benchmark runner at`benchmarks.{benchmark_id}.benchmark`
- `agent: str` - source the agent contract under `contracts.{agent}`
- `dataset: dict[str, Any]` - all context passed onto the benchmark runner and dataset
  - `dataset.name: str` - used to source the dataset under `datasets.{name}.dataset`

The remaining named arguments will be available inside of the dataset as a private field, called `_config`.

```python
class Dataset(ABC):
    """Constructs dataset for a benchmark."""

    _config: DatasetConfig

    def __init__(self, config: DatasetConfig):
        self._config = config
    ...
```

All values under agent_config are optional by default, but depending on the benchmark and harness it may be required.

```python
class AgentConfig(BaseModel):
    """Generic agent configuration"""

    model: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    reasoning_effort: str | None = None
    extra: dict[str, Any] = {}
```

In order to call this yaml file once created, simply specify the path when running the cli

```bash

# Example run command
python runner.py --config config/ioi.yaml

```
