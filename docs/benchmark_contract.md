# Agent & Benchmark Contract

## Agent Contract

Implement a function in `agents/your_agent/main.py` (default: `run`):

```python
def run(input: dict[str, dict], **kwargs) -> dict[str, str]:
    """Process tasks and return outputs.
    
    Args:
        input: Dictionary mapping task IDs to task data
        **kwargs: Additional arguments
        
    Returns:
        Dictionary mapping task IDs to agent outputs
    """
    # Your agent logic here
    return {"task_1": "output", "task_2": "output"}
```

**Usage:** `--agent-function main.run` specifies which function to call from the agent's main.py.

## Benchmark Contract

Implement `get_dataset()` in `benchmarks/your_benchmark/main.py`:

```python
def get_dataset() -> dict[str, dict]:
    """Return benchmark tasks."""
    return {"task_1": {"input": "..."}, "task_2": {"input": "..."}}

# Optional attributes
requires_sandbox = False  # VM execution required?
setup_script = "setup.sh"  # VM setup script path
```

## Flow

1. **Load**: Benchmark module loads `main.py`
2. **Get dataset**: `benchmark.get_dataset()` → `{task_id: task_data}`
3. **Run agent**: `agent.run(dataset, **agent_args)` → `{task_id: output}`
4. **Evaluate** (optional): Custom evaluation can be done externally or via `benchmark.evaluate_output()`
