# Benchmark Contract

External benchmark repositories must implement a specific contract to work with the agentic-harness.

## Required Structure

```
your_benchmark/
├── main.py          # Main benchmark implementation
├── setup.sh         # Optional: VM setup script
└── data/            # Optional: Benchmark data files
```

## Required Methods in main.py

### 1. `get_dataset()` -> dict[str, dict]

Returns the benchmark dataset mapping task IDs to task data.

```python
def get_dataset() -> dict[str, dict]:
    """Get the benchmark dataset.
    
    Returns:
        Dictionary mapping task IDs to task data dictionaries
    """
    return {
        "task_1": {"input": "...", "expected": "..."},
        "task_2": {"input": "...", "expected": "..."},
    }
```

### 2. `evaluate_output(agent_output: dict[str, Any], run_id: str)` -> dict[str, Any]

Evaluates agent solutions against the benchmark.

```python
def evaluate_output(agent_output: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Evaluate agent solutions.
    
    Args:
        agent_output: Dictionary mapping task IDs to agent outputs
        run_id: Unique identifier for this run
        
    Returns:
        Evaluation results dictionary
    """
    results = {}
    for task_id, output in agent_output.items():
        # Evaluate this task
        results[task_id] = {"score": 1.0, "passed": True}
    return results
```

## Optional Attributes

### `requires_sandbox: bool`

Set to `True` if the benchmark requires VM or Docker execution.

```python
requires_sandbox = False  # Run locally
requires_sandbox = True   # Requires VM execution
```

### `setup_script: str`

Path to VM setup script (relative to benchmark directory).

```python
setup_script = "setup.sh"
```

## Data Flow

1. **Load benchmark**: `get_benchmark("your_benchmark")` loads `main.py`
2. **Get dataset**: Call `benchmark.module.get_dataset()` to get tasks
3. **Run agent**: Pass dataset to agent via `agent.run(dataset, **agent_args)`
4. **Evaluate**: Call `benchmark.module.evaluate_output(agent_output, run_id)`
5. **Process results**: Extract metrics and format outputs

## Example Implementation

```python
# benchmarks/my_benchmark/main.py

def get_dataset() -> dict[str, dict]:
    """Load dataset from data files or define inline."""
    return {
        "math_1": {
            "problem": "What is 2+2?",
            "context": "elementary_math"
        },
        "math_2": {
            "problem": "What is 5*3?",
            "context": "elementary_math"
        }
    }

def evaluate_output(agent_output: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Evaluate agent responses."""
    results = {}
    expected = {
        "math_1": "4",
        "math_2": "15"
    }
    
    for task_id, output in agent_output.items():
        is_correct = output.strip() == expected.get(task_id, "").strip()
        results[task_id] = {
            "correct": is_correct,
            "expected": expected.get(task_id),
            "got": output
        }
    
    return results

# Optional: Sandbox requirements
requires_sandbox = False
setup_script = "setup.sh"
```

