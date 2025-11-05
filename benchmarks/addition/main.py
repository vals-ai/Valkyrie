"""Simple addition benchmark."""


def get_dataset() -> dict[str, dict]:
    """Return 5 addition problems."""
    return {
        "task_1": {"a": 5, "b": 3, "expected": 8},
        "task_2": {"a": 12, "b": 8, "expected": 20},
        "task_3": {"a": 25, "b": 15, "expected": 40},
        "task_4": {"a": 100, "b": 50, "expected": 150},
        "task_5": {"a": 7, "b": 9, "expected": 16},
    }


def evaluate_output(output: dict[str, str], run_id: str) -> dict:
    """Evaluate agent answers against expected results."""
    dataset = get_dataset()
    correct = 0
    total = len(dataset)
    
    for task_id, task in dataset.items():
        if task_id in output:
            try:
                agent_answer = int(output[task_id])
                if agent_answer == task["expected"]:
                    correct += 1
            except (ValueError, TypeError):
                pass
    
    return {"correct": correct, "total": total, "accuracy": correct / total}
