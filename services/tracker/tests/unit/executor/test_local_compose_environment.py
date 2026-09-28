"""Local Compose must supply the runner's required environment."""

import ast
from pathlib import Path

import yaml


TRACKER_ROOT = Path(__file__).resolve().parents[3]
EXECUTOR_SOURCE = TRACKER_ROOT / "src" / "tracker" / "executor"


def _environment_keys(source: Path) -> set[str]:
    tree = ast.parse(source.read_text())
    return {
        node.slice.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "os"
        and node.value.attr == "environ"
    }


def test_compose_tracker_supplies_local_runner_environment() -> None:
    runner_keys = _environment_keys(EXECUTOR_SOURCE / "runner.py")
    launcher_keys = _environment_keys(EXECUTOR_SOURCE / "launcher.py")
    # ECS-only override settings are not needed on the local launch path.
    required = runner_keys | {key for key in launcher_keys if not key.startswith("EXECUTOR_RUNNER_")}
    # The local payload key is read with .get(), but is required for the local encryption mode.
    required.add("EXECUTOR_PAYLOAD_LOCAL_KEY")

    compose = yaml.safe_load((TRACKER_ROOT / "docker-compose.yml").read_text())
    environment = compose["services"]["tracker"]["environment"]
    defined = {item.split("=", 1)[0] for item in environment}
    assert required <= defined, f"Compose tracker is missing runner settings: {sorted(required - defined)}"
