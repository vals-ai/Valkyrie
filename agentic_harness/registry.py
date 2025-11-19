from importlib import import_module
from pathlib import Path
from typing import Callable, Type, TypeVar

from agentic_harness.base.agent import Agent
from agentic_harness.base.benchmark import Benchmark

AGENTS_DIR = Path(__file__).parent.parent / "agents"
DEFAULT_BENCHMARKS_DIR = Path(__file__).parent.parent / "benchmarks"

AgentT = TypeVar("AgentT", bound=Agent)
BenchmarkT = TypeVar("BenchmarkT", bound=Benchmark)

_agent_registry: dict[str, type[Agent]] = {}
_benchmark_registry: dict[str, type[Benchmark]] = {}

_agents_imported = False
_benchmarks_imported = False


def _import_modules(base_dir: Path, package: str, module_name: str) -> None:
    """Import every package submodule named ``module_name`` under ``base_dir``."""

    if not base_dir.exists():
        return

    for path in base_dir.iterdir():
        if not path.is_dir():
            continue

        module_path = path / f"{module_name}.py"
        if not module_path.is_file():
            continue

        import_module(f"{package}.{path.name}.{module_name}")


def _ensure_agents_imported() -> None:
    """Lazily import all agent modules once per process."""

    global _agents_imported
    if _agents_imported:
        return

    _import_modules(AGENTS_DIR, "agents", "agent")
    _agents_imported = True


def _ensure_benchmarks_imported() -> None:
    """Lazily import all benchmark modules once per process."""

    global _benchmarks_imported
    if _benchmarks_imported:
        return

    _import_modules(DEFAULT_BENCHMARKS_DIR, "benchmarks", "benchmark")
    _benchmarks_imported = True


def register_agent(name: str) -> Callable[[Type[AgentT]], Type[AgentT]]:
    """Decorator that registers an Agent subclass under ``name``."""

    def decorator(cls: Type[AgentT]) -> Type[AgentT]:
        _agent_registry[name] = cls
        return cls

    return decorator

def register_benchmark(name: str) -> Callable[[Type[BenchmarkT]], Type[BenchmarkT]]:
    """Decorator that registers a Benchmark subclass under ``name``."""

    def decorator(cls: Type[BenchmarkT]) -> Type[BenchmarkT]:
        _benchmark_registry[name] = cls
        return cls

    return decorator


def load_agent(agent_name: str) -> Agent:
    """Instantiate the registered agent identified by ``agent_name``."""

    _ensure_agents_imported()
    try:
        agent_cls = _agent_registry[agent_name]
    except KeyError:
        raise ValueError(f"Agent {agent_name} not found")

    return agent_cls()


def load_benchmark(benchmark_name: str) -> Benchmark:
    """Instantiate the registered benchmark identified by ``benchmark_name``."""

    _ensure_benchmarks_imported()
    try:
        benchmark_cls = _benchmark_registry[benchmark_name]
    except KeyError:
        raise ValueError(f"Benchmark {benchmark_name} not found")

    return benchmark_cls()
