from importlib import import_module
from inspect import isclass
from typing import TypeVar

from agentic_harness.base.agent import Agent
from agentic_harness.base.benchmark import Benchmark
from agentic_harness.base.dataset import Dataset
from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import BaseConfig

from agentic_harness.logger import get_logger

logger = get_logger(__name__)

AGENT_MODULE = "agent"
BENCHMARK_PACKAGE = "benchmarks"
BENCHMARK_MODULE = "benchmark"
DATASET_PACKAGE = "datasets"
DATASET_MODULE = "dataset"
CONTRACT_PACKAGE = "contracts"
CONTRACT_MODULE = "contract"

T = TypeVar("T")


def _load_component_instance(
    component_name: str,
    package: str,
    module_name: str,
    base_cls: type[T],
) -> type[T]:
    """Import the expected module and instantiate the single subclass it defines."""

    try:
        module = import_module(f"{package}.{component_name}.{module_name}")
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"{package.title()} {component_name} not found at path: {f'{package}.{component_name}.{module_name}'}"
        ) from exc

    matching_classes: list[type[T]] = []
    for attr in vars(module).values():
        if not isclass(attr) or attr.__module__ != module.__name__:
            continue

        if issubclass(attr, base_cls) and attr is not base_cls:
            matching_classes.append(attr)

    if not matching_classes:
        raise ValueError(
            f"{package.title()} {component_name} does not define a {base_cls.__name__}"
        )

    if len(matching_classes) > 1:
        raise ValueError(
            f"{package.title()} {component_name} defines multiple {base_cls.__name__} subclasses"
        )

    cls = matching_classes[0]
    return cls


def load_agent(agent_name: str) -> type[Agent]:
    """Instantiate the agent identified by ``agent_name``."""

    return _load_component_instance(
        agent_name,
        BENCHMARK_PACKAGE,
        AGENT_MODULE,
        Agent,
    )


def load_benchmark(benchmark_name: str) -> type[Benchmark]:
    """Instantiate the benchmark identified by ``benchmark_name``."""

    return _load_component_instance(
        benchmark_name,
        BENCHMARK_PACKAGE,
        BENCHMARK_MODULE,
        Benchmark,
    )


def load_dataset(dataset_name: str) -> type[Dataset]:
    """Instantiate the dataset identified by ``dataset_name``."""

    return _load_component_instance(
        dataset_name,
        DATASET_PACKAGE,
        DATASET_MODULE,
        Dataset,
    )


def load_contract(contract_name: str) -> type[AgentContract]:
    """Instantiate the contract identified by ``contract_name``."""
    return _load_component_instance(
        contract_name,
        CONTRACT_PACKAGE,
        CONTRACT_MODULE,
        AgentContract,
    )


def create_benchmark(config: BaseConfig) -> Benchmark:
    """Loads required components and constructs a benchmark object"""

    logger.info(f"Creating benchmark with config: `{str(config)}`")

    dataset_name = config.dataset.get("name", None)
    if dataset_name is None:
        raise ValueError("`dataset.name` is required")

    # Parse the dataset, agent, and contract
    Dataset = load_dataset(dataset_name)
    Agent = load_agent(config.benchmark)
    BenchmarkClass = load_benchmark(config.benchmark)
    Contract = load_contract(config.agent)

    # Instantiate the benchmark
    dataset = Dataset(config.dataset)
    agent = Agent(Contract(config.agent_config))

    logger.info("Loaded components...")

    return BenchmarkClass(dataset=dataset, agent=agent)
