from importlib import import_module
from inspect import isclass
from typing import Any, TypeVar

from src.base.contract import AgentContract
from src.base.dataset import Dataset
from src.base.environment import Environment
from src.base.types import BaseConfig
from src.base_agent import BaseAgent
from src.benchmarks.base_benchmark import Benchmark
from src.evaluators import BlankEvaluator
from src.logger import get_logger

logger = get_logger(__name__)

BENCHMARK_PACKAGE = "src.benchmarks"
BENCHMARK_MODULE = "benchmark"
DATASET_PACKAGE = "src.datasets"
DATASET_MODULE = "dataset"
CONTRACT_PACKAGE = "contracts"
CONTRACT_MODULE = "contract"
ENVIRONMENT_PACKAGE = "src.environments"

T = TypeVar("T")


def _load_component_instance(
    component_name: str,
    package: str,
    base_cls: type[T],
    module_name: str | None = None,
) -> type[T]:
    """Import the expected module and instantiate the single subclass it defines."""

    module_path = "src.benchmarks.base_benchmark" if component_name == "base" else f"{package}.{component_name}"

    if module_name and component_name != "base":
        module_path += f".{module_name}"

    module = import_module(module_path)

    matching_classes: list[type[T]] = []
    for attr in vars(module).values():
        if not isclass(attr) or attr.__module__ != module.__name__:
            continue

        if issubclass(attr, base_cls):
            matching_classes.append(attr)

    if base_cls in matching_classes and len(matching_classes) > 1:
        matching_classes = [cls for cls in matching_classes if cls is not base_cls]

    if not matching_classes:
        raise ValueError(f"{package.title()} {component_name} does not define a {base_cls.__name__}")

    if len(matching_classes) > 1:
        raise ValueError(f"{package.title()} {component_name} defines multiple {base_cls.__name__} subclasses")

    cls = matching_classes[0]
    return cls


def load_benchmark(benchmark_name: str) -> type[Benchmark]:
    """Instantiate the benchmark identified by ``benchmark_name``."""

    return _load_component_instance(
        benchmark_name,
        BENCHMARK_PACKAGE,
        Benchmark,
        BENCHMARK_MODULE,
    )


def load_dataset(dataset_name: str) -> type[Dataset]:
    """Instantiate the dataset identified by ``dataset_name``."""

    return _load_component_instance(
        dataset_name,
        DATASET_PACKAGE,
        Dataset,
        DATASET_MODULE,
    )


def load_contract(contract_name: str) -> type[AgentContract]:
    """Instantiate the contract identified by ``contract_name``."""
    return _load_component_instance(
        contract_name,
        CONTRACT_PACKAGE,
        AgentContract,
    )


def load_environment(environment_name: str) -> type[Environment]:
    """Instantiate the environment identified by ``environment_name``."""
    return _load_component_instance(
        environment_name,
        ENVIRONMENT_PACKAGE,
        Environment,
    )


def parse_environment(environment_config: dict[str, Any]) -> Environment | None:
    """Fetches the environment config if it exists"""
    environment = environment_config.get("name", None)
    if environment is None:
        return None

    Environment = load_environment(environment)

    return Environment(config=environment_config)


def create_agent(config: BaseConfig) -> BaseAgent:
    """Loads required components and constructs an agent object"""
    Contract = load_contract(config.agent.name)
    environment = parse_environment(config.environment)

    # NOTE: Hardcode the evaluator for now - introduce additional options as we need them
    evaluator = BlankEvaluator()

    return BaseAgent(Contract(config.agent), evaluator, environment)


def create_benchmark(config: BaseConfig) -> Benchmark:
    """Loads required components and constructs a benchmark object"""

    logger.info(f"Creating benchmark with config: `{str(config)}`")

    dataset_name = config.dataset.get("name", None)
    if dataset_name is None:
        raise ValueError("`dataset.name` is required")

    # Parse the dataset, agent, and contract
    Dataset = load_dataset(dataset_name)
    BenchmarkClass = load_benchmark(config.benchmark)

    # Instantiate the benchmark
    dataset = Dataset(config.dataset)
    agent = create_agent(config)

    logger.info("Loaded components...")

    return BenchmarkClass(dataset=dataset, agent=agent, environment=agent.environment)
