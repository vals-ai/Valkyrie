from importlib import import_module
from inspect import isclass
from typing import TypeVar

from src.base.benchmark import Benchmark
from src.base.dataset import Dataset
from src.base.contract import AgentContract
from src.base.types import BaseConfig

from src.base_agent import BaseAgent
from src.logger import get_logger
from evaluators.platform_evaluate import PlatformEvaluator

logger = get_logger(__name__)

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
    base_cls: type[T],
    module_name: str | None = None,
) -> type[T]:
    """Import the expected module and instantiate the single subclass it defines."""

    module_path = f"{package}.{component_name}"
    if module_name is not None:
        module_path += f".{module_name}"

    module = import_module(module_path)

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


def create_benchmark(config: BaseConfig) -> Benchmark:
    """Loads required components and constructs a benchmark object"""

    logger.info(f"Creating benchmark with config: `{str(config)}`")

    dataset_name = config.dataset.get("name", None)
    if dataset_name is None:
        raise ValueError("`dataset.name` is required")

    # Parse the dataset, agent, and contract
    Dataset = load_dataset(dataset_name)
    BenchmarkClass = load_benchmark(config.benchmark)
    Contract = load_contract(config.agent)

    # NOTE: Hardcode the evaluator for now - introduce additional options as we need them
    evaluator = PlatformEvaluator()

    # Instantiate the benchmark
    dataset = Dataset(config.dataset)
    agent = BaseAgent(Contract(config.agent_config), evaluator)

    logger.info("Loaded components...")

    return BenchmarkClass(dataset=dataset, agent=agent)
