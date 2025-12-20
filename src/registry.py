import ast
import inspect
from importlib import import_module
from inspect import isclass
from typing import TypeVar

from src.base.contract import AgentContract
from src.base.dataset import Dataset
from src.base.environment import Environment
from src.base.types import AgentConfig, BaseConfig, EnvironmentConfig
from src.base_agent import AgentRunner
from src.benchmarks.base_benchmark import BenchmarkRunner
from src.evaluators import BlankEvaluator
from src.logger import get_logger

logger = get_logger(__name__)

BENCHMARK_PACKAGE = "src.benchmarks"
BENCHMARK_MODULE = "benchmark"
DATASET_PACKAGE = "src.datasets"
DATASET_MODULE = "dataset"
CONTRACT_PACKAGE = "contracts"
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


def load_benchmark(benchmark_name: str) -> type[BenchmarkRunner]:
    """Instantiate the benchmark identified by ``benchmark_name``."""

    return _load_component_instance(
        benchmark_name,
        BENCHMARK_PACKAGE,
        BenchmarkRunner,
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


def parse_environment(
    environment_config: EnvironmentConfig, contract_name: str, submodule_name: str
) -> Environment | None:
    """Fetches the environment config if it exists"""
    environment = environment_config.name
    if not environment:
        raise ValueError("`environment.name` is required")

    Environment = load_environment(environment)

    return Environment(config=environment_config, submodule_name=submodule_name, contract_name=contract_name)


def find_submodule_from_contract(contract_name: str) -> str:
    """
    Returns the set of submodule names imported from `submodules.*`

    TODO: Just move the contract to the submodule, and then we can just use the submodule name
    """
    contract = import_module(f"{CONTRACT_PACKAGE}.{contract_name}")

    source = inspect.getsource(contract)
    tree = ast.parse(source)

    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("submodules."):
                parts = node.module.split(".")
                if len(parts) >= 2:
                    found.add(parts[1])

    if not found:
        raise ValueError(f"Contract {contract_name} does not use any submodules")

    if len(found) > 1:
        raise ValueError(
            f"Contract {contract_name} uses multiple submodules: {found}. Please consolidate into a single submodule."
        )

    return list(found)[0]


def create_agent(agent_config: AgentConfig, environment_config: EnvironmentConfig | None) -> AgentRunner:
    """Loads required components and constructs an agent object"""
    Contract = load_contract(agent_config.name)
    submodule_name = find_submodule_from_contract(agent_config.name)

    environment = None
    if environment_config:
        environment = parse_environment(environment_config, agent_config.name, submodule_name)

    # NOTE: Hardcode the evaluator for now - introduce additional options as we need them
    evaluator = BlankEvaluator()

    return AgentRunner(Contract(agent_config), evaluator, environment)


def create_benchmark(config: BaseConfig) -> BenchmarkRunner:
    """Loads required components and constructs a benchmark object"""

    logger.info(f"Creating benchmark with config: `{str(config)}`")

    dataset_name = config.dataset.name
    if not dataset_name:
        raise ValueError("`dataset.name` is required")

    # Parse the dataset, agent, and contract
    Dataset = load_dataset(dataset_name)
    BenchmarkClass = load_benchmark(config.benchmark)

    # Instantiate the benchmark
    dataset = Dataset(config.dataset)
    agent = create_agent(config.agent, config.environment)

    logger.info("Loaded components...")

    return BenchmarkClass(dataset=dataset, agent=agent, environment=agent.environment)
