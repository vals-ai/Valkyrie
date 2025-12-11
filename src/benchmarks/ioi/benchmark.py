from src.base import Dataset
from src.base_agent import BaseAgent
from src.benchmarks.fab.benchmark import FinanceAgentBenchmark
from src.utils import setup_environment

setup_environment()


class IOIBenchmark(FinanceAgentBenchmark):
    """
    IOI benchmark class

    NOTE: Uses the same benchmark class as what is inside of benchmarks.fab.benchmark.FinanceAgentBenchmark
    since we are using the same harness for both benchmarks with different tools and metadata collection.

    This is _not_ the usual case which is why it may seem weird
    """

    def __init__(self, dataset: Dataset, agent: BaseAgent):
        super().__init__(dataset, agent)
