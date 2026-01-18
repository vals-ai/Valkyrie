from pydantic import BaseModel


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str
    """Model key (e.g., openai/gpt-4o)"""


class BenchmarkConfig(BaseModel):
    """Configuration for running an agent on a benchmark."""

    benchmark: str
    """Name of the benchmark to run (e.g., swebench, finance)"""

    concurrency: int = 5
    """Number of tasks to run concurrently"""

    task_ids: list[str] | None = None
    """Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908)"""

    slice: str | None = None
    """Slice string to use for slicing the benchmark (e.g., 1-10)"""

    contract: str
    """Path to contract directory (e.g., contracts/claude_code)"""

    agent_config: AgentConfig | None = None
    """Agent configuration"""
