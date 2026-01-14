import os
from agentic_harness.base.contract import AgentContract
from dotenv import load_dotenv

load_dotenv()

contract = AgentContract(
    name="claude_code",
    artifacts=["submodules/claude_code", "setup.sh"],
    install_cmd="bash setup.sh",
    run_cmd="claude_code -p {{problem_statement}}",
    env={"ANTHROPIC_API_KEY": os.environ["ANTHROPIC_API_KEY"]},
)
