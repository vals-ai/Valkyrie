from agentic_harness import AgentContract
from dotenv import load_dotenv

load_dotenv()

contract = AgentContract(
    name="sweagent",
    artifacts=["setup.sh"],
    install_cmd="bash setup.sh",
    run_cmd="echo hello world",
)
