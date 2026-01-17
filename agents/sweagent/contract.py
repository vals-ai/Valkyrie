from agentic_harness import AgentContract
from dotenv import dotenv_values

_run_cmd = """
sweagent run \
    --env.deployment.type=local \
    --env.repo.type=preexisting \
    --env.repo.repo_name=/testbed \
    --agent.model.provider=vals \
    --agent.model.name="grok/grok-4-fast-reasoning" \
    --problem_statement.text={{problem_statement}} \
    --config=/bundle/sweagent/submodules/sweagent/config/default.yaml
"""

_env = {k: v for k, v in dotenv_values().items() if v is not None}

contract = AgentContract(
    name="sweagent",
    artifacts=["setup.sh", "submodules/sweagent"],
    install_cmd="bash setup.sh",
    run_cmd=_run_cmd,
    env=_env,
)
