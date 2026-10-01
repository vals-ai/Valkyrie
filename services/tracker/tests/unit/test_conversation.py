import json

import pytest

from tracker.conversation import ConversationConfig, OUTPUT_PATH, UserTurn, run_conversation


@pytest.mark.asyncio
async def test_run_opt_in_and_headers_are_explicit():
    from tracker.database.models import AgentContractRequest, Benchmark, AWSBenchmarkArguments
    from tracker.types import StartBenchmarkRequest, RunExecutionRequest
    from tracker.utils.resources import create_benchmark_service_client

    contract = AgentContractRequest(name="capable", conversation=ConversationConfig())
    ordinary = StartBenchmarkRequest(
        contract=contract, benchmark_name="valsmith", custom_benchmark_service="https://example.com"
    )
    assert ordinary.multi_turn is False
    opted = ordinary.model_copy(update={"multi_turn": True})
    for request in [ordinary, opted, RunExecutionRequest(**opted.model_dump())]:
        async with request.benchmark_service as client:
            assert (client._headers.get("x-valkyrie-conversation") == "valkyrie.conversation.v1") is request.multi_turn
    args = AWSBenchmarkArguments(
        contract=contract, concurrency=1, multi_turn=True, sandbox_provider_secret_name="provider"
    )
    benchmark = Benchmark(
        name="valsmith", arguments=args, aws_managed=True, custom_benchmark_service="https://example.com"
    )
    assert benchmark.managed_start_benchmark_request().multi_turn is True
    async with benchmark.benchmark_service() as client:
        assert client._headers["x-valkyrie-conversation"] == "valkyrie.conversation.v1"
    assert AWSBenchmarkArguments(contract=contract, concurrency=1).multi_turn is False
    with pytest.raises(ValueError, match="run option"):
        create_benchmark_service_client("https://example.com", {"X-Valkyrie-Conversation": "valkyrie.conversation.v1"})


@pytest.mark.asyncio
async def test_opt_in_requires_capability_from_resolved_bundle():
    import io
    import zipfile
    from unittest.mock import AsyncMock

    from fastapi import HTTPException
    from main import _resolve_contract_from_s3
    from tracker.database.models import AgentContractRequest
    from tracker.types import StartBenchmarkRequest

    # A caller claiming capability cannot give it to an ordinary published agent.
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "ordinary/contract.yaml", "name: ordinary\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n"
        )
    store = AsyncMock()
    store.get_bytes.return_value = archive.getvalue()
    request = StartBenchmarkRequest(
        contract=AgentContractRequest(name="ordinary", conversation=ConversationConfig()),
        benchmark_name="valsmith",
        multi_turn=True,
    )
    with pytest.raises(HTTPException) as error:
        await _resolve_contract_from_s3(request, store)
    assert error.value.status_code == 400
    ordinary = await _resolve_contract_from_s3(request.model_copy(update={"multi_turn": False}), store)
    assert ordinary.conversation is None


@pytest.mark.asyncio
async def test_simulator_stop_finishes_conversation_not_correctness():
    sandbox, store = Sandbox(), Store()
    agent_calls = []

    async def next_user(payload):
        return UserTurn(
            protocol="valkyrie.conversation.v1",
            turn=payload["turn"],
            action="message" if payload["turn"] == 0 else "stop",
            message="fix it" if payload["turn"] == 0 else "",
            policy_version="p",
        )

    async def agent(timeout):
        agent_calls.append(1)
        sandbox.files[OUTPUT_PATH] = b'{"turn":0,"message":"done"}'
        return None, 1

    result = await run_conversation(
        sandbox=sandbox,
        config=ConversationConfig(),
        problem_path="/problem",
        context={},
        store=store,
        artifact_prefix="c",
        next_user=next_user,
        agent_turn=agent,
        execution_is_current=lambda: True,
    )
    assert result == (None, 1)
    assert len(agent_calls) == 1
    summary = json.loads(store.files["c/summary.json"])
    assert summary["status"] == "stopped"
    assert "score" not in summary


class Store:
    def __init__(self):
        self.files = {}

    async def exists(self, key):
        return key in self.files

    async def put_bytes(self, key, content):
        self.files[key] = content


class Sandbox:
    def __init__(self):
        self.files = {}

    async def upload_file(self, path, content):
        self.files[path] = content

    async def download_file(self, path):
        return self.files[path]


@pytest.mark.asyncio
async def test_persistent_turns_and_no_replay():
    sandbox, store = Sandbox(), Store()
    calls = []

    async def next_user(payload):
        calls.append(payload)
        return UserTurn(
            protocol="valkyrie.conversation.v1",
            turn=payload["turn"],
            action="message",
            message=f"request {payload['turn']}",
            policy_version="frozen",
        )

    async def agent(timeout):
        turn = len(calls) - 1
        if turn:
            assert sandbox.files["code.py"] == b"edited"
        sandbox.files["code.py"] = b"edited"
        sandbox.files[OUTPUT_PATH] = json.dumps({"turn": turn, "message": "What else?"}).encode()
        return None, 2.0

    kwargs = dict(
        sandbox=sandbox,
        config=ConversationConfig(max_turns=2),
        problem_path="/problem",
        context={"run_id": "r"},
        store=store,
        artifact_prefix="task/conversation",
        next_user=next_user,
        agent_turn=agent,
        execution_is_current=lambda: True,
    )
    reason, duration = await run_conversation(**kwargs)
    assert reason is None and duration == 4
    assert calls[1]["history"][1]["content"] == "What else?"
    assert json.loads(store.files["task/conversation/summary.json"])["status"] == "turn_limit"
    with pytest.raises(RuntimeError, match="claimed"):
        await run_conversation(**kwargs)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_stale_reply_is_unscored():
    sandbox, store = Sandbox(), Store()

    async def next_user(payload):
        return UserTurn(
            protocol="valkyrie.conversation.v1", turn=0, action="message", message="hello", policy_version="p"
        )

    async def agent(timeout):
        sandbox.files[OUTPUT_PATH] = b'{"turn":4,"message":"stale"}'
        return None, 1

    with pytest.raises(ValueError, match="Stale"):
        await run_conversation(
            sandbox=sandbox,
            config=ConversationConfig(),
            problem_path="/problem",
            context={},
            store=store,
            artifact_prefix="c",
            next_user=next_user,
            agent_turn=agent,
            execution_is_current=lambda: True,
        )
    assert json.loads(store.files["c/error.json"])["status"] == "unscored"
