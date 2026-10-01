import json

import pytest

from tracker.conversation import ConversationConfig, OUTPUT_PATH, UserTurn, run_conversation


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
