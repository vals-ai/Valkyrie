"""Opt-in conversation protocol; ordinary single-turn runs are unchanged.

The benchmark companion owns private task context. Only visible messages cross
into the agent sandbox. A started run is deliberately not crash-resumable: its
durable receipt makes an uncertain continuation fail closed, rather than replay.
"""

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

INPUT_PATH = "/workspace/conversation-input.json"
OUTPUT_PATH = "/workspace/conversation-output.json"
MAX_MESSAGE_BYTES = 100_000


class ConversationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol: Literal["valkyrie.conversation.v1"] = "valkyrie.conversation.v1"
    max_turns: int = Field(default=3, ge=1, le=20)
    timeout_seconds: int = Field(default=1800, ge=1, le=14400)


class UserTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol: Literal["valkyrie.conversation.v1"]
    turn: int = Field(ge=0)
    action: Literal["message", "stop"]
    message: str = Field(max_length=MAX_MESSAGE_BYTES)
    policy_version: str


class AssistantTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    turn: int = Field(ge=0)
    message: str = Field(min_length=1, max_length=MAX_MESSAGE_BYTES)


async def companion_turn(client: Any, payload: dict[str, Any]) -> UserTurn:
    """Narrow CBS extension shim, using the already authenticated/pinned client.

    No retry decorator and no redirects (including credential forwarding). CBS
    does not yet expose extension methods; isolate its private transport here.
    """
    response = await client._http_client.post(
        f"{client._url}/conversation/turn",
        json=payload,
        follow_redirects=False,
        timeout=1800,
    )
    response.raise_for_status()
    if len(response.content) > MAX_MESSAGE_BYTES * 2:
        raise ValueError("Conversation companion response exceeded its byte limit")
    return UserTurn.model_validate_json(response.content)


async def run_conversation(
    *,
    sandbox: Any,
    config: ConversationConfig,
    problem_path: str,
    context: dict[str, Any],
    store: Any,
    artifact_prefix: str,
    next_user: Callable[[dict[str, Any]], Awaitable[UserTurn]],
    agent_turn: Callable[[float], Awaitable[tuple[Any, float]]],
    execution_is_current: Callable[[], bool],
) -> tuple[Any, float]:
    started = time.monotonic()
    history: list[dict[str, str]] = []
    prefix = artifact_prefix.rstrip("/")
    claim = f"{prefix}/started.json"
    # Executor dispatch authority supplies single ownership. This receipt also
    # prevents the sandbox-recovery path from silently replaying an old session.
    if await store.exists(claim):
        raise RuntimeError("Conversation already claimed; explicit recovery required")
    if not execution_is_current():
        raise RuntimeError("Conversation execution authority lost")
    await store.put_bytes(claim, json.dumps(context).encode())
    agent_seconds = 0.0
    policy_version: str | None = None

    async def save(name: str, value: Any) -> None:
        if not execution_is_current():
            raise RuntimeError("Conversation execution authority lost")
        await store.put_bytes(f"{prefix}/{name}.json", json.dumps(value).encode())

    try:
        async with asyncio.timeout(config.timeout_seconds):
            for turn in range(config.max_turns):
                request = {**context, "protocol": config.protocol, "turn": turn, "history": history}
                await save(f"turn-{turn:03d}-request", request)
                user = await next_user(request)
                if user.turn != turn or (turn == 0 and user.action != "message"):
                    raise ValueError("Invalid opening or out-of-order simulator turn")
                if policy_version is not None and policy_version != user.policy_version:
                    raise ValueError("Simulator policy changed during the run")
                policy_version = user.policy_version
                await save(f"turn-{turn:03d}-user", user.model_dump())
                if user.action == "stop":
                    await save("summary", {"status": "stopped", "turns": turn, "policy_version": policy_version})
                    return None, agent_seconds
                if not user.message.strip():
                    raise ValueError("Simulator returned an empty user message")
                history.append({"role": "user", "content": user.message})
                if not execution_is_current():
                    raise RuntimeError("Conversation execution authority lost")
                # No task spec, verifier output, or simulator reasoning enters these files.
                await sandbox.upload_file(problem_path, user.message.encode())
                await sandbox.upload_file(INPUT_PATH, json.dumps({"turn": turn, "messages": history}).encode())
                await sandbox.upload_file(OUTPUT_PATH, b"{}")
                await save(f"turn-{turn:03d}-agent-started", {"turn": turn})
                remaining = config.timeout_seconds - (time.monotonic() - started)
                reason, duration = await agent_turn(max(0.1, remaining))
                agent_seconds += duration
                if reason is not None:
                    await save("summary", {"status": "agent_terminated", "turns": turn + 1, "reason": str(reason)})
                    return reason, agent_seconds
                raw = await sandbox.download_file(OUTPUT_PATH)
                if len(raw) > MAX_MESSAGE_BYTES:
                    raise ValueError("Assistant turn exceeded its byte limit")
                assistant = AssistantTurn.model_validate_json(raw)
                if assistant.turn != turn:
                    raise ValueError("Stale assistant response")
                history.append({"role": "assistant", "content": assistant.message})
                await save(f"turn-{turn:03d}-assistant", assistant.model_dump())
            await save("summary", {"status": "turn_limit", "turns": config.max_turns, "policy_version": policy_version})
            return None, agent_seconds
    except BaseException as error:
        # Failure is infrastructure/unscored, not a solver correctness result.
        if execution_is_current():
            await save("error", {"type": type(error).__name__, "status": "unscored", "turns": len(history) // 2})
        raise
