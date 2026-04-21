import asyncio
from typing import Any
from unittest.mock import Mock

import pytest

from tracker import sandbox as sandbox_module
from tracker.sandbox import _create_pty_session


class TestPtyHandshakeSemaphore:
    async def test_semaphore_caps_concurrent_handshakes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Concurrent _create_pty_session calls never exceed the cap on in-flight
        sandbox.process.create_pty_session calls.

        Regression guard: if someone removes the `async with _pty_handshake_slot(...)`
        wrapper around the handshake call, concurrency becomes unbounded.
        """
        cap = 5
        total = 25
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(cap))

        concurrent = 0
        max_concurrent = 0

        async def fake_create_pty_session(*_args: Any, **_kwargs: Any) -> Mock:
            nonlocal concurrent, max_concurrent

            concurrent += 1
            max_concurrent = max(max_concurrent, concurrent)

            # Hold long enough for other tasks to pile up behind the gate
            await asyncio.sleep(0.05)

            concurrent -= 1
            return Mock()

        mock_sandbox = Mock()
        mock_sandbox.process.create_pty_session = fake_create_pty_session

        results = await asyncio.gather(
            *[_create_pty_session(mock_sandbox, f"session-{i}", lambda _data: None) for i in range(total)]
        )

        assert len(results) == total
        assert max_concurrent <= cap
        # Sanity: without the cap we'd see total concurrency; confirm contention actually happened
        assert max_concurrent > 1

    async def test_semaphore_released_after_handshake_returns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        The semaphore slot is released as soon as the handshake call returns, before the
        caller does anything else with the handle (handle.wait, send_input, etc.).

        Regression guard: if someone widens the `async with _pty_handshake_slot(...)` scope
        — e.g. wraps code after create_pty_session returns, or wraps the whole caller flow
        around handle.wait() — a second concurrent handshake would block until the first
        task finishes its session lifetime. With cap=1 and a simulated long-running
        post-handshake step, that regression turns this test into a timeout.
        """
        cap = 1
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(cap))

        async def fake_create_pty_session(*_args: Any, **_kwargs: Any) -> Mock:
            return Mock()

        mock_sandbox = Mock()
        mock_sandbox.process.create_pty_session = fake_create_pty_session

        events: list[str] = []

        async def task_holding_handle() -> None:
            await _create_pty_session(mock_sandbox, "s1", lambda _data: None)
            events.append("a:handshake_done")

            # Simulate a long-running handle.wait() AFTER the handshake.
            # The slot must already be released, otherwise the other task can't acquire.
            await asyncio.sleep(0.2)
            events.append("a:post_handshake_done")

        async def task_needing_slot() -> None:
            # Tiny delay so task A acquires the slot first
            await asyncio.sleep(0.01)
            await _create_pty_session(mock_sandbox, "s2", lambda _data: None)
            events.append("b:handshake_done")

        await asyncio.wait_for(asyncio.gather(task_holding_handle(), task_needing_slot()), timeout=1.0)

        # Task B's handshake must complete BEFORE task A's post-handshake work finishes.
        # That ordering is only reachable if the slot was released at handshake exit.
        assert events.index("b:handshake_done") < events.index("a:post_handshake_done")
