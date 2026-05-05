import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from daytona import DaytonaNotFoundError, ExecuteResponse
from daytona.common.errors import DaytonaError

from tracker import sandbox as sandbox_module
from tracker.database.models import AgentContractRequest
from tracker.exceptions import SSLConnectionError, SandboxError, SandboxGoneError, SandboxSetupError
from tracker.sandbox import (
    _check_sandbox_health,
    _create_pty_session,
    _is_transient_daytona_connection_error,
    upload_agent_artifacts,
)
from tracker.types import AWSCredentials


class TestCheckSandboxHealthRetry:
    @pytest.fixture(autouse=True)
    def _fast_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sandbox_module, "_HEALTHCHECK_RETRY_WAIT_SECONDS", 0.0)
        from tenacity import (
            before_sleep_log,
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_fixed,
        )

        async def _refresh(sandbox: Any) -> None:
            await sandbox.refresh_data()

        monkeypatch.setattr(
            sandbox_module,
            "_refresh_sandbox_data_with_retry",
            retry(
                retry=retry_if_exception(sandbox_module._is_transient_daytona_connection_error),
                stop=stop_after_attempt(sandbox_module._HEALTHCHECK_RETRY_ATTEMPTS),
                wait=wait_fixed(0.0),
                before_sleep=before_sleep_log(sandbox_module.logger, 30),
                reraise=True,
            )(_refresh),
        )

    async def test_predicate_matches_wrapped_daytona_broken_pipe(self) -> None:
        for msg in (
            "Failed to refresh sandbox data: [Errno 32] Broken pipe",
            "Failed to get sandbox: Server disconnected",
            "Connection reset by peer",
        ):
            assert _is_transient_daytona_connection_error(DaytonaError(msg)), f"Expected match for: {msg}"

    async def test_predicate_matches_direct_aiohttp_errors(self) -> None:
        assert _is_transient_daytona_connection_error(aiohttp.ServerDisconnectedError())
        assert _is_transient_daytona_connection_error(aiohttp.ClientOSError(32, "Broken pipe"))
        assert _is_transient_daytona_connection_error(asyncio.TimeoutError())

    async def test_predicate_rejects_daytona_not_found(self) -> None:
        assert not _is_transient_daytona_connection_error(DaytonaNotFoundError("Sandbox X not found"))

    async def test_predicate_rejects_unrelated_errors(self) -> None:
        assert not _is_transient_daytona_connection_error(DaytonaError("Invalid auth token"))
        assert not _is_transient_daytona_connection_error(ValueError("totally unrelated"))

    async def test_retries_wrapped_daytona_broken_pipe_then_succeeds(self) -> None:
        sandbox = AsyncMock()
        sandbox.name = "mock-sandbox-1"
        sandbox.state = "started"
        attempt_count = 0

        async def _flaky_refresh() -> None:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise DaytonaError("Failed to refresh sandbox data: [Errno 32] Broken pipe")

        sandbox.refresh_data = _flaky_refresh

        await _check_sandbox_health(sandbox)
        assert attempt_count == 3

    async def test_retries_direct_aiohttp_server_disconnected_then_succeeds(self) -> None:
        sandbox = AsyncMock()
        sandbox.name = "mock-sandbox-1"
        sandbox.state = "started"
        attempt_count = 0

        async def _flaky_refresh() -> None:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise aiohttp.ServerDisconnectedError()

        sandbox.refresh_data = _flaky_refresh

        await _check_sandbox_health(sandbox)
        assert attempt_count == 2

    async def test_persistent_transient_exhausts_and_raises_sandbox_error(self) -> None:
        sandbox = AsyncMock()
        sandbox.name = "mock-sandbox-1"
        sandbox.state = "started"

        async def _always_fails() -> None:
            raise DaytonaError("Failed to refresh sandbox data: [Errno 32] Broken pipe")

        sandbox.refresh_data = _always_fails

        with pytest.raises(SandboxError) as exc_info:
            await _check_sandbox_health(sandbox)
        assert not isinstance(exc_info.value, SandboxGoneError)
        assert "Broken pipe" in str(exc_info.value)

    async def test_daytona_not_found_maps_to_sandbox_gone_error(self) -> None:
        sandbox = AsyncMock()
        sandbox.name = "mock-sandbox-1"
        sandbox.state = "started"

        async def _not_found() -> None:
            raise DaytonaNotFoundError("Sandbox with ID or name X not found")

        sandbox.refresh_data = _not_found

        with pytest.raises(SandboxGoneError) as exc_info:
            await _check_sandbox_health(sandbox)
        assert "no longer exists" in str(exc_info.value)
        assert isinstance(exc_info.value, SandboxError)

    async def test_dead_state_still_raises_plain_sandbox_error(self) -> None:
        sandbox = AsyncMock()
        sandbox.name = "mock-sandbox-1"
        sandbox.refresh_data = AsyncMock(return_value=None)
        sandbox.state = next(iter(sandbox_module._DEAD_SANDBOX_STATES))

        with pytest.raises(SandboxError) as exc_info:
            await _check_sandbox_health(sandbox)
        assert not isinstance(exc_info.value, SandboxGoneError)
        assert "crashed" in str(exc_info.value)


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


class TestUploadAgentArtifacts:
    @pytest.mark.parametrize(
        "exit_code,retryable",
        [
            (35, True),  # curl SSL/TLS error — transient, retry with new sandbox
            (1, False),  # generic failure — deterministic, fail the task
        ],
    )
    async def test_exit_code_maps_to_retryable_exception(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        exit_code: int,
        retryable: bool,
    ) -> None:
        """
        Exit code 35 (curl SSL/TLS) raises SandboxSetupError so process_task retries
        with a fresh sandbox. All other non-zero exit codes raise the base SandboxError,
        which marks the task as failed without a sandbox retry.

        Test Cases:
            - Exit code 35 raises SandboxSetupError (retryable — triggers a new sandbox)
            - Other non-zero exit codes raise SandboxError but not SandboxSetupError (non-retryable)
        """
        mock_sandbox = AsyncMock()
        mock_sandbox.name = "test-sandbox"

        monkeypatch.setattr(
            sandbox_module,
            "_exec",
            AsyncMock(return_value=ExecuteResponse(exit_code=exit_code, result="error output")),
        )
        monkeypatch.setattr(
            "tracker.sandbox.create_presigned_url",
            Mock(return_value="https://example.com/presigned"),
        )

        aws = AWSCredentials(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_default_region="us-east-1",
        )

        expected = SSLConnectionError if retryable else SandboxError
        with pytest.raises(expected) as exc_info:
            await upload_agent_artifacts(mock_sandbox, contract, "bench-123", aws, "test-bucket")

        if not retryable:
            assert not isinstance(exc_info.value, SandboxSetupError)
