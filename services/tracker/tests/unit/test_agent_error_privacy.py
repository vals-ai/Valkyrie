"""Privacy checks for the optional sandbox-written error report.

Run: pytest tests/unit/test_agent_error_privacy.py
"""

from collections.abc import AsyncIterator
from unittest.mock import Mock

import pytest
from benchmark_service import ExecResult
from benchmark_service.sandbox import SandboxCommandError

from tracker.exceptions import AgentRunFailedError
from tracker.sandbox import create_agent_error_redactor, stream_command_output


async def reported_failure(content: str, secret_values: tuple[str, ...] | None) -> tuple[str, list[str]]:
    commands: list[str] = []

    async def command(_command: str) -> AsyncIterator[str]:
        yield "unrelated raw output"
        raise SandboxCommandError(1)

    async def execute(command: str) -> ExecResult:
        commands.append(command)
        if command.startswith("head -c "):
            limit = int(command.split()[2])

            return ExecResult(exit_code=0, output=content.encode()[:limit].decode(errors="replace"))

        return ExecResult(exit_code=0, output="1000000000")

    sandbox = Mock(id="privacy-test", command=command, exec=execute)
    sandbox.name = "privacy-test"
    sandbox.state = "started"

    with pytest.raises(AgentRunFailedError) as error:
        await stream_command_output(
            sandbox,
            "run-agent",
            lambda _: None,
            redact_error=None if secret_values is None else create_agent_error_redactor(secret_values),
        )

    return str(error.value), commands


class TestAgentErrorPrivacy:
    """Mask configured secrets while preserving bounded original-error summaries."""

    @pytest.mark.parametrize("content", ["test-secret-value", "ValueError: test-secret-value"])
    async def test_opaque_secrets_disable_optional_error_read(self, content: str) -> None:
        message, commands = await reported_failure(content, None)

        assert message == "Sandbox error: Agent command failed with exit code 1"
        assert not any(command.startswith("head -c ") for command in commands)

    async def test_known_secret_values_are_masked_before_error_is_raised(self) -> None:
        message, _ = await reported_failure(
            "ValueError: rejected test-secret-long and test-secret",
            ("test-secret", "test-secret-long", ""),
        )

        assert message.endswith("ValueError: rejected [REDACTED] and [REDACTED]")
        assert "test-secret" not in message
        assert "unrelated raw output" not in message

    async def test_multiline_secret_is_masked_before_whitespace_normalization(self) -> None:
        message, _ = await reported_failure("ValueError: rejected line-one\nline-two", ("line-one\nline-two",))

        assert message.endswith("ValueError: rejected [REDACTED]")

    @pytest.mark.parametrize("content", ["raw prompt or model output", "", "ValueError:", "ValueError: bad\x00text"])
    async def test_malformed_report_keeps_only_exit_code(self, content: str) -> None:
        message, _ = await reported_failure(content, ())

        assert message == "Sandbox error: Agent command failed with exit code 1"

    async def test_oversized_report_is_not_truncated_through_a_secret(self) -> None:
        content = "ValueError: " + "x" * 2030 + "test-secret-value"

        message, commands = await reported_failure(content, ("test-secret-value",))

        assert message == "Sandbox error: Agent command failed with exit code 1"
        assert any(command.startswith("head -c 2049 ") for command in commands)

    async def test_small_original_error_is_preserved_with_no_declared_secrets(self) -> None:
        message, _ = await reported_failure("ValueError: no patch\n", ())

        assert message.endswith("ValueError: no patch")
