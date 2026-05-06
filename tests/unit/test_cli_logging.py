import io
import logging

import pytest

from valkyrie.cli.logging import configure_cli_logging


@pytest.fixture(autouse=True)
def reset_logging_disable() -> None:
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    yield
    logging.disable(previous_disable_level)


def _emit_info_log() -> str:
    stream = io.StringIO()
    logger = logging.getLogger("httpx")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    try:
        logger.info("HTTP Request: GET https://benchmark-tracker.vals.ai/health")
    finally:
        logger.removeHandler(handler)

    return stream.getvalue()


def test_configure_cli_logging_suppresses_logs_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALKYRIE_CLI_LOGS", raising=False)

    configure_cli_logging()

    assert _emit_info_log() == ""


def test_configure_cli_logging_allows_logs_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKYRIE_CLI_LOGS", "true")

    configure_cli_logging()

    assert "HTTP Request" in _emit_info_log()


@pytest.mark.parametrize("value", ["1", "TRUE", "yes", "on", "false"])
def test_configure_cli_logging_suppresses_logs_for_non_true_values(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKYRIE_CLI_LOGS", value)

    configure_cli_logging()

    assert _emit_info_log() == ""
