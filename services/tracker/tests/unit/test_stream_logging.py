from tracker.utils import extract_stream_log_lines, normalize_stream_log, should_skip_stream_log_line


def test_normalize_stream_log_strips_ansi_and_normalizes_newlines() -> None:
    raw = "\x1b[92mhello\x1b[0m\r\nworld\rline\n"
    assert normalize_stream_log(raw) == "hello\nworld\nline\n"


def test_should_skip_stream_log_line_filters_openhands_serializer_noise() -> None:
    assert should_skip_stream_log_line(
        "/bundle/openhands/.venv/lib/python3.12/site-packages/pydantic/main.py:464: UserWarning: Pydantic serializer warnings:"
    )
    assert should_skip_stream_log_line(
        "PydanticSerializationUnexpectedValue(Expected 10 fields but got 5: Expected `Message`)"
    )
    assert should_skip_stream_log_line("return self.__pydantic_serializer__.to_python(")
    assert should_skip_stream_log_line(
        "/bundle/openhands/.venv/lib/python3.12/site-packages/requests/__init__.py:113: RequestsDependencyWarning:"
    )
    assert should_skip_stream_log_line("01:12:18 - openhands:INFO: view.py:76 - Inserting summary at offset 1")
    assert should_skip_stream_log_line(
        "01:12:18 - openhands:INFO: conversation_memory.py:868 - Initial user action (id=1) has been condensed."
    )
    assert should_skip_stream_log_line("01:06:02 - openhands:INFO: base.py:175 - Loaded plugins for runtime abc123")


def test_should_skip_stream_log_line_keeps_useful_openhands_output() -> None:
    assert not should_skip_stream_log_line(
        "20:11:00 - openhands:INFO: agent_controller.py:676 - Setting agent(CodeActAgent) state from AgentState.RUNNING to AgentState.FINISHED"
    )


def test_extract_stream_log_lines_preserves_partial_lines_until_complete() -> None:
    pending, lines = extract_stream_log_lines("partial line", "")
    assert pending == "partial line"
    assert lines == []

    pending, lines = extract_stream_log_lines(" continued\nnext line\n", pending)
    assert pending == ""
    assert lines == ["partial line continued", "next line"]


def test_extract_stream_log_lines_filters_chunked_spam_once_complete() -> None:
    pending, lines = extract_stream_log_lines("01:12:18 - openhands:INFO: view.py:76 - Insert", "")
    assert pending
    assert lines == []

    pending, lines = extract_stream_log_lines("ing summary at offset 1\nuseful output\n", pending)
    assert pending == ""
    assert lines == ["useful output"]
