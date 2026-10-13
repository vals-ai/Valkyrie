"""Secret CLI behavior through the real SDK and an HTTP mock transport.

Run: uv run pytest tests/unit/cli/test_secret.py
"""

import io
import json
from collections.abc import Callable
from functools import partial

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk import ValkyrieClient

import valkyrie.cli.secret as secret_cli
from valkyrie.cli.main import cli

_REFERENCE = "valkyrie/tenants/00000000-0000-0000-0000-000000000001/daytona"
_ENTRY = {"name": "daytona", "reference": _REFERENCE}

Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture
def requests() -> list[httpx.Request]:
    return []


@pytest.fixture
def invoke(monkeypatch: pytest.MonkeyPatch, requests: list[httpx.Request]) -> Callable[..., Result]:
    """Use an access key and replace only the SDK's HTTP transport."""
    monkeypatch.setenv("VALKYRIE_API_KEY", "test-key")
    monkeypatch.setenv("VALKYRIE_URL", "https://tracker.test")

    def run(*arguments: str, input: str | None = None, response: httpx.Response | None = None) -> Result:
        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if response is not None:
                return response
            body = {"secrets": [_ENTRY]} if request.method == "GET" else _ENTRY
            return httpx.Response(200, json=body)

        client = partial(ValkyrieClient, transport=httpx.MockTransport(handler))
        monkeypatch.setattr("valkyrie.cli.secret.ValkyrieClient", client)
        return CliRunner().invoke(cli, ["secret", *arguments], input=input)

    return run


@pytest.mark.parametrize("stdin", ['{"api_key": "s3cret"}\n', '  {"api_key": "s3cret"}\r\n\n'])
def test_set_sends_the_trimmed_value_only_in_the_body(
    invoke: Callable[..., Result], requests: list[httpx.Request], stdin: str
) -> None:
    result = invoke("set", "daytona", input=stdin)

    assert result.exit_code == 0, result.output
    (request,) = requests
    assert (request.method, request.url.path) == ("PUT", "/secrets/daytona")
    assert json.loads(request.content) == {"value": '{"api_key": "s3cret"}'}
    assert "s3cret" not in str(request.url)
    assert "s3cret" not in result.output
    assert _REFERENCE in result.output


def test_set_reads_a_hidden_prompt_on_a_terminal(
    invoke: Callable[..., Result], requests: list[httpx.Request], monkeypatch: pytest.MonkeyPatch
) -> None:
    class Terminal(io.StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(secret_cli.click, "get_text_stream", lambda _name: Terminal())

    result = invoke("set", "openai-key", input="tok-123\n")

    assert result.exit_code == 0, result.output
    assert json.loads(requests[0].content) == {"value": "tok-123"}
    assert "tok-123" not in result.output


@pytest.mark.parametrize(("name", "stdin"), [("daytona", " \n"), ("..", "value"), ("a/b", "value")])
def test_set_rejects_empty_values_and_bad_names_without_a_request(
    invoke: Callable[..., Result], requests: list[httpx.Request], name: str, stdin: str
) -> None:
    result = invoke("set", name, input=stdin)

    assert result.exit_code != 0
    assert requests == []


@pytest.mark.parametrize("output_format", ["text", "json"])
def test_list_shows_names_and_references(invoke: Callable[..., Result], output_format: str) -> None:
    result = invoke("list", "--format", output_format)

    assert result.exit_code == 0, result.output
    if output_format == "json":
        assert json.loads(result.output) == {"secrets": [_ENTRY]}
    else:
        assert "daytona" in result.output and _REFERENCE in result.output


@pytest.mark.parametrize(
    ("arguments", "answer", "deleted"), [(["-y"], None, True), ([], "y\n", True), ([], "n\n", False)]
)
def test_remove_asks_before_deleting(
    invoke: Callable[..., Result],
    requests: list[httpx.Request],
    arguments: list[str],
    answer: str | None,
    deleted: bool,
) -> None:
    result = invoke("remove", "daytona", *arguments, input=answer)

    assert result.exit_code == 0, result.output
    assert [(request.method, request.url.path) for request in requests] == (
        [("DELETE", "/secrets/daytona")] if deleted else []
    )


def test_api_errors_are_shown_without_a_traceback(invoke: Callable[..., Result]) -> None:
    missing = httpx.Response(404, json={"detail": "Secret 'daytona' does not exist"})

    result = invoke("remove", "daytona", "-y", response=missing)

    assert result.exit_code == 1
    assert "does not exist" in result.output
    assert "Traceback" not in result.output
