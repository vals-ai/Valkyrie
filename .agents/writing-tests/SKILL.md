---
name: writing-tests
description: Use when adding or changing Valkyrie tests, choosing proof for a code change, or validating a PR; routes changed surfaces to unit, local-integration, SDK/contract, package, and approval-gated live suites without duplicate coverage.
---

# Generating Tests for Valkyrie

**Read this entire reference before writing tests. Apply every section and rubric below; do not skip any.**

## Overview

This is a set of musts when generating tests for Valkyrie. There are three owned test layers:

1. **Unit tests** — Isolated CLI or tracker behavior with external boundaries mocked.
2. **Local integration tests** — Credential-free vertical slices across repository-owned boundaries such as Click CLI → tracker client → FastAPI → database. These run in the default PR suites.
3. **Live integration tests** — External AWS, benchmark-service, sandbox, or other credentialed boundaries. These run separately and require explicit setup.

Choose the smallest owning layer or combination of layers that proves the changed behavior. Keep fast unit coverage broad, use local integration for repository-owned contracts, and reserve live integration for external boundaries that local tests cannot prove.

Keep tests **proportional to the change**. A one-line bug fix gets one narrow regression test, not a broad new suite; a new subsystem warrants fuller coverage. Test volume should track the size and risk of what changed.

## Pre-PR suite routing

Classify the changed surfaces before opening or updating a PR, then run every applicable default suite:

| Changed surface | Mandatory pre-PR proof |
| --- | --- |
| Root CLI/tracker client (`src/valkyrie/cli/**`) | Repo root `make test` |
| Standalone Python SDK or Tracker SDK contract | `uv run pytest tests/unit/sdk tests/contract -q` |
| SDK package, version, build, or release tooling | Local executable checks through artifact/install validation in `.github/workflows/sdk-package.yml`; final-head CI must pass the complete workflow, including compatibility jobs |
| Tracker API/worker/database only | `(cd services/tracker && make test)` |
| Infrastructure/CDK (`infra/**`) | `(cd infra && make lint && make test && make typecheck)` |
| AWS/benchmark-service/sandbox/external boundary | Applicable local proof, then the smallest existing path under `services/tracker/tests/integration/live/`, its named smoke, or the full tracker live target when the scope requires it—agent-triggered/manual execution needs exact approval |

A cross-layer change owns the union of its rows. Reuse or extend existing behavioral coverage; add a test only where existing coverage cannot prove the changed boundary. Regardless of semantic classification, any changed path matched by `.github/workflows/sdk-package.yml` also owns the SDK package row; one SDK/contract run may satisfy both SDK rows. For local SDK package proof, use the PR base SHA as `BASE_SHA`; do not duplicate CI-only upload work locally. Live proof never substitutes for required local proof.

Record the clean final PR HEAD SHA, changed surfaces, commands, results, and durations. A later PR commit, rebase, or dirty-tree change invalidates the evidence. If a live workflow intentionally starts red until manually authorized, treat that as a sentinel, not as a product failure or proof that a later head passed.

Do not use broad pytest discovery as a substitute for routing. In particular, from `services/tracker`, `uv run pytest` or `uv run pytest tests/integration` can collect credentialed live tests; use the table's explicit target or path.

Prefer a specific test name over prose. Add a one-sentence test docstring only when the name cannot explain the scenario; do not enumerate test cases that the assertions already show. Add inline comments only for non-obvious intent or constraints, not to narrate arrange/act/assert.

### Module docstrings

Every `test_*.py` module needs a module-level docstring at the top of the file. It must contain the pytest command to run that file, followed by one or two short sentences describing what the module tests. Keep it tight: a one-line summary plus the run command is usually enough. Do not pad it with "add cases here…" / "keep X elsewhere" boilerplate.

```python
"""Tests for command-argument parsing.

Run: pytest tests/unit/cli/command_1/test_command_argument.py

Covers CLI command-argument parsing: flag resolution, defaults, and validation errors.
"""
```

### Test naming

A test name should be short, concise, and describe the behavior under test. Avoid names that are vague (`test_works`, `test_command`) or incomplete (`test_parse`). State what is exercised and, where useful, the expected outcome.

```python
# Avoid: vague or incomplete.
def test_parse() -> None: ...
def test_deploy_works() -> None: ...

# Prefer: short but specific about the behavior.
def test_parse_argument_rejects_unknown_flag() -> None: ...
def test_deploy_command_returns_completed_status() -> None: ...
```

### Grouping tests into classes

Group tests that exercise the same unit into a `Test*` class so related cases live together and share fixtures. The class name names the unit under test; a one-line class docstring states its scope. Do not add `__init__` or inheritance — pytest collects plain classes.

```python
class TestValidateKwargs:
    """Validation of agent kwargs against the contract schema."""

    def test_applies_default_when_value_omitted(self) -> None:
        ...

    def test_rejects_unknown_kwarg(self) -> None:
        ...
```

### Documenting API keys

Local integration tests must not require credentials. For a live test, reuse the existing non-secret variable name from the nearest fixture/workflow and document any new name in `docs/contributing/tracker-service.mdx` plus the relevant CI secret mapping. Keep values only in the untracked local environment or approved secret store; do not invent a second `TEST_*` alias or a new committed env template.

### Spacing and inline comments

Structure each test as **arrange, act, assert**: set up the inputs and context, perform the single action under test, then assert on the outcome.

Separate logical steps with a blank line so each reads as a distinct unit. **This is independent of comments** — insert the blank line whenever a new step begins, even when there is no comment to introduce it. A new step includes performing the action, starting a fresh group of assertions, or pulling a value out of a result to assert on next. In particular, a statement that sets up the following assertions (for example, extracting a nested value before checking it) begins a new step: put a blank line before it instead of sandwiching it against the previous `assert`.

```python
# Avoid: a new step (extract, then assert on it) is sandwiched against the prior assert.
def test_start_benchmark_sets_provider_secret(...) -> None:
    tracker.start_benchmark(...)
    assert mock_http_client.json is not None
    harness_config = mock_http_client.json["harness_config"]
    assert isinstance(harness_config, dict)
    assert harness_config["sandbox_provider_secret_name"] == "DaytonaSecrets"

# Prefer: a blank line before each new step, with or without a comment.
def test_start_benchmark_sets_provider_secret(...) -> None:
    tracker.start_benchmark(...)

    assert mock_http_client.json is not None

    harness_config = mock_http_client.json["harness_config"]
    assert isinstance(harness_config, dict)
    assert harness_config["sandbox_provider_secret_name"] == "DaytonaSecrets"
```

A `return` always gets a blank line above it — it concludes a step, so never sandwich it against the line before.

```python
# Avoid: return sandwiched against the assignments.
def get(self, url: str) -> httpx.Response:
    self.url = url
    self.params = params
    self.json = json
    return httpx.Response(200, json={"status": "success"})

# Prefer: blank line before the return.
def get(self, url: str) -> httpx.Response:
    self.url = url
    self.params = params
    self.json = json

    return httpx.Response(200, json={"status": "success"})
```

When you do add an inline comment, put it on its own line directly above the code it describes, with a blank line above the comment. Never trail a comment on the end of a code line.

```python
# Prefer: comments introduce steps that are already separated by blank lines.
def test_deploy_command(api_client: ApiClient) -> None:
    # Run the command exactly as the CLI would invoke it.
    result = run_cli(["deploy", "--name", "svc"])
    assert result.status == "completed"

    # Confirm the resource is retrievable through the real API.
    fetched = api_client.get_service("svc")
    assert fetched.name == "svc"
```

### Async tests

This project configures `pytest-asyncio` in auto mode via `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

In auto mode, `pytest-asyncio` collects `async def` tests automatically, so you do **not** need to decorate them with `@pytest.mark.asyncio`. Just write the test as a coroutine.

```python
# Avoid: redundant marker when asyncio_mode is "auto".
@pytest.mark.asyncio
async def test_fetch_service_returns_payload():
    ...

# Prefer: no marker needed; the async test is collected automatically.
async def test_fetch_service_returns_payload() -> None:
    # Await the real coroutine under test.
    result = await fetch_service("svc")
    assert result.name == "svc"
```

Only add `@pytest.mark.asyncio` if the mode is changed to `strict`, or check the current `asyncio_mode` in `pyproject.toml` before deciding.

### Determinism

A test must produce the same result on every run. Never rely on randomness, wall-clock time, or sleeps for behavior you assert on. Seed any randomness, use fixed inputs, and freeze or inject the clock so the outcome is reproducible.

```python
# Avoid: result depends on the current time and a random value.
def test_token_is_unique():
    token = generate_token()  # uses random + datetime.now() internally
    assert token != generate_token()

# Prefer: control the inputs so the output is fixed and assertable.
def test_token_encodes_seeded_value(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pin the clock and the random source the generator reads from.
    monkeypatch.setattr("valkyrie.tokens.now", lambda: FIXED_TIMESTAMP)
    monkeypatch.setattr("valkyrie.tokens.random_suffix", lambda: "abc123")

    assert generate_token() == "2026-01-01-abc123"
```

When the code under test sleeps (a retry or backoff loop, for example), patch `time.sleep` in the unit test so it returns immediately. The test still exercises the retry logic, but runs instantly instead of waiting out real delays. (This applies only to unit tests, where the wait is the *code's* delay; integration tests wait on a real system and must poll instead — see the integration rubrics.)

```python
def test_fetch_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    # Skip the real backoff delay so the test does not actually wait.
    monkeypatch.setattr("valkyrie.client.time.sleep", lambda _seconds: None)

    result = fetch_with_retry(attempts=3)

    assert result.status == "ok"
```

### Fixture scope

Default to `scope="function"` so each test gets a fresh instance and tests stay isolated. Widen the scope only for resources that are expensive to build and safe to share read-only across tests (for example, a session-scoped API key or a read-only client). Never share mutable state across tests through a widened fixture — that reintroduces cross-test coupling.

### Prefer built-in fixtures

Reach for pytest's built-in fixtures before writing your own setup or teardown. Use `tmp_path` for temporary files and directories (never `tempfile` by hand or a hard-coded path), `capsys` to capture stdout/stderr, and `monkeypatch` to patch attributes and environment variables (it auto-reverts after the test). These are isolated and cleaned up for you, which removes a common source of cross-test leakage.

```python
def test_writes_config_to_disk(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # tmp_path is a unique directory, removed automatically after the test.
    config_path = tmp_path / "valkyrie.toml"

    write_config(config_path)

    # capsys captures what the function printed.
    assert "wrote config" in capsys.readouterr().out
```

### Prefer generator fixtures with `finally` cleanup

When a fixture provides a resource that needs teardown — a generator, context manager, client, or connection — yield it from a fixture and tear it down in a `finally` block. This keeps tests flat (no nested `with` blocks drifting rightward) and guarantees cleanup even when the test raises.

```python
@pytest.fixture
def sandbox() -> Iterator[Sandbox]:
    sb = create_sandbox()

    try:
        yield sb
    finally:
        # Runs even if the test fails.
        sb.cleanup()

def test_upload(sandbox: Sandbox) -> None:
    sandbox.upload_file("/tmp/x", b"data")
    assert sandbox.exists("/tmp/x")
```

### Linting and typing

Tests are held to the same standard as application code: `ruff` and `basedpyright` must both pass before a test is merged. Run them through the make targets:

```bash
make style       # ruff format + ruff check --fix
make typecheck   # basedpyright
```

Type every instance. Annotate fixtures, mock objects, and helper return types so `basedpyright` can check them, and `cast` when a value's static type is wider than what the test actually holds (for example, narrowing a `MagicMock` to the interface it stands in for). Do not leave instances as inferred `Any`.

```python
# Avoid: untyped fixture, value left as Any.
@pytest.fixture
def api_client(api_key):
    return ApiClient(api_key=api_key)

# Prefer: typed fixture, and cast where the static type is too wide.
from typing import cast

@pytest.fixture
def api_client(api_key: str) -> ApiClient:
    return ApiClient(api_key=api_key)

def test_dispatch_sends_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = MagicMock()
    monkeypatch.setattr("valkyrie.dispatch.client", mock_client)

    dispatch_event({"id": 1})

    # Cast the mock to the interface it replaces so attribute access is checked.
    cast(Client, mock_client).send.assert_called_once_with({"id": 1})
```

Avoid `# type: ignore` and `# noqa` / `# ruff: noqa`. Fix the underlying issue instead — add the annotation, cast the value, or restructure the code. Reach for an ignore only when a rule is genuinely wrong for a line, and when you must, scope it to the specific rule (`# type: ignore[reason]`, `# noqa: RULE`) and add a short comment explaining why.

### Unused arguments

`ruff`'s `ARG` rules flag unused function and method arguments, and tests are held to the same lint bar. Which of three cases applies decides the fix:

1. **Genuinely dead** — the argument is needed by nothing. Remove it.
2. **Intentionally unused but signature-bound** — a stub or lambda that replaces a real callable via `monkeypatch` must match that callable's signature even for parameters it ignores. Prefix those with an underscore (`_task`, `_output_root`); `ruff` ignores underscore-prefixed arguments.
3. **A fixture used only for its side effect** — a fixture that gates or primes the test (validating credentials, setting up state) but whose value the test never reads. Apply it with `@pytest.mark.usefixtures("...")` instead of taking it as an unused parameter. Do not underscore a fixture parameter: pytest injects fixtures by name, so `_fixture` would not resolve.

```python
# 2. Signature-bound stub: underscore the params the stub ignores.
def _stub_load_status(run_id: str, _output_root: Path) -> BuildStatusSummary:
    return BuildStatusSummary(run_id=run_id, total=0, statuses={})

# 3. Side-effect-only fixture: usefixtures, not an unused parameter.
@pytest.mark.usefixtures("aws_credentials")
def test_list_builds_reads_manifests() -> None:
    ...
```

**Exception — mock methods that mirror a real signature.** A `Mock*` client or protocol stand-in must keep the exact parameter names of the interface it replaces, because the code under test calls it by keyword (for example a boto3 client's `describe_images(repositoryName=..., maxResults=..., nextToken=...)`). Parameters the mock ignores are interface fidelity, not dead code — do not strip or rename them even though `ARG` flags them, and prefer explicit typed parameters over collapsing them into `**kwargs`, which loses the signature the mock exists to document.

## When not to write tests

Not every change needs a test. An unnecessary test adds maintenance cost, slows the suite, and creates noise in PRs without protecting any behavior. Do not write a test when:

1. **There is no logic of yours to verify.** Trivial pass-throughs, constants, getters/setters, and one-line delegations have nothing meaningful to assert, and neither does third-party behavior — do not test the framework, the standard library, Pydantic, or an HTTP client. Assume dependencies work and test only how your code uses them (see unit rule 2).
2. **The test would not catch a real bug.** A change-detector that restates the implementation line for line breaks on every refactor without ever finding a defect, and a test written purely to raise the coverage number protects nothing. Assert on observable behavior; coverage is a signal, not the goal.
3. **The coverage already exists or the code is throwaway.** If an existing test exercises the path, extend it instead of adding a near-duplicate (see unit rules 9–10); if the code is a prototype or spike that will not ship, add tests once it becomes real.

Choosing the wrong layer also counts as an unnecessary test. Isolated logic belongs in a unit test. A flow crossing repository-owned CLI/client/API/database boundaries belongs in local integration even when it needs no external credential. An external API call needs designated live integration or smoke ownership; do not rely on a mocked unit test to prove a live call works.

## Test behavior, not implementation lines

Assert the observable outcome of the finished behavior, not that a specific line ran or that an intermediate variable exists. Do not write "this line of code exists"-style assertions; write "when this functionality completes, it leads to this result."

```python
# Avoid: asserting an internal attribute was set or a line was reached.
# Prefer: assert the outcome the feature promises.
def test_run_start_creates_sandbox() -> None:
    result = start_run(cfg)

    # The behavior led to this object being instantiated.
    assert isinstance(result.sandbox, Sandbox)
```

### Do not mock the work and then assert the wiring

The most common form of this mistake: replace the function that does the real work with a fake that records how it was called, then assert on that record. The test now only proves "this line called that function with these arguments" — it restates the implementation, breaks on every refactor, and cannot catch a real bug because the behavior it was checking has been mocked away. This pattern often survives from TDD, where a collaborator was stubbed to make the first failing test pass; once the real behavior exists, replace the stub-and-assert-call test with an outcome test, or delete it (see "Remove TDD scaffolding once behavior is confirmed").

```python
# Avoid: the real work is mocked out, and the test asserts only that it was called.
def test_build_status_command(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Path]] = []
    monkeypatch.setattr(build_status_command, "load_build_status", lambda *args: calls.append(args))

    run_cli(["build-status", "--run-id", "run-1"])

    # Only proves the CLI forwarded these arguments — a change-detector.
    assert calls == [("run-1", Path("/tmp/harbor"))]

# Prefer: drive the command and assert the observable result (its output contract, a
# created resource, a raised error). If the command is thin glue over a helper that is
# already tested elsewhere, do not write this test at all.
def test_build_status_command_reports_counts(capsys: pytest.CaptureFixture[str]) -> None:
    run_cli(["build-status", "--run-id", "run-1"])

    status = json.loads(capsys.readouterr().out)
    assert status["run_id"] == "run-1"
    assert status["succeeded"] == 3
```

Two specific forms to reject outright:

- **Call-order or call-count assertions on internal collaborators** — e.g. asserting the order in which AWS clients were constructed (`assert client_calls == ["cloudformation", "codebuild", "s3"]`) or that a helper was invoked N times. These assert *how* the code is wired, not *what* it produces.
- **Asserting a mock recorded exact arguments** as the test's only assertion, when the behavior those arguments drive has itself been mocked out.

When you do need to check that a real external call was made correctly (e.g. the exact payload sent to a boto3 client that you are deliberately faking at the boundary), assert on the one payload that represents the contract — not on construction order, call counts, or every intermediate call.

### Assert the contract in output, not the formatting

CLI or API output assertions are fine when the output is a real contract — machine-readable JSON, an exit code, a documented message. Assert the fields that matter, not the entire payload verbatim, and never assert incidental human formatting.

```python
# Avoid: brittle — restates the whole dict and checks table formatting.
assert json.loads(out) == {"run_id": "run-1", "succeeded": 3, "failed": 0, "pending": 0, "elapsed_ms": 812}
assert "Run ID" in out
assert "731" in out

# Prefer: assert the meaningful fields of the contract.
status = json.loads(out)
assert status["run_id"] == "run-1"
assert status["succeeded"] == 3
```

When several tests only vary the input and expected output of the same call, `parametrize` them instead of copy-pasting the full assertion (see unit rule 11).

## Remove TDD scaffolding once behavior is confirmed

Tests written before implementation to drive TDD are scaffolding. Once the real functionality is in place and verified to match, delete or replace placeholder and now-obsolete assertions instead of leaving them behind — for example, a test that asserts "value `x` is no longer in object `y`" that only reflected a mid-development state. Keep the tests that describe the shipped behavior; drop the ones that only described the path to it.

A common leftover is the stub-and-assert-call test: a collaborator was mocked to make the first failing test pass, and the test still only checks that the collaborator was called. Replace it with an outcome assertion or delete it — see "Do not mock the work and then assert the wiring."

## Test location

Before creating tests, determine whether an existing module already owns the behavior and extend it. Follow the nearby organization first—for example, root CLI local flows are consolidated in `test_read_commands.py` and `test_write_commands.py`—and mirror source layout only when no existing aggregate owner fits.

Test module names must be short and descriptive. Do not create a second module merely to mirror a source path when an existing behavioral module owns the flow.

Paths may mirror between unit and integration tests when no existing aggregate owner fits; the layers remain separate.

### Example

Illustrative source code:

```
src/valkyrie/cli/run/fetch.py
src/valkyrie/cli/run/update.py
```

Local integration tests:

```
tests/integration/local/cli/test_read_commands.py
tests/integration/local/cli/test_write_commands.py
services/tracker/tests/integration/local/api/test_benchmarks_status.py
```

Live integration tests:

```
services/tracker/tests/integration/live/<external-boundary>/test_<flow>.py
```

Unit tests:

```
tests/unit/cli/run/test_fetch.py
tests/unit/cli/run/test_update.py
```

### Path templates

Unit test path:

```
tests/unit/<feature-submodule>/test_<feature_1>.py
tests/unit/<feature-submodule>/test_<feature_2>.py
```

Local integration test paths:

```
tests/integration/local/<feature-submodule>/test_<feature>.py
services/tracker/tests/integration/local/<feature-submodule>/test_<feature>.py
```

Live integration test path:

```
services/tracker/tests/integration/live/<external-boundary>/test_<flow>.py
```

## Organizing conftest fixtures hierarchically

Just as tests mirror the source submodule layout (see "Test location"), the fixtures that support them should be broken up the same way. Do not let a single top-level `conftest.py` grow into a catch-all. Pytest discovers `conftest.py` by walking the directory tree, so a fixture defined in a `conftest.py` is available to that directory and everything beneath it. Place each fixture at the shallowest level where it is actually shared, and no higher.

```
tests/
  unit/
    cli/
      conftest.py             # cli_runner and other CLI-only fixtures
  integration/
    local/
      conftest.py             # credential-free local boundary fixtures
services/tracker/tests/
  conftest.py                 # tracker-wide fixtures
  integration/
    conftest.py               # shared integration credentials and helpers
    local/
      conftest.py             # tracker-local boundary fixtures
    live/
      conftest.py             # live-boundary clients and cleanup
```

Guidelines:

- Put a fixture in the **lowest** `conftest.py` that covers every test using it. A fixture used only by CLI tests belongs in `tests/unit/cli/conftest.py`, not the root. This keeps each `conftest.py` small and makes it obvious which fixtures belong to which area.
- Only promote a fixture upward when a second sibling area needs it. Widening scope prematurely turns the root `conftest.py` back into a junk drawer.
- A deeper `conftest.py` can override a fixture name from a shallower one when a subtree needs different setup; prefer this to adding conditionals inside one shared fixture.
- Do not create empty or pass-through `conftest.py` files just to mirror a directory — add one only when that level actually introduces a fixture.

When you instead need to split fixtures **within a single scope** by topic (not by directory), one `conftest.py` per folder is the limit — you cannot add a second. Move the fixtures into a `fixtures/` package of plain modules and register them from the rootdir `conftest.py`:

```python
# tests/conftest.py
pytest_plugins = [
    "tests.fixtures.db",
    "tests.fixtures.api",
]
```

Rule of thumb: split by **directory** → nested `conftest.py`; split by **topic** within one scope → a `fixtures/` package wired in via `pytest_plugins`.

## Running the suites

A test's suite is determined by its root and directory, not by a marker. Use the pre-PR routing table above. Root `make test` does not collect `tests/unit/sdk` or `tests/contract`; SDK package/release changes must mirror every local check in `.github/workflows/sdk-package.yml`, not only pytest. Credentialed live tests are separately owned by `.github/workflows/tracker-integration-tests.yaml`; agent-triggered/manual execution requires exact approval, while automatic CI follows that workflow's protected-branch gate. Do not park required local proof behind `skip`/`xfail`.

Behavior changes need existing or focused proof, and coverage should not regress. Registry, documentation, generated-output-only, and refactor-only changes need no new test when existing checks already prove them.

Tracker commands and live environment variables are documented in `docs/contributing/tracker-service.mdx`; suite ownership and final-head routing are defined here and in the Make/workflow targets.

## Unit tests

When creating unit tests, follow these rubrics.

1. **Do not create verbose tests.** Tests should be consistent and purposeful. A test that does not cover behavior which can change and break a flow has no purpose. Keep tests short and to the point, and use the mock constructor classes provided in `conftest.py` (or add your own there) to reduce setup and boilerplate.

2. **Do not test basic Pydantic functionality.** Tests should focus on behavior, not types. Asserting that an attribute exists or was set, with no intermediate flow that mutates it, provides little testing value. These tests churn whenever the underlying type changes and add needless verbosity and noise to PRs.

   ```python
   # Avoid: tests only that the model stores what it was given.
   def test_user_model_sets_name():
       user = User(name="ada")
       assert user.name == "ada"  # Pydantic already guarantees this.

   # Prefer: test the behavior built on top of the model.
   def test_user_display_name_falls_back_to_email_when_name_missing() -> None:
       user = User(email="ada@example.com")

       # Display name should derive from the email local-part when no name is set.
       assert user.display_name == "ada"
   ```

3. **Do not make test classes private, and do not nest them inside methods.** Classes should not be private, and they should not live inside methods unless the class being mocked does the same (unlikely — so don't). Do not name mock classes `Fake*`; they are `Mock*` classes. Large, reusable classes belong in `conftest.py`, not inlined in a test module.

   ```python
   # Avoid
   def test_dispatch_routes_event():
       class _fakeClient:  # private, inlined, and named "fake"
           def send(self): ...

   # Prefer (in conftest.py)
   class MockClient:
       """Records calls so tests can assert on dispatch behavior."""

       def __init__(self) -> None:
           self.sent_payloads: list[dict[str, object]] = []

       def send(self, payload: dict[str, object]) -> None:
           self.sent_payloads.append(payload)
   ```

4. **Always import at the top of the module; avoid inline imports.** Imports belong at the top and must respect lint rules. The only acceptable reason to import inside a method is to break a circular import, or to patch a symbol that a fixture does not already provide.

5. **Name variables fully; do not abbreviate.** Variable names should be specific and human-readable. Abbreviations are hard to parse and not descriptive. Names follow standard conventions, derive from the class they represent, use `snake_case`, and respect singular/plural rules.

   ```python
   # Avoid
   res = parse(arg)
   usrs = [u1, u2]

   # Prefer
   parsed_argument = parse(raw_argument)
   active_users = [first_user, second_user]
   ```

6. **Use fixtures and `conftest.py` before writing utilities.** Fixtures hold commonly used instances that can be reused — mocked objects and objects that take setup to construct are good candidates. Put them in `conftest.py` so later tests can reuse them. Always check `conftest.py` before writing a new utility; an existing fixture can often be extended without breaking current callers, which reduces duplication.

   ```python
   # conftest.py
   import pytest

   @pytest.fixture
   def command_argument() -> CommandArgument:
       """A baseline command argument other tests can adjust as needed."""
       return CommandArgument(name="deploy", flags={"force": False})
   ```

   A plain helper function is fine — and clearer than a fixture — when the setup is a pure transform that takes arguments and returns a value with no teardown (for example, `_write_yaml(tmp_path, content)`). Use a fixture when the setup is shared, stateful, or needs teardown. Rule of thumb: parameterized, throwaway setup → helper; shared lifecycle → fixture.

   When the same object is constructed in many tests — for example Click's `CliRunner` — expose it as a fixture instead of reinstantiating it in every test. Tests then receive it as a parameter, and the fixture's scope (see "Fixture scope") controls how often it is rebuilt: keep the default function scope unless construction is expensive and the object is safe to share.

   ```python
   # Avoid: every test rebuilds the same runner.
   def test_cli_prints_help() -> None:
       runner = CliRunner()
       result = runner.invoke(cli, ["--help"])

       assert result.exit_code == 0

   # Prefer (in conftest.py): construct it once and inject it.
   @pytest.fixture
   def cli_runner() -> CliRunner:
       return CliRunner()

   # Tests receive the runner as a parameter.
   def test_cli_prints_help(cli_runner: CliRunner) -> None:
       result = cli_runner.invoke(cli, ["--help"])

       assert result.exit_code == 0
   ```

7. **Keep tests isolated to the functionality under test.** Focus on the change you made, not on features or behavior that already existed and is covered elsewhere.

8. **Follow the conventions of nearby tests.** Read other tests in the same module to learn the established patterns and format. Do not invent new formats or conventions when an existing one works.

9. **Consolidate related tests.** A single test can cover more than one case. If a feature adds an object or attribute, extend an existing test with a few lines rather than creating a new one. Tests accumulate coverage over time and become more thorough than many small, shallow ones.

10. **Prefer one test covering multiple cases over many single-case tests.** This follows from rule 9. Use the docstring's "Test cases" section to make the covered cases explicit.

    ```python
    def test_command_argument_validation() -> None:
        """
        Validate command-argument parsing across valid and invalid inputs.

        Test cases:
        - A well-formed argument parses successfully.
        - A missing required field raises ValidationError.
        - An unknown flag raises ValidationError.
        """
        # Well-formed input parses and preserves the flag.
        parsed = parse_argument({"name": "deploy", "force": True})
        assert parsed.flags["force"] is True

        # Missing the required name is rejected.
        with pytest.raises(ValidationError):
            parse_argument({"force": True})

        # Unknown flags are rejected.
        with pytest.raises(ValidationError):
            parse_argument({"name": "deploy", "unknown": True})
    ```

11. **Use `@pytest.mark.parametrize` for independent input/output pairs.** Rules 9–10 are about consolidating a *flow* into one test body. When the cases are independent variations of the same call — different inputs producing different outputs — parametrize instead, so each case is reported separately and a failure points to the exact input.

    ```python
    @pytest.mark.parametrize(
        ("raw_argument", "expected_force"),
        [
            ({"name": "deploy", "force": True}, True),
            ({"name": "deploy"}, False),  # default applied when omitted
        ],
    )
    def test_parse_argument_resolves_force_flag(
        raw_argument: dict[str, object], expected_force: bool
    ) -> None:
        """Each input/output pair is reported as its own case."""
        assert parse_argument(raw_argument).flags["force"] is expected_force
    ```

12. **Patch with `monkeypatch` for methods and `MagicMock` for objects.** Use `monkeypatch.setattr` to replace a method, attribute, or function, and patch it where it is *used*, not where it is defined. Use `MagicMock` (or a `Mock*` class from `conftest.py`) when you need a stand-in object whose calls you assert on. Mock only the external boundary — never the unit under test.

    ```python
    def test_dispatch_sends_payload(monkeypatch: pytest.MonkeyPatch) -> None:
        # Replace the network method where the dispatcher imports it.
        mock_client = MagicMock()
        monkeypatch.setattr("valkyrie.dispatch.client", mock_client)

        dispatch_event({"id": 1})

        # Assert on the call made to the mocked object.
        mock_client.send.assert_called_once_with({"id": 1})
    ```

13. **Make module-local constants private.** A constant used only inside a single test module should be prefixed with an underscore, signalling it is local to that module and not meant to be imported elsewhere. (This is the opposite of rule 3 for classes: mock classes are shared and public in `conftest.py`, whereas one-off test data is private to its module.) If a constant is needed by more than one module, move it to `conftest.py` and drop the prefix.

    ```python
    # Avoid: looks shared/importable, but is only used in this module.
    RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
    STARTED_AT = datetime(2026, 6, 24, tzinfo=timezone.utc)

    # Prefer: the underscore marks it private to this test module.
    _RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
    _STARTED_AT = datetime(2026, 6, 24, tzinfo=timezone.utc)
    ```

14. **Mock at the right boundary: `pytest-httpx` for external APIs, FastAPI overrides for internal services.** Mock outbound calls to external/third-party APIs with `pytest-httpx` (`httpx_mock`). Mock internal FastAPI services with dependency overrides (`app.dependency_overrides`), not raw HTTP mocks, so the internal contract is exercised through the app.

    ```python
    # External API → pytest-httpx.
    def test_fetch_user(httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url="https://api.vendor.com/users/1", json={"id": 1})

        assert get_user(1).id == 1

    # Internal FastAPI service → dependency override.
    def test_create_order(client: TestClient) -> None:
        app.dependency_overrides[get_db] = lambda: fake_db

        assert client.post("/orders", json={...}).status_code == 201
    ```

15. **Shorten repetitive mock setup with `functools.partial`.** When the same mock is configured repeatedly with only one or two varying arguments, bind the constants with `partial` instead of repeating the full call.

    ```python
    from functools import partial

    add_ok = partial(httpx_mock.add_response, url=API_URL, status_code=200)
    add_ok(json={"page": 1})
    add_ok(json={"page": 2})
    ```

16. **Extract inlined logic to module-level helpers, and use factories for many-param objects.** Do not inline large blocks of setup or logic inside a test body; move them to module-level helpers (or `conftest.py`) that the test calls, so the test reads as intent, not machinery. When a constructor needs many fields, write a factory helper with sensible defaults so each test sets only the fields that matter, instead of spreading a long constructor call across every test.

    ```python
    # Module-level factory with defaults — tests override only what matters.
    def _make_task(**overrides: object) -> Task:
        defaults = dict(id="t1", status="PENDING", model="gpt-4", dataset="d1", retries=0)
        return Task(**{**defaults, **overrides})

    def test_error_task_is_retryable() -> None:
        task = _make_task(status="ERROR")

        assert task.is_retryable()
    ```

17. **Collapse duplication as you add tests.** After adding a test, check whether it overlaps an existing one. Fold independent rejection or input/output variations into an existing `@pytest.mark.parametrize` case list instead of writing a new function, and delete any test whose code path a new guard or refactor has made unreachable — for example, a lower-layer test for an input that an upper layer now rejects.

## Integration tests

When creating integration tests, follow these rubrics.

1. **Choose local versus live ownership before mocking.** Use local integration for repository-owned boundaries and live integration only for external systems. Exercise every repository-owned component in the selected local slice; keep the external boundary real in a live test. A cross-layer change requires both owning suites and behavioral coverage at the changed boundary, but reuse existing coverage when it already proves the contract.

2. **Use real flows that mirror what end users do.** Drive the test through the same paths a user would take when using the application.

   ```python
   # Lives under services/tracker/tests/integration/live/; no decorator is needed.
   def test_deploy_command_runs_against_live_api(
       api_client: ApiClient, unique_service_name: str
   ) -> None:
       """A valid deploy is completed and retrievable through the live API."""
       # Run the command exactly as the CLI would invoke it.
       result = run_cli(["deploy", "--name", unique_service_name])
       assert result.status == "completed"

       # Confirm the resource exists by fetching it back through the real API.
       fetched = api_client.get_service(unique_service_name)
       assert fetched.name == unique_service_name
   ```

3. **Clean up clients and connections after the test runs** when they are not torn down by default. Integration tests open real clients, sockets, and sessions; leaking them causes resource exhaustion, flaky runs, and cross-test interference. Prefer a generator-based fixture that yields the resource and tears it down in a `finally` block, so cleanup happens even when the test fails.

   ```python
   # conftest.py
   from collections.abc import Iterator

   import pytest

   @pytest.fixture
   def api_client(api_key: str) -> Iterator[ApiClient]:
       """Provide a live API client and guarantee it is closed after the test."""
       client = ApiClient(api_key=api_key)

       # Yield the connected client to the test.
       try:
           yield client
       finally:
           # Always close the client, even if the test raises.
           client.close()
   ```

4. **Clean up remote resources a live test creates.** Tests that create real entities (services, records, uploads) must delete them afterward, or each run pollutes the environment and later runs collide. Tear down in the fixture's `finally` so cleanup happens even on failure, and give each resource a unique name so parallel or repeated runs never clash.

   ```python
   # conftest.py
   import uuid
   from collections.abc import Iterator

   import pytest

   @pytest.fixture
   def unique_service_name() -> str:
       """A collision-free name so repeated and parallel runs do not clash."""
       return f"integration-test-{uuid.uuid4().hex[:8]}"

   @pytest.fixture
   def created_service(api_client: ApiClient, unique_service_name: str) -> Iterator[Service]:
       """Create a service for the test and always remove it afterward."""
       service = api_client.create_service(unique_service_name)

       try:
           yield service
       finally:
           # Delete the resource even if the test fails midway.
           api_client.delete_service(unique_service_name)
   ```

5. **Source live API keys and environment variables from a fixture, and validate them on initialization.** Never read or assert on environment variables inside a test module. Centralize live setup in a fixture that fails fast with a clear message when an explicitly selected live suite is missing a required value. Never make the default local suites depend on this fixture.

   ```python
   # conftest.py
   import os
   import pytest

   @pytest.fixture(scope="session")
   def api_key() -> str:
       """Source and validate the API key once per selected live-test session."""
       value = os.environ.get("BENCHMARK_SERVICE_AUTH_KEY")

       # Fail fast (never skip) when the key is missing.
       if not value:
           pytest.fail("BENCHMARK_SERVICE_AUTH_KEY must be set to run this live suite.")

       return value
   ```

6. **Wait on conditions, not on `sleep`, and fix flakes rather than masking them.** Real systems are eventually consistent, but an arbitrary `sleep()` is both slow and flaky. Poll the actual condition with a bounded timeout instead. The wait here is real, so you cannot mock it away (unlike a unit test, where you patch `time.sleep`; see Determinism). If a test is flaky, fix the root cause — do not paper over it with blanket reruns (`pytest-rerunfailures`), which hide real bugs and let failures merge.

   ```python
   def wait_for_service_ready(api_client: ApiClient, name: str, timeout_seconds: float = 30.0) -> None:
       """Poll until the service reports ready or the timeout elapses."""
       deadline = time.monotonic() + timeout_seconds

       # Re-check the real status until it is ready or time runs out.
       while time.monotonic() < deadline:
           if api_client.get_service(name).status == "ready":
               return

           # Small backoff between polls, not a fixed guess at the total wait.
           time.sleep(0.5)

       pytest.fail(f"Service {name} was not ready within {timeout_seconds}s.")
   ```

7. **Give every API call an explicit owner.** Internal CLI → tracker HTTP calls need credential-free local coverage through the changed boundary. Calls to AWS, benchmark services, sandboxes, or other external systems need a designated live test or named smoke. A mocked unit test is not a substitute for either contract.

8. **Gate only live tests that actually need credentials.** Credential-free local integration runs everywhere. Depend on a fail-fast credential fixture only from tests that genuinely use that external credential, so an intentionally selected live suite fails clearly without contaminating local ownership. A fixture that only gates on a credential should be applied with `@pytest.mark.usefixtures(...)`, not taken as an unused parameter.

9. **Do not duplicate coverage a smoke or dispatch workflow already owns.** Some external flows are exercised by a separate CI/smoke workflow that provisions credentials, runs a real build, and tears it down. Reference that exact workflow and final-head run instead of duplicating a costly path, but still keep repository-owned local coverage. Reserve the live pytest suite for cheap, self-cleaning external round trips.
