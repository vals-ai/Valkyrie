---
name: writing-tests
description: Conventions and rubrics for generating unit and integration tests in the Valkyrie repo — test layout, module/test docstrings, naming, fixtures, mocking, determinism, typing, and integration cleanup. Use whenever writing, adding, editing, or reviewing tests (test_*.py) or setting up conftest fixtures.
---

# Generating Tests for Valkyrie

## Overview

This is a set of musts when generating tests for Valkyrie. There are two types of tests you can add:

1. **Integration tests** — These tests target specific parts of the codebase and rely on API calls and API keys. They are not mocked. Running them requires external dependencies and setup.
2. **Unit tests** — These tests target and mock parts of the codebase. They do not rely on API calls or API keys, so they can be run from anywhere, including from a fresh checkout of the codebase. Integration tests, by contrast, may require additional setup before they can run.

Aim for roughly a **70% unit / 30% integration** split. Unit tests are fast and run everywhere, so they carry the bulk of coverage; integration tests are slower and need credentials, so keep them targeted at the real API paths that matter. Skew toward more unit tests, but never drop integration coverage for an API call (see the integration rubrics).

For every test, write a docstring in the following format:

```python
"""
Short 1-2 sentence description of what the test covers.

Test cases:
- Test case 1
- Test case 2
...
"""
```

Inside each test, use short, single-sentence inline comments to describe the important parts. Keep them brief enough that someone can read the comment and immediately understand the code.

### Docstring example

```python
def test_parse_command_argument_handles_optional_flags() -> None:
    """
    Verify that command-argument parsing resolves optional flags and applies defaults.

    Test cases:
    - A flag passed explicitly overrides its default.
    - A flag omitted from the input falls back to its default.
    - An unknown flag raises a ValidationError.
    """
    ...
```

### Module docstrings

Every `test_*.py` module needs a module-level docstring at the top of the file. It must contain the pytest command to run that file, followed by 2-3 sentences describing what the module tests and what kinds of tests belong in it. This gives anyone opening the file an immediate way to run it and a clear sense of its scope.

```python
"""Tests for command-argument parsing.

Run: pytest tests/unit/cli/command_1/test_command_argument.py

Covers parsing of CLI command arguments — flag resolution, default values, and
validation errors. Add cases here for any new argument behavior or new flags on
the command_argument parser; keep provider- or transport-level concerns elsewhere.
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

Any API key required to run tests must be documented in the designated keys file if one already exists, so others can configure their environment without reading through test code. Name these keys with a `TEST_*` prefix to distinguish them from production keys and prevent accidental use against live systems.

The same keys must also be added to a `.local.env` template with empty values. This file is committed as the canonical list of keys a contributor needs to supply, so they can copy it and fill in their own values without exposing any secrets.

```bash
# .local.env — committed template, empty values only
TEST_VALKYRIE_API_KEY=
TEST_TRACKER_API_KEY=
```

```bash
# Designated keys file (e.g. .env) — local, filled in, never committed
TEST_VALKYRIE_API_KEY=...   # used by integration tests
TEST_TRACKER_API_KEY=...    # used by tracker integration tests
```

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

## When not to write tests

Not every change needs a test. An unnecessary test adds maintenance cost, slows the suite, and creates noise in PRs without protecting any behavior. Do not write a test when:

1. **There is no logic of yours to verify.** Trivial pass-throughs, constants, getters/setters, and one-line delegations have nothing meaningful to assert, and neither does third-party behavior — do not test the framework, the standard library, Pydantic, or an HTTP client. Assume dependencies work and test only how your code uses them (see unit rule 2).
2. **The test would not catch a real bug.** A change-detector that restates the implementation line for line breaks on every refactor without ever finding a defect, and a test written purely to raise the coverage number protects nothing. Assert on observable behavior; coverage is a signal, not the goal.
3. **The coverage already exists or the code is throwaway.** If an existing test exercises the path, extend it instead of adding a near-duplicate (see unit rules 9–10); if the code is a prototype or spike that will not ship, add tests once it becomes real.

Choosing the wrong layer also counts as an unnecessary test. Do not write an integration test when no real API call or external dependency is involved — that behavior belongs in a unit test. Conversely, every real API call does need integration coverage (see the integration rubrics), so do not rely on a mocked unit test to prove a live call works.

## Test location

Before creating tests, determine whether there are existing test modules that cover related functionality. Related tests should be coupled together.

Test module names must be short and descriptive — short enough that someone knows at a glance what the module contains. The submodule that holds the tests follows the same naming convention as the submodule of the code under test. Mirroring the layout makes related tests easy to find.

These paths are mirrored between unit and integration tests, but the two are never combined.

### Example

Source code:

```
cli/command_1/command_argument.py
cli/command_2/command_argument.py
```

Integration tests:

```
tests/integration/cli/command_1/test_command_argument.py
tests/integration/cli/command_2/test_command_argument.py
```

Unit tests:

```
tests/unit/cli/command_1/test_command_argument.py
tests/unit/cli/command_2/test_command_argument.py
```

### Path templates

Unit test path:

```
tests/unit/<feature-submodule>/test_<feature_1>.py
tests/unit/<feature-submodule>/test_<feature_2>.py
```

Integration test path:

```
tests/integration/<feature-submodule>/test_<feature_1>.py
tests/integration/<feature-submodule>/test_<feature_2>.py
```

## Running the suites

A test's suite is determined by where it lives — by directory, not by any marker. Unit tests live under `tests/unit/` and integration tests live under the `tests/integration/` submodule. Placing a test in that directory is what makes it an integration test; there is no `@pytest.mark.integration` decorator to add.

Run a suite by pointing pytest at its directory:

```bash
uv run pytest tests/unit          # unit suite
uv run pytest tests/integration   # integration suite (requires TEST_* keys)
uv run pytest                     # everything
```

Every test runs on every PR and before merge, so write each one to be ready for that. Do not park a test behind `skip`/`xfail` to keep it from running — those get forgotten and rot over time.

Coverage is available through `pytest-cov`. Point it at the package under test, `src/valkyrie`:

```bash
uv run pytest tests/unit --cov=src/valkyrie --cov-report=term-missing
```

New code must ship with tests, and coverage should not regress in a PR.

Full instructions for running the suites and the environment variables each one requires live in `services/tracker/README.md`. Treat that file as the source of truth for setup, and update it whenever the run steps or required variables change.

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

## Integration tests

When creating integration tests, follow these rubrics.

1. **Do not mock what is being tested.** The point of an integration test is to exercise the real component and its real dependencies.

2. **Use real flows that mirror what end users do.** Drive the test through the same paths a user would take when using the application.

   ```python
   # Lives under tests/integration/ — that location makes it an integration test; no decorator.
   def test_deploy_command_runs_against_live_api(
       api_client: ApiClient, unique_service_name: str
   ) -> None:
       """
       Exercise the full deploy command against the live API.

       Test cases:
       - A valid deploy request returns a completed status.
       - The created resource is retrievable after deploy.
       """
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

4. **Clean up remote resources the test creates.** Tests that create real entities (services, records, uploads) must delete them afterward, or each run pollutes the environment and later runs collide. Tear down in the fixture's `finally` so cleanup happens even on failure, and give each resource a unique name so parallel or repeated runs never clash.

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

5. **Source API keys and environment variables from a fixture, and validate them on initialization.** Never read or assert on environment variables inside a test module — that logic is hidden, gets duplicated, and cannot be reused. Centralize it in a fixture that fails fast with a clear message when a required value is missing. The fixture must fail instantly and must never `skip`: integration tests run on every PR and before merge, so a missing key is a misconfiguration to surface loudly, not a reason to silently pass.

   ```python
   # conftest.py
   import os
   import pytest

   @pytest.fixture(scope="session")
   def api_key() -> str:
       """Source and validate the API key once per session for all integration tests."""
       value = os.environ.get("TEST_VALKYRIE_API_KEY")

       # Fail fast (never skip) when the key is missing.
       if not value:
           pytest.fail("TEST_VALKYRIE_API_KEY must be set to run integration tests.")

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

7. **Cover every API call with at least one integration test.** Each piece of functionality that makes a real API call must have a designated integration test proving it works end to end against the live service. A unit test that mocks the call is not a substitute — it verifies wiring, not that the request actually succeeds. When you add or change a code path that hits an API, add or extend the integration test that exercises it.
