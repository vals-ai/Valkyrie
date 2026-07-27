# One-Way Run Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the new CLI and SDK canonical-only while retaining legacy compatibility exclusively at the Tracker HTTP and persistence boundaries.

**Architecture:** The Tracker continues exposing canonical `/runs` routes and the existing legacy routes. The CLI and SDK call only `/runs` and parse only canonical fields, eliminating route retry logic and legacy public-model aliases. Physical storage, task broker, payload, and telemetry compatibility remain unchanged.

**Tech Stack:** Python 3.12+, FastAPI, Pydantic v2, httpx, pytest, uv, Ruff, basedpyright

## Global Constraints

- Existing legacy Tracker request and response shapes remain unchanged.
- CLI and SDK calls use only canonical `/runs` routes and canonical run response fields.
- Physical database/Alembic names, S3 prefixes, Taskiq identities, persisted payload keys, and telemetry aliases remain unchanged.
- Legitimate benchmark-definition and benchmark-service terminology remains unchanged.
- Do not add replacement compatibility helpers or aliases.

---

### Task 1: Make the CLI Tracker client canonical-only

**Files:**
- Modify: `tests/unit/cli/test_tracker_client.py`
- Modify: `tests/unit/cli/test_tracker_http.py`
- Modify: `src/valkyrie/cli/tracker_client.py`

**Interfaces:**
- Consumes: canonical Tracker routes under `/runs`
- Produces: existing `TrackerService` public methods with no legacy HTTP retry behavior

- [ ] **Step 1: Replace fallback tests with a failing canonical-only test**

Replace the fallback test with a test whose handler returns an exact FastAPI
route-miss response for the canonical request and fails if any second request is
made:

```python
def test_tracker_client_does_not_retry_a_legacy_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"detail": "Not Found"}, request=request)

    # Construct TrackerService with MockTransport using the existing helper.
    with pytest.raises(TrackerServiceError, match="Not Found"):
        tracker.fetch_run(run_id)

    assert [request.url.path for request in requests] == [f"/runs/{run_id}"]
```

Delete `test_stream_fallback_reuses_shared_params_when_no_legacy_override` and
update HTTP fixture handlers in `test_tracker_http.py` to return canonical
payloads from `/runs/{run_id}`, `/runs/{run_id}/metadata`,
`/runs/{run_id}/results`, `/runs/{run_id}/results/exists`,
`/runs/{run_id}/analysis`, and `/runs/{run_id}/events`.

- [ ] **Step 2: Run the canonical-only test and verify it fails**

Run:

```bash
uv run pytest tests/unit/cli/test_tracker_client.py -k "does_not_retry_a_legacy_route" -vv
```

Expected: FAIL because the current client makes a second request to
`/fetch-benchmark`.

- [ ] **Step 3: Remove CLI fallback implementation**

Delete `_is_missing_route`, `_get_with_legacy_fallback`,
`_post_with_legacy_fallback`, `_patch_with_legacy_fallback`, and
`_stream_with_legacy_fallback`. Replace call sites with direct httpx calls:

```python
response = self._client.get(
    f"{self._base_url}/runs/{run_id}",
)
```

```python
with self._client.stream(
    "GET",
    f"{self._base_url}/runs/{run_id}/events",
    timeout=None,
) as response:
    ...
```

Preserve `fetch_benchmark_tasks` and benchmark-service names because they refer
to benchmark definitions and services rather than run instances.

- [ ] **Step 4: Run CLI client tests**

Run:

```bash
uv run pytest tests/unit/cli/test_tracker_client.py tests/unit/cli/test_tracker_http.py -vv
```

Expected: PASS with no requests to legacy run-instance routes.

### Task 2: Make the Python SDK canonical-only

**Files:**
- Modify: `tests/unit/sdk/test_sdk.py`
- Modify: `tests/unit/sdk/test_models.py`
- Modify: `tests/unit/sdk/test_run_tasks_resource.py`
- Modify: `tests/unit/sdk/test_run_workflows_v2.py`
- Modify: `tests/fixtures/sdk_api/start.json`
- Modify: `tests/fixtures/sdk_api/fetch.json`
- Modify: `tests/fixtures/sdk_api/list.json`
- Modify: `tests/fixtures/sdk_api/results.json`
- Modify: `packages/valkyrie-sdk/src/valkyrie/sdk/client.py`
- Modify: `packages/valkyrie-sdk/src/valkyrie/sdk/resources/runs.py`
- Modify: `packages/valkyrie-sdk/src/valkyrie/sdk/models/runs.py`
- Modify: `packages/valkyrie-sdk/src/valkyrie/sdk/models/run_tasks.py`

**Interfaces:**
- Consumes: canonical Tracker responses containing `run_id`, `runs`,
  `run_arguments`, `benchmark_name`, and `/runs` routes
- Produces: unchanged `ValkyrieClient.runs` method signatures and canonical
  `Run*` model names

- [ ] **Step 1: Write failing SDK canonical-only tests**

Replace the SDK fallback test with:

```python
async def test_runs_resource_does_not_retry_legacy_routes(make_client) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"detail": "Not Found"}, request=request)

    client = make_client(handler)
    async with client:
        with pytest.raises(ValkyrieAPIError):
            await client.runs.fetch(run_id)

    assert [request.url.path for request in requests] == [f"/runs/{run_id}"]
```

Add model tests that reject legacy-only fields:

```python
with pytest.raises(ValidationError):
    GetRunResponse.model_validate({**canonical_payload, "benchmark_id": run_id, "run_id": PydanticUndefined})

with pytest.raises(ValidationError):
    ListRunsResponse.model_validate({"benchmarks": []})
```

Use concrete payload dictionaries already provided by the test fixtures rather
than introducing new fixtures.

- [ ] **Step 2: Run the SDK tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/sdk/test_sdk.py tests/unit/sdk/test_models.py -k "does_not_retry or legacy" -vv
```

Expected: FAIL because the SDK retries legacy routes and accepts legacy fields.

- [ ] **Step 3: Remove SDK fallback implementation and aliases**

Change `ValkyrieClient.request_model` back to a single request:

```python
async def request_model(
    self,
    method: str,
    path: str,
    model: type[ResponseModel],
    *,
    params: dict[str, Any] | None = None,
    json: Any = None,
) -> ResponseModel:
    try:
        response = await self._client.request(method, path, params=params, json=json)
    except httpx.HTTPError as exc:
        raise ValkyrieTransportError(f"Valkyrie request failed: {exc}") from exc
    self.raise_for_status(response)
    return model.model_validate(response.json())
```

Delete `is_missing_route`, all `fallback_path` and `fallback_params` arguments,
and all two-route loops in `RunsResource`. Stream and analyze exactly one
canonical path.

Replace public SDK aliases with canonical fields:

```python
class GetRunResponse(ResponseModel):
    run_id: UUID
```

```python
class ListRunsResponse(ResponseModel):
    runs: list[RunSummary]
```

Remove `AliasChoices` imports when no longer used.

- [ ] **Step 4: Run all SDK tests**

Run:

```bash
uv run pytest tests/unit/sdk tests/contract/test_sdk_tracker_contract.py -vv
```

Expected: PASS; legacy Tracker routes remain in the Tracker schema contract,
while SDK request tests use only canonical routes.

### Task 3: Remove the impossible Tracker response guard

**Files:**
- Modify: `services/tracker/main.py`
- Modify: `services/tracker/tests/unit/test_main.py`

**Interfaces:**
- Consumes: `fetch_benchmark(..., connect=False)`, which always returns
  `FetchBenchmarkResponse`
- Produces: unchanged canonical `GET /runs/{run_id}` response

- [ ] **Step 1: Run the valid canonical route test before refactoring**

Run:

```bash
uv run pytest services/tracker/tests/unit/test_main.py -k "canonical_run_routes" -vv
```

Expected: PASS.

- [ ] **Step 2: Remove the defensive error-path test and runtime branch**

Delete
`test_get_run_returns_structured_500_for_unexpected_legacy_response`. Replace
the runtime `isinstance` branch with a static cast:

```python
response = cast(
    FetchBenchmarkResponse,
    await fetch_benchmark(
        benchmark_id=run_id,
        connect=False,
        session=session,
        harness_config=harness_config,
        org=org,
    ),
)
return GetRunResponse.from_legacy(response)
```

Import `cast` from `typing`. Remove comments added only to defend impossible or
implementation-obvious states.

- [ ] **Step 3: Run Tracker canonical and legacy route tests**

Run:

```bash
uv run pytest services/tracker/tests/unit/test_main.py tests/contract/test_sdk_tracker_contract.py -vv
```

Expected: PASS for both route families.

### Task 4: Sync, verify, publish, and update the PR

**Files:**
- Modify only files required to resolve overlap with the latest `origin/dev`
- Modify PR #622 description through GitHub

**Interfaces:**
- Consumes: latest `origin/dev`
- Produces: updated `ss/benchmark-run-rename` branch and accurate PR description

- [ ] **Step 1: Merge the latest target branch**

Run:

```bash
git fetch origin dev
git merge --no-edit origin/dev
```

Expected: clean merge or conflicts limited to files modified by both branches.

- [ ] **Step 2: Run repository verification**

Run the configured CLI, SDK, Tracker, contract, lint, typecheck, and package
commands from the repository workflows. At minimum:

```bash
uv run pytest tests/unit/cli tests/unit/sdk tests/contract -q
uv run pytest services/tracker/tests/unit -q
uv run ruff check .
uv run ruff format --check .
```

Then build fresh CLI/SDK artifacts and repeat the repository-defined smoke
commands from `.github/workflows/cli-tool-smoke-test.yaml` and
`scripts/sdk/verify_sdk_install.py`.

- [ ] **Step 3: Audit for forbidden legacy client dependencies**

Run:

```bash
rg -n "fallback_path|fallback_params|legacy_fallback|is_missing_route|/fetch-benchmark|/fetch-benchmarks|/stop-benchmark|/retry-or-resume-benchmark" \
  src/valkyrie/cli packages/valkyrie-sdk/src/valkyrie/sdk
```

Expected: no legacy run-instance route fallback in the CLI or SDK. Matches for
benchmark-definition/service endpoints remain allowed.

- [ ] **Step 4: Commit, push, and update PR #622**

Commit only the planned cleanup and merge resolution. Push
`ss/benchmark-run-rename`, then update the PR description so it states:

- Tracker provides backward compatibility.
- CLI and SDK are canonical-only.
- Tracker must deploy before the canonical-only clients.
- Fresh verification results correspond to the pushed head.
