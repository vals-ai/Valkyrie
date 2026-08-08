---
name: repository-design-pattern
description: Use when adding, changing, or reviewing tracker database persistence; choose the right repository, preserve tenant and transaction boundaries, and test repository behavior.
---

# Tracker Repository Design Pattern

Tracker repositories own database queries and persistence rules for one domain area. They receive a caller-owned `sqlmodel.Session` and expose domain operations instead of leaking SQL into routes, orchestration helpers, or workers.

## Use a repository when

Use a repository when code:

- Reads or mutates tracker database rows.
- Must enforce organization (`org_id`) isolation.
- Combines several database writes into one domain operation.
- Depends on compare-and-set (CAS) predicates, execution authority, or row locks.
- Is used by more than one route, helper, or worker boundary.

Do not use a repository for provider, sandbox, benchmark-service, broker, dispatch-admission, or other external operations. Keep those operations in the caller-owned orchestration layer.

## Choose the repository

| Operation                                                         | Repository                |
| ----------------------------------------------------------------- | ------------------------- |
| Organization lookup or idempotent creation                        | `OrgRepository`           |
| Benchmark lookup, task selection, filters, or task counts         | `BenchmarkRepository`     |
| Task lookup, creation, runnable selection, or terminal results    | `TaskRepository`          |
| Evaluation results, score inputs, reporting, or pagination        | `ReportingRepository`     |
| Stop, retry, resume, or benchmark row locking                     | `RunControlRepository`    |
| Authority-fenced task status, errors, evaluation, or resume state | `TaskExecutionRepository` |

Import repositories from `tracker.database.repositories`. Add a method to the existing repository for its domain before creating another repository.

## Session and transaction ownership

Repositories never commit. The caller owns the session lifetime, transaction boundary, commit, and final rollback.

FastAPI routes should use the request-scoped providers in `tracker.database.dependencies`:

```python
from tracker.database.dependencies import BenchmarkRepositoryDep


def route(repository: BenchmarkRepositoryDep) -> None:
    ...
```

The providers depend on `get_session`. FastAPI caches that dependency per request, so multiple repository dependencies share one `Session`. Worker code should construct repositories from the worker's session at that session boundary.

Pass repositories explicitly into helpers. Do not construct a repository from a hidden global, create a second session inside a helper, or make a helper silently fall back to repository construction.

Inject a raw `Session` only when the route or helper directly uses it for a transaction, commit, rollback, lock, or fresh-session stream. Remove dead `session: Session = Depends(get_session)` parameters from routes that use repository dependencies instead. Run Ruff's `ARG` checks to catch unused session parameters.

A repository may roll back its current transaction when an authority fence, CAS predicate, or required row check rejects a write. Callers must treat a `False` result as a rejected operation and re-establish the intended transaction state before continuing.

The SSE results stream is an intentional exception to ordinary request scoping. It opens a fresh `Session`, `BenchmarkRepository`, and `ReportingRepository` for each poll so each event observes committed state without reusing a stale identity map.

## Preserve database invariants

Every organization-owned query and write must include the relevant `org_id` predicate. Do not trust a row loaded without scope for a tenant-owned operation.

Keep concurrency guards in the repository operation:

- Preserve `rowcount` checks for CAS updates.
- Preserve execution-authority locks and benchmark row locks.
- Keep force-stop and retry state changes in the same caller-controlled transaction phase.
- Return an explicit result such as `None`, `False`, a count, or a selection when the operation was not accepted.

A repository method should stage all writes for its domain operation. The caller decides when to flush, commit, retry, or hand control to an external operation.

## External-operation boundaries

Keep database persistence separate from external work. The caller should:

1. Use a repository to load or lock the required rows.
2. Commit any phase whose state must survive an external operation.
3. Perform provider, sandbox, benchmark-service, broker, or dispatch work.
4. Reacquire rows through the appropriate repository before finalizing state.

Do not add network calls, sandbox cleanup, broker enqueueing, or dispatch admission to a repository method.

## Adding a repository method

1. Identify the domain owner from the table above.
2. Read the existing repository and its callers before editing.
3. Define the method around an observable domain operation, not a generic SQL wrapper.
4. Pass `org_id` explicitly for organization-owned data.
5. Preserve existing status, authority, lock, and CAS predicates.
6. Do not commit inside the method.
7. Pass the repository explicitly through routes and helpers.
8. Add focused tests for the behavior and its failure or isolation path.

## Testing repository behavior

Keep repository unit tests together under:

```text
services/tracker/tests/unit/database/repositories/
```

Run the repository unit tests with:

From `services/tracker`, run:

```bash
uv run pytest tests/unit/database/repositories/test_repositories.py tests/unit/database/repositories/test_run_control_repository.py tests/unit/database/repositories/test_task_execution_repository.py -q
```

Test outcomes rather than mocked call wiring. Repository tests should cover the relevant risks:

- Organization isolation hides foreign rows.
- A caller rollback removes staged repository writes.
- A rejected authority or stale-attempt CAS leaves no partial terminal result.
- A dangling breakdown rolls back the evaluation and status changes together.
- Explicit empty task selections do not become whole-run operations.
- PostgreSQL row locks serialize concurrent run-control operations.

Use local integration tests under `services/tracker/tests/integration/local/database/` when the behavior depends on PostgreSQL locking, transaction isolation, or multiple sessions. Keep credentialed provider and sandbox coverage in the existing live integration locations.

## Avoid these patterns

- Calling `session.commit()` from a repository.
- Passing an unscoped model into a tenant-owned write without checking `org_id`.
- Replacing a CAS or authority predicate with a read-then-write sequence.
- Moving provider or sandbox calls into a repository to simplify a caller.
- Creating one repository per endpoint instead of grouping operations by domain.
- Adding a test that asserts only which collaborator was called.
