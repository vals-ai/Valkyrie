---
name: valkyrie-repo-guidance
description: "Load when working in vals-ai/Valkyrie to follow repo branch, setup, hosted-mode tracker, endpoint testing, and Sentry triage conventions."
---

# Valkyrie Repo Guidance

## Defaults

- Use `dev` as the base branch for current code, dependency checks, and PRs.
- CLI/orchestrator code is at the repo root; tracker service code is under `services/tracker`; AWS CDK infra is under `infra`.
- The CLI commands are `valkyrie ...` and the `valk ...` alias.

## Setup and checks

```bash
make install
make lint
make typecheck
make unit-test
(cd services/tracker && make install)
(cd services/tracker && uv run ruff format --check .)
(cd services/tracker && uv run ruff check .)
(cd services/tracker && uv run basedpyright)
(cd services/tracker && make test-unit)
```

Start the local tracker stack when needed:

```bash
(cd services/tracker && docker compose up -d)
curl http://localhost:8000/health
```

## Hosted-mode tracker testing

- `services/tracker/docker-compose.yml` must forward hosted-mode environment variables to every container that needs them. Verify with `docker exec ... env` after starting the stack.
- Hosted-mode local tests commonly need `AUTH_REQUIRED=true`, `DESCOPE_PROJECT_ID`, and `DESCOPE_MANAGEMENT_KEY` available in the tracker container; do not assume host environment variables reached Docker.
- Restart the tracker stack after changing compose env forwarding.
- `valk config` commands rewrite the config file; re-check the file after running them during a test.

## Endpoint testing

- Prefer testing benchmark flows through the CLI client.
- Do not raw-curl tracker benchmark endpoints that require harness context unless you also provide the required `X-Harness-*` headers. Missing harness headers can produce misleading 500s from malformed requests.
- `/health` is safe to curl directly.

## Valkyrie run error triage

- Sentry org: `vals-ai`; region URL: `https://us.sentry.io`; project slug: `valkyrie`.
- Query IDs as tags, not plain UUID text:
  - `benchmark_id:<run_id>` for a Valkyrie benchmark/run.
  - `task_id:<task_id>` for a single task.
- For run logs, search Sentry logs with `benchmark_id:<run_id> severity:error`. If error-event search routes to spans, fall back to issues plus issue events scoped by `benchmark_id`.
