# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-11]

### Added
- Add CloudWatch log retrieval for benchmark runs and tasks: paginated tracker log routes, a `client.logs` Python SDK resource (page/fetch/stream for run and task scopes), and a `valkyrie run logs` CLI command with text-query and time-range filters plus `--follow` live tailing; grants the tracker's IAM role scoped CloudWatch read permissions (#756)
- Add result previews for active runs: `valkyrie run results --preview` and `client.runs.preview()` publish a fresh S3 snapshot without changing run state, archiving the prior canonical result under `archive/<timestamp>/` first and invoking the run's completion callback; supports scoring a task subset via `--task-ids`/`--task-ids-file` and rejects requests that reference unknown task IDs (#757)

### Fixed
- Bump `create-benchmark-service` to v0.37.2 to stop the outer sandbox's `HOME=/root` from leaking into `docker compose exec`, which broke agent setup on Terminal-Bench 4 tasks whose images run as non-root (#771)
- Report Sentry events from the bench stage under their own `bench` environment instead of folding them into `production` (#747)
