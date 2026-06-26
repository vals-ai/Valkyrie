# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2026-06-26]

### Added

- Add `--connect` flag to `run start`, `run resume`, and `run retry` to stream run updates immediately after the command succeeds, eliminating the need for a separate `run fetch --connect` step (#488)
- Add run labels: `--label`/`-l` on `run start` attaches a tag to the run; `--label`/`-l` on `run list` filters by label (case-insensitive) (#483)
- Show final score in `run fetch` output when a completed benchmark has a final evaluation (#475)
- Add automated daily changelog GitHub Actions workflow that collects merged PR details and opens a changelog PR via Claude (#486)

### Changed

- Move output download commands from `agent` to `run` subcommand: `valkyrie agent outputs` → `valkyrie run outputs`, `valkyrie agent output` → `valkyrie run output`; tracker endpoint renamed from `/fetch-agent-outputs/{run_id}` to `/fetch-run-outputs/{run_id}` (#485)
- Upgrade `daytona` dependency lower bound from `>=0.180.0` to `>=0.189.0` (#473)

### Fixed

- Sanitize CloudWatch log stream names by replacing `:` and `*` characters (forbidden by AWS) with `_`, preventing silent log loss for task IDs such as `provider/model:fast` (#484)
