# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-10]

### Added
- Add machine-readable CLI output: `run fetch --format json|jsonl` (one-time snapshot or connected stream), `run list --format json --all` (paginates every matching run into one document), and `run status --ids <id-1>,<id-2> --format json` (batch lookup with de-duplication and missing-ID reporting); connected text fetches now also show benchmark, agent, model, dataset, run ID, label, starter, concurrency, and start time before streaming progress (#522)
- Add optional `egress_allowlist` field to agent contracts, restricting sandbox network egress to the listed hosts only while the agent's `run_cmd` executes, then clearing the rules afterward so evaluation setup/teardown keep normal network access (#505)
- Add Harbor compose sandbox support: sandbox creation and command execution now recognize `ComposeSource`/`ComposeSandbox`, routing agent commands to the compose `main` service while leaving non-compose sandbox sources unchanged (#504)

### Fixed
- Fix agent timeout wrapping to run the command through `sh -c` with proper quoting instead of prefixing `timeout <n> <run_cmd>` directly, avoiding broken invocations for compound run commands (#504)
- Retry egress rule apply/clear operations on transient provider sandbox errors instead of failing immediately (#504)

### Changed
- Pin tracker to `create-benchmark-service` v0.12.0 for compose sandbox support, up from v0.11.0 added for egress allowlist support (#504, #505)
