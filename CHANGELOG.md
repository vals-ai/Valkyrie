# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-19]

### Added

- Add managed AWS execution: the CLI and SDK can start runs using the Tracker/ExecutorHost's own ECS task-role credentials instead of sending static AWS access keys. Local agent uploads and artifact downloads keep using the AWS SDK credential chain (including AWS SSO profiles), and configs with a complete `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` pair keep using the existing access-key path unchanged (#708, #545, #544)
- Persist the AWS execution mode (managed vs. access-key) on each run so retrying or resuming reuses the mode the run originally started with, instead of picking up whatever mode the current local config selects (#708, #544)
- Enable managed AWS execution in dev for the `vals.ai` organization allowlist, and make the `/aws-runtime` metadata endpoint report managed availability only when a request could actually start a managed run (#704)

### Changed

- Grant the dev ExecutorHost task role read access to all Secrets Manager secrets in the dev account/Region so agent contracts can resolve provider, agent, and webhook secrets without deployment inventory; production and release-test stay closed with no broad secret access (#704)
- Replace the maintenance-classification auto-fail with a required GitHub Environment approval gate: maintenance-classified infrastructure changes now wait for an authorized reviewer instead of requiring a force merge (#695)
- Resize the dev database instance from `t4g.small` to `r7g.large` (#698)

### Fixed

- Wait 30 seconds before the final retry when uploading agent artifacts to a sandbox, instead of retrying immediately, so a transient network/DNS fault has time to clear before the last attempt (#703)
- Finalize forced-stop state before sandbox provider cleanup runs, closing a race between stop finalization and cleanup (#694)
- Read dev deployment sizing expectations from stage config instead of a stale hardcoded value (#678)
