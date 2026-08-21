# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-21]

### Added
- Start managed benchmark runs without requiring static AWS access keys; Tracker and ExecutorHost use their deployment task-role credentials instead (#708)
- Preserve failure history across task attempt retries via a new task-attempt failure-history table (#688)
- Add a CI maintenance-approval gate: changes classified as maintenance-required now need explicit sign-off through a protected GitHub Environment before the deploy workflow proceeds (#695)
- Pass the tracker-validated model and reasoning-effort variant into benchmark setup as `VALKYRIE_AGENT_MODEL`/`VALKYRIE_AGENT_VARIANT`, trusted only when the tracker itself rebuilt the contract from the published agent bundle (#718)

### Changed
- Enable managed AWS execution in production for explicitly allowlisted organizations, giving Tracker scoped Secrets Manager access and ExecutorHost access to its own account/region secrets (#717)
- Enable managed AWS execution in dev (#704)
- Managed runtime and hosted mode now accept `MODEL_GATEWAY_URL`/`MODEL_GATEWAY_API_KEY` as allowed contract secrets instead of rejecting any declared secret reference (#727)
- Bump `create-benchmark-service` to v0.31.1, picking up an idle watchdog on the evaluation stream and removal of the per-poll Daytona sandbox refresh (#723)

### Fixed
- Infra maintenance classifier no longer forces a full maintenance outage when an ExecutorHost task-definition change is limited to environment variables; such changes now redeploy as rolling-safe (#722)
- Access-key AWS runs now return a clear error explaining that the run needs its original legacy AWS configuration, instead of a generic missing-header error (#719)
- Resume an interrupted evaluation stream from durable continuation state after a WebSocket disconnect or idle failure, instead of failing the task and losing sandbox ID, exit reason, and duration metadata (#702)
- Restore a single Alembic migration head after the failure-provenance and managed-AWS-execution migrations diverged (#711)
- Reset terminal runs before resuming an empty run so they no longer get stuck (#715)
- Back off between agent contract upload retries instead of retrying immediately (#703)
- Stop stale/old task work from overwriting a newer retry's results (#706)
- Recover opted-in tasks after sandbox loss instead of leaving them failed (#683)
