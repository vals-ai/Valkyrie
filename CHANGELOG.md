# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-31]

### Added
- Replace the legacy Worker/taskiq execution path with stable, digest-verified ExecutorHost releases following a `CANDIDATE` → `ACTIVE` → `DRAINING` → `RETIRED` lifecycle, so new Executor deployments can move new work forward without interrupting work already in progress (#601)

### Changed
- Run executor release-control preflight (dev and production) under a dedicated release role instead of the general deployment role, since the general role cannot read the executor release launch parameter (#631)

### Fixed
- Load the trusted maintenance classifier from the repository default branch instead of the PR base commit, and verify fetched base/candidate commits without executing candidate code, fixing prod deployment gate checks that failed because prod didn't yet contain the classifier; also bump the `create-benchmark-service` dependency from v0.22.3 to v0.22.4 across lockfiles (#632)
- Stop crashing with a misleading `ValueError` when reading agent command duration in the sandbox if the timing files are missing (e.g. `/tmp/.valkyrie` was removed mid-run); duration now falls back to a tracker-side monotonic measurement instead (#630)
