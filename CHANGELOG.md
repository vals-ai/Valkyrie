# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-05]

### Changed
- Tracker infrastructure now self-provisions its ACM/TLS certificate per stage instead of importing a certificate ARN from an SSM parameter, and no longer imports the dev certificate alongside the account-local hosted zone (#645)
- Forward benchmark sandbox secret references to sandbox providers, with a new `InvalidSandboxConfigurationError` raised for invalid sandbox configurations (#642)
- CI: required dev checks (tracker unit/integration tests, executor build, SDK package/compatibility) now always trigger on every PR instead of being path-filtered, with an inline guard that skips the heavy work and reports success when the diff doesn't touch relevant paths — fixes PRs getting stuck waiting on required checks that GitHub never creates (#649)

### Fixed
- Fix 500 error when retrying or resuming a benchmark run that already had a saved final score; Tracker now clears the in-memory reference before deleting the old final-score record so SQLAlchemy doesn't attempt to re-save a deleted row (#646)
