# Changelog

All notable changes to Valkyrie will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2026-06-28]

### Added

- Add run-level CloudWatch log and S3 artifact URL links in the tracker UI, with sortable task list columns (#489)
- Add run labels: database migration, model field, and API support for tagging runs with custom labels (#483)
- Include final benchmark score in run fetch API response (#475)
- Add automated daily changelog GitHub Actions workflow using Claude Code (#486)
- Document public agent registry in README with link to community agents (#478)

### Changed

- Rename "agent outputs" to "run outputs" throughout the codebase and UI (#485)
- Consolidate run attribution on `started_by_email`, dropping the separate `run_by_email` column (#482)
- Integrate updated benchmark service provider selection logic in tracker (#454)
- Allow `valkyrie connect` to be used after `run start` and `run stop` commands (#488)
- Upgrade Daytona sandbox dependency to 0.189.0 (#473)
- Update README to clarify sandbox provider config (AWS Secrets Manager) and `-s` flag behavior (#480)
- Rename hosted API key terminology in documentation (#481)

### Fixed

- Sanitize CloudWatch `logStreamName` to strip invalid characters (`:` and `*`) (#484)
- Fix task ID filtering logic in retry operations (#476)
- Address miscellaneous prod deploy review issues (#494)

### Removed

- Remove Python contract support; contracts are now YAML-only (#463)
