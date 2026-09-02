# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-02]

### Fixed
- Add `-H/--header` support to `valk run resume` and `valk run retry`, matching `run start`, so custom benchmark service auth headers (e.g. non-`Authorization` headers like srebench's `x-descope-api-key`) are forwarded correctly instead of causing resume/retry to fail authentication (#744)
