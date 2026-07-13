# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-13]

### Fixed
- Strip leading/trailing whitespace from `valk config init` prompts (API key, and self-hosted required/defaulted values) so pasted values with stray newlines/spaces no longer get written verbatim into the config (#539)
- Bound agent dependency installs with a 10 minute timeout so a hung install command fails fast (as a retryable `SandboxError`) instead of pinning a worker indefinitely (#540)
