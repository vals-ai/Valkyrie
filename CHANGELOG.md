# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-27]

### Security
- Restrict Tracker/ExecutorHost ECS security group egress from allow-all to explicit rules for VPC-internal Postgres (5432), Redis (6379), benchmark-service Cloud Map calls (8001), DNS (53), and HTTPS (443) (#840)

### Fixed
- Pin tracker's `taskiq` dependency to `<0.13` to prevent import failures caused by `taskiq-redis` relying on a compat shim removed in taskiq 0.13.0 (#855)
