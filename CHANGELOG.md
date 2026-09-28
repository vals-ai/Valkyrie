# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-28]

### Security
- Restrict Tracker and ExecutorHost ECS security group egress from allow-all (`0.0.0.0/0`, all protocols) to explicit rules for VPC-internal Postgres (5432), Redis (6379), benchmark-service Cloud Map calls (8001), DNS (53 TCP/UDP), and HTTPS (443) (#840)

### Fixed
- Pin `create-benchmark-service` to v0.41.1 and bound `taskiq` to `<0.13` across Tracker, executor-artifact, and root lockfiles to restore Modal retry egress, which had been failing before generation due to a Taskiq/taskiq-redis incompatibility (#857)
