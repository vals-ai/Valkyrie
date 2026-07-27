# One-Way Run Compatibility

## Goal

Keep the benchmark-to-run migration backward compatible for existing Tracker
consumers without making the new CLI and SDK depend on legacy Tracker routes or
response fields.

## Boundary

- The Tracker exposes both canonical `/runs` routes and the existing legacy
  routes.
- Existing legacy Tracker request and response shapes remain unchanged.
- The CLI and SDK call only canonical `/runs` routes and accept canonical run
  response fields.
- Physical database and Alembic names, S3 prefixes, Taskiq identities,
  persisted payload keys, and telemetry aliases remain unchanged.
- Legitimate benchmark-definition and benchmark-service names remain unchanged.

## Code Changes

- Remove CLI and SDK legacy-route fallback helpers, parameters, call sites, and
  tests.
- Remove legacy field aliases from public SDK run models.
- Remove runtime checks for response states that are impossible under the
  canonical handler's fixed arguments; retain static typing without adding
  runtime branches.
- Keep server-side legacy route coverage and canonical route contract tests.

## Rollout

1. Deploy the Tracker containing both canonical and legacy routes.
2. Release the canonical-only CLI and SDK.
3. Migrate remaining external consumers to `/runs`.
4. Remove legacy Tracker routes and serialized fields only after their consumers
   have migrated.

## Verification

- Canonical CLI and SDK tests assert only `/runs` requests.
- Legacy Tracker route tests continue to pass unchanged.
- Canonical Tracker contract, SDK package, CLI, Tracker, lint, and type checks
  pass.
- Fresh CLI and SDK artifacts pass smoke tests.

## Rollback

Rolling back the CLI or SDK remains safe because the Tracker continues serving
the legacy API. Rolling back the Tracker must not happen after canonical-only
clients are released unless the clients are rolled back first.
