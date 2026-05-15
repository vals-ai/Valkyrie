# Development

Local development guide for the Agentic Harness.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager (`brew install uv`)

### Environment

Add inside of `.env`

```env
TRACKER_SERVICE_URL=http://localhost:8000
```

## Installation

### CLI

```bash
make install
```

Creates `.venv` and installs dependencies from `pyproject.toml`.

### Install as a tool globally

```bash
make tool-install
```

Installs `valkyrie` as a standalone executable so you can run it without the `uv run` prefix. Uses editable install so code changes take effect immediately. If not added to your PATH, run `uv tool update-shell`.

### Tracker service

```bash
make tracker-service   # Build and run Docker container
```

The service will be available at `http://localhost:8000`.

## Environment Setup

### CLI (valkyrie config)

The CLI reads credentials from `~/.config/valkyrie/valkyrie.yaml`. Run `valkyrie config init` to create it.

```bash
valkyrie config init
```

### Testing with hosted mode (Descope auth)

Start the tracker with auth enabled:

```bash
AUTH_REQUIRED=true \
DESCOPE_PROJECT_ID=<your-project-id> \
DESCOPE_MANAGEMENT_KEY=<your-management-key> \
make tracker-service
```

`DESCOPE_MANAGEMENT_KEY` is server-side tracker config. It lets the local tracker
resolve the access key's bound user email through Descope's management API when
testing hosted-mode run attribution.

The tracker expects Descope access-key exchange responses to expose the bound user
id through custom claims. For example:

```json
{
  "keyId": "K2abc",
  "sessionToken": {
    "sub": "K2abc",
    "customClaims": {
      "user_id": "U2abc"
    }
  }
}
```

Then configure the CLI for hosted mode:

```bash
valkyrie config init
# Choose "hosted", provide your Descope API key and AWS credentials
```

Without the env vars, the service runs in self-hosted mode (no auth, default org).

## Code Quality

```bash
make style       # ruff format + ruff check --fix
make typecheck   # basedpyright (strict mode)
```

## Versioning

Semantic versioning is used for the prod branch. Below demonstrates what is acceptable to include in the pr title for a deploy. Valkyrie uses [github-tag-action](https://github.com/anothrNick/github-tag-action) to automatically handle release versions as long as the pr title contains a required tag.  

| Tag | Effect | Example |
| --- | --- | --- |
| `#patch` | Patch bump | v0.4.0 -> v0.4.1 |
| `#minor` | Minor bump | v0.4.1 -> v0.5.0 |
| `#major` | Major bump | v0.5.0 -> v1.0.0 |

## Releases

Binary versions are released when commits are tagged:

- **Dev**: Must manually tag a commit to trigger a release
- **Prod**: Automatically tagged and released on push

## Documentation

| Topic | Link |
| --- | --- |
| Lambda integration | [LAMBDA_USAGE.md](LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](CONTRACTS.md) |
| Tracker service | [TRACKER.md](../services/tracker/README.md) |
| Database & migrations | [DATABASE.md](../services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](../infra/README.md) |
