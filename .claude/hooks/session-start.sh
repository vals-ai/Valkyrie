#!/bin/bash
# SessionStart hook for Claude Code on the web: installs dependencies, the
# valkyrie CLI, and writes the CLI config from environment secrets.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

# Python deps (same as `make install`, minus the cache clean so cached state is reused).
if [ ! -x .venv/bin/python ]; then
  uv venv --python 3.12
fi
uv sync --dev

# Editable global `valkyrie` / `valk` executables.
if ! uv tool list 2>/dev/null | grep -q '^valkyrie '; then
  uv tool install -e .
fi
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$CLAUDE_ENV_FILE"
fi
export PATH="$HOME/.local/bin:$PATH"

# CLI config from environment secrets. Never overwrites an existing config.
#   Hosted:      VALKYRIE_API_KEY (+ optional VALKYRIE_HOSTED_ENV=bench|prod, default bench)
#   Self-hosted: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET
# `config init` sources these from the environment; the piped answers only pick the
# mode and accept defaults for LOG_GROUP / LOG_RETENTION_POLICY.
config_path="${VALKYRIE_CONFIG_PATH:-$HOME/.config/valkyrie/valkyrie.yaml}"
if [ -f "$config_path" ]; then
  echo "Valkyrie config already present at $config_path" >&2
elif [ -n "${VALKYRIE_API_KEY:-}" ]; then
  if ! printf 'hosted\n%s\n\n\n\n\n\n\n' "${VALKYRIE_HOSTED_ENV:-bench}" | valkyrie config init >/dev/null 2>&1; then
    echo "WARNING: hosted 'valkyrie config init' failed (is the tracker host allowed by the network policy?)" >&2
  fi
elif [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] \
  && [ -n "${AWS_DEFAULT_REGION:-}" ] && [ -n "${S3_BUCKET:-}" ]; then
  if ! printf 'self-hosted\n\n\n\n\n\n\n' | valkyrie config init >/dev/null 2>&1; then
    echo "WARNING: self-hosted 'valkyrie config init' failed" >&2
  fi
else
  echo "Skipping valkyrie config: set VALKYRIE_API_KEY (hosted) or AWS_* + S3_BUCKET (self-hosted)" >&2
fi
