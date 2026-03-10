#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/opt/ms-playwright}"
PREBUILT_VENV="${OPENHANDS_PREBUILT_VENV:-/opt/openhands-venv}"

cd /bundle/openhands
mkdir -p /logs/openhands

if [ -x "${PREBUILT_VENV}/bin/python" ]; then
    rm -rf .venv
    ln -s "${PREBUILT_VENV}" .venv
    echo "Using prebuilt OpenHands runtime from ${PREBUILT_VENV}"
    exit 0
fi

apt-get update
apt-get install -y --no-install-recommends curl git build-essential ca-certificates tmux
rm -rf /var/lib/apt/lists/*

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
source "$HOME/.local/bin/env" 2>/dev/null || true

uv venv .venv
source .venv/bin/activate
uv pip install "openhands-ai" "toml" "playwright"
# OpenHands browser mode needs Playwright's system libraries in addition to the browser binary.
python -m playwright install --with-deps chromium
