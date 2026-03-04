#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends curl git build-essential ca-certificates tmux
rm -rf /var/lib/apt/lists/*

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
source "$HOME/.local/bin/env" 2>/dev/null || true

cd /bundle/openhands
uv venv .venv
source .venv/bin/activate
uv pip install "openhands-ai" "toml"

mkdir -p /logs/openhands
