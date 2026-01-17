#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y curl git build-essential tmux

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh

# Source uv so it's available in this session
source "$HOME/.local/bin/env" 2>/dev/null || true

# Install the sweagent Python package
cd submodules/sweagent && uv sync 

# Wrapper script so `sweagent` is on PATH
cat > /usr/local/bin/sweagent << 'WRAPPER'
#!/bin/bash
source /bundle/sweagent/submodules/sweagent/.venv/bin/activate
exec python -m sweagent.run.run "$@"
WRAPPER
chmod +x /usr/local/bin/sweagent
