#!/bin/bash

# Fail fast
set -euo pipefail

# Install Claude Code CLI
curl -fsSL https://claude.ai/install.sh | bash

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh

# Source uv so it's available in this session
source "$HOME/.local/bin/env" 2>/dev/null || true

# Install the claude_code Python package
cd submodules/claude_code && uv sync

# Wrapper script so `claude_code` is on PATH
# TODO: abstract away the internal directory structure of the task environment (i.e. /bundle/claude_code)
cat > /usr/local/bin/claude_code << 'WRAPPER'
#!/bin/bash
source /bundle/claude_code/submodules/claude_code/.venv/bin/activate
exec python /bundle/claude_code/submodules/claude_code/main.py "$@"
WRAPPER
chmod +x /usr/local/bin/claude_code
