#!/bin/bash

# Fail fast
set -euo pipefail

# Install Claude Code CLI
curl -fsSL https://claude.ai/install.sh | bash

# Create logs directory
mkdir -p /logs

# Make claude available system-wide
ln -sf "$HOME/.local/bin/claude" /usr/local/bin/claude
