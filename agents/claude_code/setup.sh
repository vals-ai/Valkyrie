#!/bin/bash

# Fail fast
set -euo pipefail

# Install Claude Code CLI
curl -fsSL https://claude.ai/install.sh | bash

# Create logs directory
mkdir -p /logs

# Add ~/.local/bin to PATH for future shells
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
