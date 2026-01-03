#!/bin/bash
# Setup script for claude_code agent
# Installs Claude Code CLI and sets up the environment

set -euo pipefail

# echo "Installing dependencies..."
# apt update && apt install -y curl vim

echo "Installing Claude Code..."
curl -fsSL https://claude.ai/install.sh | bash

# Add Claude Code to PATH
export PATH=$HOME/.local/bin:$PATH

echo "Setup complete!"
echo "Claude Code installed at: $(which claude || echo 'Not found in PATH')"
