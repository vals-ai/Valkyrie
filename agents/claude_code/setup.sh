#!/bin/bash

# Fail fast
set -euo pipefail

# Install curl if not already available
if ! command -v curl &> /dev/null; then
  apt-get update && apt-get install -y curl || yum install -y curl || apk add --no-cache curl
fi

# Install Claude Code CLI
curl -fsSL https://claude.ai/install.sh | bash

# Create logs directory
mkdir -p /logs

# Make claude available system-wide
ln -sf "$HOME/.local/bin/claude" /usr/local/bin/claude
