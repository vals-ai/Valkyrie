#!/bin/bash

# Fail fast
set -euo pipefail

# Ensure curl exists on minimal base images (e.g. python:slim)
if ! command -v curl >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update
        apt-get install -y --no-install-recommends curl ca-certificates
        rm -rf /var/lib/apt/lists/*
    else
        echo "curl is required but apt-get is unavailable to install it."
        exit 127
    fi
fi

# Install Claude Code CLI
curl -fsSL https://claude.ai/install.sh | bash

# Create logs directory
mkdir -p /logs

# Make claude available system-wide
ln -sf "$HOME/.local/bin/claude" /usr/local/bin/claude
