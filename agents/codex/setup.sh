#!/bin/bash

# Fail fast
set -euo pipefail

# Install curl if not already available
if ! command -v curl &> /dev/null; then
  apt-get update && apt-get install -y curl || yum install -y curl || apk add --no-cache curl
fi

export DEBIAN_FRONTEND=noninteractive

# Install Node.js via nvm
curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
nvm install 22

# Install Codex CLI
npm i -g @openai/codex

# Make node, npm, and codex available system-wide
ln -sf "$(which node)" /usr/local/bin/node
ln -sf "$(which npm)" /usr/local/bin/npm
ln -sf "$(which codex)" /usr/local/bin/codex

# Set up Codex authentication
if [ -z "$OPENAI_API_KEY" ]; then
    echo "OPENAI_API_KEY is not set"
    exit 1
fi

mkdir -p "$HOME/.codex"
cat <<EOF >"$HOME/.codex/auth.json"
{
  "OPENAI_API_KEY": "${OPENAI_API_KEY}"
}
EOF

# Create logs directory
mkdir -p /logs
