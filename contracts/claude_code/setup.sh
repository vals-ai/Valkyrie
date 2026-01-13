#!/bin/bash

set -euo pipefail

echo "Installing Claude Code..."
curl -fsSL https://claude.ai/install.sh | bash

echo "Setup complete!"
echo "Claude Code installed at: $(which claude || echo 'Not found in PATH')"
