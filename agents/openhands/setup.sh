#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/opt/ms-playwright}"
PREBUILT_VENV="${OPENHANDS_PREBUILT_VENV:-/opt/openhands-venv}"
OPENHANDS_REPO_URL="${OPENHANDS_REPO_URL:-https://github.com/vals-ai/OpenHands.git}"
OPENHANDS_REPO_REF="${OPENHANDS_REPO_REF:-b655e25042ab6c7cc736475919a58659d9bd6b77}"
OPENHANDS_SRC_DIR="${OPENHANDS_SRC_DIR:-/tmp/openhands-src}"

cd /bundle/openhands
mkdir -p /logs/openhands

has_compatible_openhands_runtime() {
    local python_bin="$1"
    "${python_bin}" - <<'PY' >/dev/null 2>&1
import model_library  # noqa: F401
import openhands.llm.llm  # noqa: F401
PY
}

if [ -x "${PREBUILT_VENV}/bin/python" ] && has_compatible_openhands_runtime "${PREBUILT_VENV}/bin/python"; then
    rm -rf .venv
    ln -s "${PREBUILT_VENV}" .venv
    echo "Using prebuilt OpenHands runtime from ${PREBUILT_VENV}"
    exit 0
fi

if [ -x "${PREBUILT_VENV}/bin/python" ]; then
    echo "Prebuilt OpenHands runtime at ${PREBUILT_VENV} is missing the Vals fork or model-proxy; falling back to local install."
fi

apt-get update
apt-get install -y --no-install-recommends curl git build-essential ca-certificates tmux
rm -rf /var/lib/apt/lists/*

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
source "$HOME/.local/bin/env" 2>/dev/null || true

uv venv .venv
source .venv/bin/activate

rm -rf "${OPENHANDS_SRC_DIR}"
git clone "${OPENHANDS_REPO_URL}" "${OPENHANDS_SRC_DIR}"
git -C "${OPENHANDS_SRC_DIR}" checkout "${OPENHANDS_REPO_REF}"

uv pip install -e "${OPENHANDS_SRC_DIR}/model-proxy"
uv pip install -e "${OPENHANDS_SRC_DIR}"
uv pip install "protobuf==6.30.0"

if ! has_compatible_openhands_runtime "$(pwd)/.venv/bin/python"; then
    echo "Installed OpenHands runtime is missing required Vals fork dependencies."
    exit 1
fi

# OpenHands browser mode needs Playwright's system libraries in addition to the browser binary.
python -m playwright install --with-deps chromium
