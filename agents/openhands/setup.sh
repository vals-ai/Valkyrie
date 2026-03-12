#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/opt/ms-playwright}"
PREBUILT_VENV="${OPENHANDS_PREBUILT_VENV:-/opt/openhands-venv}"
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

if [ ! -d "${OPENHANDS_SRC_DIR}" ] || [ ! -f "${OPENHANDS_SRC_DIR}/pyproject.toml" ] || [ ! -d "${OPENHANDS_SRC_DIR}/model-proxy" ]; then
    if [ -x "${PREBUILT_VENV}/bin/python" ]; then
        echo "Prebuilt OpenHands runtime at ${PREBUILT_VENV} is missing the Vals fork or model-proxy."
    else
        echo "Prebuilt OpenHands runtime not found at ${PREBUILT_VENV}."
    fi
    echo "Provide a compatible prebuilt runtime in the sandbox snapshot or mount local OpenHands sources at ${OPENHANDS_SRC_DIR}."
    exit 1
fi

apt-get update
apt-get install -y --no-install-recommends curl git build-essential ca-certificates tmux
rm -rf /var/lib/apt/lists/*

# Install uv
curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh
source "$HOME/.local/bin/env" 2>/dev/null || true

uv venv .venv
source .venv/bin/activate

uv pip install -e "${OPENHANDS_SRC_DIR}/model-proxy"
uv pip install -e "${OPENHANDS_SRC_DIR}"
uv pip install "protobuf==6.30.0"

if ! has_compatible_openhands_runtime "$(pwd)/.venv/bin/python"; then
    echo "Installed OpenHands runtime is missing required Vals fork dependencies."
    exit 1
fi

# OpenHands browser mode needs Playwright's system libraries in addition to the browser binary.
python -m playwright install --with-deps chromium
