#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y curl git build-essential

# Remove stale build artifacts that may have been bundled
rm -rf model_proxy/build model_proxy/dist model_proxy/*.egg-info

# Install model_proxy; .git is stripped during bundling so fake the version
export SETUPTOOLS_SCM_PRETEND_VERSION_FOR_MODEL_LIBRARY=0.0.0
pip install --no-cache-dir setuptools setuptools-scm
pip install --no-cache-dir -e model_proxy/

# Create logs directory
mkdir -p /logs
