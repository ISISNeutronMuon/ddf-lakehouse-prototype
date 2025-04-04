#!/bin/bash
# Assume uv is available.
# Create a local virtual environment, install build requirements and build into the given directory.
set -euo pipefail

if [ $# != 2 ]; then
  echo "Usage: $0 <project_root> <build_dir>"
  exit 1
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "Ignoring \`VIRTUAL_ENV=$VIRTUAL_ENV\`. Falling back on uv-discovered environments"
  unset VIRTUAL_ENV
fi

# Constants
PROJECT_ROOT=$1
BUILD_DIR=$(realpath $2)
SOURCE_DIR=$(realpath $PROJECT_ROOT/source)
VENV_DIR=$PROJECT_ROOT/.venv

# Create and configure environment
pushd $PROJECT_ROOT
test -d $VENV_DIR || uv venv $VENV_DIR
uv pip install -r requirements.txt
popd

# Build
uv run sphinx-build $PROJECT_ROOT/source $BUILD_DIR
