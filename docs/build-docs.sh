#!/bin/bash
# Assume uv is available.
# Create a local virtual environment, install build requirements and build
# into a `build` subdirectory of the project root
set -euo pipefail

if [ $# = 0 ]; then
  echo "Usage: $0 <project_root> [...other arguments passed to sphinx-build]"
  exit 1
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "Ignoring \`VIRTUAL_ENV=$VIRTUAL_ENV\`. Falling back on uv-discovered environments"
  unset VIRTUAL_ENV
fi

# Constants
VENV_DIR=.venv
BUILD_DIR=build
DEFAULT_BUILD_ARGS="--jobs auto"

# Argument handling
project_root=$1
shift
user_build_args=$*

# Create and configure environment
pushd $project_root
test -d $VENV_DIR || uv venv $VENV_DIR
uv pip install -r requirements.txt

# Build
uv run sphinx-build $DEFAULT_BUILD_ARGS $user_build_args source $BUILD_DIR
popd
