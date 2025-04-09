#!/bin/bash
# Assume uv is available.
# Create a local virtual environment, install build requirements and build
# into a `build` subdirectory of the project root
set -euo pipefail

if [ $# = 0 ]; then
  echo "Usage: $0 <project_root> <build_dir> [...other arguments passed to mkdocs build]"
  exit 1
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "Ignoring \`VIRTUAL_ENV=$VIRTUAL_ENV\`. Falling back on uv-discovered environments"
  unset VIRTUAL_ENV
fi

# Constants
VENV_DIR=.venv
DEFAULT_BUILD_ARGS="--clean"

# Argument handling
project_root=$1
build_dir=$2
shift 2
user_build_args=$*

# Create and configure environment
pushd $project_root
test -d $VENV_DIR || uv venv $VENV_DIR
uv pip install -r requirements.txt

# Build
uv run mkdocs build $DEFAULT_BUILD_ARGS --site-dir $build_dir
popd
