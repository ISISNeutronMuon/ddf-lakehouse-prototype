#!/bin/bash
# Assume uv is available.
# Install docs build requirements and build into the given directory.
set -eu -o pipefail

if [ $# != 1 ]; then
  echo "Usage: $0 <build_dir>"
  exit 1
fi

if [ -n "$VIRTUAL_ENV" ]; then
  echo "Ignoring \`VIRTUAL_ENV=$VIRTUAL_ENV\`. Falling back on uv-discovered environments"
  unset VIRTUAL_ENV
fi

# Constants
THIS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DOCS_SOURCE_DIR=$THIS_DIR/source
DOCS_BUILD_DIR=$1


# Install package
uv pip install .
# Build docs
sphinx-build $DOCS_SOURCE_DIR $DOCS_BUILD_DIR
