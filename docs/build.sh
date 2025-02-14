#!/bin/bash
# Assuming a base python installation, including pip, install
# docs build requirements and build into the given directory.
set -ex

if [ $# != 1 ]; then
  echo "Usage: $0 <build_dir>"
  exit 1
fi

# Constants
THIS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DOCS_SOURCE_DIR=$THIS_DIR/source
DOCS_BUILD_DIR=$1

# Build
pip install .
sphinx-build $DOCS_SOURCE_DIR $DOCS_BUILD_DIR
