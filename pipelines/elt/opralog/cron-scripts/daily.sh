#!/usr/bin/env bash

# Script configuration
set -x

# Variables required for this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ELT_SCRIPT=elt_opralog.py
ELT_LOG_LEVEL=DEBUG
ELT_ON_PIPELINE_FAILURE=log_and_continue

# Variables required by the pipeline script
export PIPELINE_ENV=prod

# Main script
pushd $SCRIPT_DIR/..
uv run $ELT_SCRIPT --log-level $ELT_LOG_LEVEL --on-pipeline-step-failure $ELT_ON_PIPELINE_FAILURE
popd
