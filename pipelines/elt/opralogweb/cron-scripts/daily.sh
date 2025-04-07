#!/usr/bin/env bash

# Script configuration
set -x

# Variables required for this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
EXTRACT_AND_LOAD_SCRIPT=extract_and_load.py
LOG_LEVEL=DEBUG
ON_PIPELINE_FAILURE=log_and_continue

# Variables required by the pipeline script
export PIPELINE_ENV=prod

# Main script
pushd $SCRIPT_DIR/..
uv run $EXTRACT_AND_LOAD_SCRIPT --log-level $LOG_LEVEL --on-pipeline-step-failure $ON_PIPELINE_FAILURE
popd
