#!/usr/bin/env bash

# Script configuration
set -euo pipefail

# Variables required for this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
EXTRACT_AND_LOAD_SCRIPT=extract_and_load.py
LOG_LEVEL=DEBUG
ON_PIPELINE_FAILURE=log_and_continue
REQUIREMENTS_TXT=requirements.txt

# Variables required by the pipeline script
export PIPELINE_ENV=prod

# Create & configure environment
pushd $SCRIPT_DIR/..
test -d .venv || uv venv
uv pip install -r $REQUIREMENTS_TXT

# Execute
uv run $EXTRACT_AND_LOAD_SCRIPT --log-level $LOG_LEVEL --on-pipeline-step-failure $ON_PIPELINE_FAILURE
