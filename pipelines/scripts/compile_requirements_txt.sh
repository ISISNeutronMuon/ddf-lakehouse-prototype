#!/bin/bash
# Use uv to compile any requirements.in files to requirements.txt files in the
# same directory. The find root is the current working directory
REQUIREMENTS_IN=requirements.in
REQUIREMENTS_TXT=requirements.txt
COMPILE_CMD="uv pip compile"

for local_requirements_in in $(find . -name $REQUIREMENTS_IN); do
  echo "Compiling $local_requirements_in -> $local_requirements_txt"
  direc=$(dirname $local_requirements_in)
  pushd $direc
  $COMPILE_CMD $REQUIREMENTS_IN > $REQUIREMENTS_TXT
  popd
done
