#!/usr/bin/env bash

set -eu

cd "$(dirname "$0")"  # Always run this script with the git repository root as the current working directory

if ! [[ $(poetry check | grep "All set!") ]];
then
  poetry check || true  # Print poetry error
  echo 'ERROR: "poetry check" failed, please make sure "pyproject.toml" project configuration is valid.';
  exit 1
fi

EXPECTED_PY_VERSION="Python 3.8.1"
ACTUAL_PY_VERSION="$(python -V)"

if [ "$EXPECTED_PY_VERSION" != "$ACTUAL_PY_VERSION" ];
then
  echo "ERROR: Inconsistent Python version detected. Expected '$EXPECTED_PY_VERSION', but found '$ACTUAL_PY_VERSION'"
  echo ""
  echo "Examine the output of 'poetry env info' for further information:"
  poetry env info
  exit 1
fi

./setup_owforecasting.sh
poetry install

if ! python -c 'import h2o';
then
  echo "ERROR: Failed to import dependency in poetry environment."
  exit 1
fi

echo "Setup finished successfully."
