#!/usr/bin/env bash

set -eu

cd "$(dirname "$0")"  # Always run this script with the git repository root as the current working directory

VENV_NAME='ow-fp'

if pyenv virtualenvs --bare | grep "$VENV_NAME" &>/dev/null;
then
  echo "WARNING: Detected existing virtualenv, do you want to remove it?"
  # Following command will ask for user confirmation:
  pyenv virtualenv-delete "$VENV_NAME"
fi

pyenv virtualenv 3.8.1 "$VENV_NAME"

echo "Setup finished successfully."
echo "Run 'pyenv activate $VENV_NAME' before running ./setup_poetry.sh"
