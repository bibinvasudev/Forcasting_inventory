#!/usr/bin/env bash

set -e

cd "$(dirname "$0")/.." # Always run this script with the git repository root as the current working directory

# Not all notebooks should be auto-cleaned, only the following
NOTEBOOKS=(
  'FC_Account_Template.ipynb'
)

# Sometimes we need to initialize pyenv again, e.g. if pre-commit is triggered by IntelliJ
if (! jupyter -h &> /dev/null); then
  eval "$(pyenv init -)"
  eval "$(pyenv virtualenv-init -)"
  PYENV_VIRTUALENV_DISABLE_PROMPT=1 pyenv activate ow-fp
fi

for notebook in "${NOTEBOOKS[@]}"; do
  jupyter nbconvert --to notebook --ClearOutputPreprocessor.enabled=True --inplace "notebooks/${notebook}"
done
