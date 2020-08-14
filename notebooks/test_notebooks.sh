#!/usr/bin/env bash

set -eux

cd "$(dirname "$0")/.." # Always run this script with the git repository root as the current working directory

# Adjust notebook to Bitbucket H2O setup
sed -i.bak -e 's/localhost:55555/localhost:54321/g' 'notebooks/FC_Account_Template.ipynb'

jupyter nbconvert --execute --stdout --inplace notebooks/FC_Account_Template.ipynb

# Revert changes to not accidentally commit when running locally
sed -i.bak -e 's/localhost:54321/localhost:55555/g' 'notebooks/FC_Account_Template.ipynb'
rm notebooks/*.bak
