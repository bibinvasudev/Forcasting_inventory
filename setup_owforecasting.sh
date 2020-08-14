#!/usr/bin/env bash

set -eux

cd "$(dirname "$0")"  # Always run this script with the git repository root as the current working directory

rm -rf owforecasting

git clone 'git@bitbucket.org:oliverwymantechssg/ow-forecasting-workbench.git' owforecasting &>/dev/null

cd owforecasting
  git checkout 'e1b30785d39b567cb656a60d1249abc82ae5f5e3' &>/dev/null
  rm -rf .git
cd ..
