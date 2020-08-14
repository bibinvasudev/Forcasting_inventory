#!/usr/bin/env bash

set -eu

cd "$(dirname "$0")"  # Always run this script with the git repository root as the current working directory

if ! ssh-add -L &>/dev/null || [ "$(ssh-add -L | wc -l | awk '{print $1}')" != '1' ];
then
  echo 'INFO: Building the Dockerfile requires your ssh-agent to access private bitbucket repositories.'
  echo 'ERROR: "ssh-add -L" should show exactly one ssh-key added, please add the matching key and remove others'
  echo 'INFO: Run "ssh-add ~/.ssh/id_rsa"'
  exit 1
fi

# BuildKit is required to enable new SSH support:
# https://docs.docker.com/develop/develop-images/build_enhancements/#using-ssh-to-access-private-data-in-builds
export DOCKER_BUILDKIT=1

if [[ "$OSTYPE" == "msys" ]]; then
  # For git bash we need the absolute path to the ssh-key
  KEY_PATH=`ssh-add -L | cut -d ' ' -f3`
  docker build --ssh default=$KEY_PATH . --tag ow-forecasting-platform
else
  docker build --ssh default . --tag ow-forecasting-platform
fi
