#!/bin/bash

set -e
set -u
set -o pipefail

# See https://confluence.atlassian.com/bitbucket/deploy-build-artifacts-to-bitbucket-downloads-872124574.html

BITBUCKET_REPO_OWNER=${BITBUCKET_REPO_OWNER:-"oliverwymantechssg"}
BITBUCKET_REPO_SLUG=${BITBUCKET_REPO_SLUG:-"pap007"}
HOW_TO_CREATE_APP_PASSWORD_AND_AUTH_STRING_URL="https://confluence.atlassian.com/bitbucket/deploy-build-artifacts-to-bitbucket-downloads-872124574.html"
echo "BITBUCKET_REPO_OWNER/BITBUCKET_REPO_SLUG=${BITBUCKET_REPO_OWNER}/${BITBUCKET_REPO_SLUG}"

if [[ -z "${BB_AUTH_STRING:-}" ]]; then ## BB_AUTH_STRING contains "BitBucketUserName:AppPassword"
   echo "BB_AUTH_STRING has NOT been set (it is empty), see ${HOW_TO_CREATE_APP_PASSWORD_AND_AUTH_STRING_URL} to create it.";
   exit -1
else
   echo "BB_AUTH_STRING has been set (it is NOT empty).";
fi

FOUND_USER_NAME=$(echo "${BB_AUTH_STRING}" | grep "\:" || true)
if [[ -z "${FOUND_USER_NAME}" ]]; then
    echo "Environment variable BB_AUTH_STRING does NOT contain a username (the user owning the AppPassword), should be of the form 'username:AppPassword', see ${HOW_TO_CREATE_APP_PASSWORD_AND_AUTH_STRING_URL} to see how it is created."
else
    echo "Environment variable BB_AUTH_STRING contains a username, as expected."
fi

DOWNLOAD_ARTIFACT="${1:-}"
TARGET_FOLDER="${2:-"."}"
DOWNLOAD_URL="https://api.bitbucket.org/2.0/repositories/${BITBUCKET_REPO_OWNER}/${BITBUCKET_REPO_SLUG}/downloads/${DOWNLOAD_ARTIFACT}"
echo ""
echo "Attempting to download ${DOWNLOAD_ARTIFACT} from ${DOWNLOAD_URL}"
curl -O -L -J                 \
     -u "${BB_AUTH_STRING}"   \
     ${DOWNLOAD_URL}          \
     -o "$(pwd)/${DOWNLOAD_ARTIFACT}"
echo "Finished downloading ${DOWNLOAD_ARTIFACT} from the BitBucket Downloads area"

if [[ ! -s "${DOWNLOAD_ARTIFACT}" ]]; then
   echo "${DOWNLOAD_ARTIFACT} not found, might have failed trying to download"
   exit -1
fi

echo "Moving the downloaded archive "${DOWNLOAD_ARTIFACT}" into the '${TARGET_FOLDER}' folder"
mv "${DOWNLOAD_ARTIFACT}" "${TARGET_FOLDER}"

ls -lash "${TARGET_FOLDER}/${DOWNLOAD_ARTIFACT}"
echo "Artifact ${DOWNLOAD_ARTIFACT} found in ${TARGET_FOLDER}."
