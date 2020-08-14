#!/usr/bin/env bash

set -e

###
# Script to validate current git user.email while committing
# - email is rejected if not present in whitelisted email addresses
#
# Adopted from https://gist.github.com/raido/afdef127f0396438093976a99491b0a5
###

if [[ -z "${BITBUCKET_COMMIT}" ]]; then
    # We are in pre-commit hook, retrieve author information as Git sees it
    AUTHORINFO=$(git var GIT_AUTHOR_IDENT) || exit 1
    EMAIL=$(printf '%s\n\n' "${AUTHORINFO}" | sed -n 's/^.* <\(.*\)> .*$/\1/p')
else
    # We are in bitbucket pipeline and $BITBUCKET_COMMIT is the commit that kicked off the pipeline
    EMAIL=$(git log --format='%ae' -n 1 $BITBUCKET_COMMIT) || exit 1
fi

# Validate emails
if [[ ! ${EMAIL} =~ ^(patrick.harbock@oliverwyman.com|ivan.hanzlicek@oliverwyman.com|pascal.wasser@oliverwyman.com|alejandro.parejagonzalez@oliverwyman.com|fabian.jost@oliverwyman.com|nicolas.greiner@oliverwyman.com|paul.bischoff@oliverwyman.com)$ ]];
then
    echo "ERROR: You are not allowed to commit with your current email: <${EMAIL}>."
    echo "Verify user.email settings of your git config"
    exit 1
fi

echo "All good!"
