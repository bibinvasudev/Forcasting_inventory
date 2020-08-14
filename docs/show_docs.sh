#!/usr/bin/env bash

# Generate documentation and show in browser

set -eu

cd "$(dirname "$0")"

rm -r _stubs || true
make clean # Build fresh documentation, avoid caching issues.

make html

if ls _build/html/_modules | \
  grep -v index.html | \
  grep -v forecasting_platform | \
  grep -v owforecasting | \
  grep -v click | \
  grep -v sqlalchemy; then
    echo "Unexpected _modules included in documentation, please check and update copyright_notice.rst if necessary."
    exit 1
fi

if [[ "$OSTYPE" == "msys" ]]; then
  python -m webbrowser -t "file://$(pwd -W)/_build/html/index.html"
else
  python -m webbrowser -t "file://$(pwd)/_build/html/index.html"
fi
