#!/usr/bin/env bash

set -eu

cd "$(dirname "$0")/.."  # Change to repository root directory

ZIP_TEMP_DIR='windows_installation/ow-forecasting-zip-temp'

rm -r "$ZIP_TEMP_DIR" &>/dev/null || true

mkdir -p "$ZIP_TEMP_DIR"

git archive --output "$ZIP_TEMP_DIR/forecasting_platform.zip" HEAD forecasting_platform  # Ignore files not in git

if [ -f "owforecasting.zip" ]; then
  # Just take the existing zip, e.g. in remote client environment
  cp "owforecasting.zip" "$ZIP_TEMP_DIR/owforecasting.zip"
else
  # Package the zip fresh, e.g. when running in local development
  python -m zipfile -c "$ZIP_TEMP_DIR/owforecasting.zip" "owforecasting"
fi

if [ -f "expected_results.zip" ]; then
  # Just take the existing zip, e.g. in remote client environment
  cp "expected_results.zip" "$ZIP_TEMP_DIR/expected_results.zip"
else
  # Package the zip fresh, e.g. when running in local development
  python -m zipfile -c "$ZIP_TEMP_DIR/expected_results.zip" "expected_results"
fi

python -m zipfile -c "$ZIP_TEMP_DIR/00 Config.zip" "00 Config"
python -m zipfile -c "$ZIP_TEMP_DIR/01 Raw data.zip" "01 Raw data"

cp pyproject.toml "$ZIP_TEMP_DIR"
cp poetry.lock "$ZIP_TEMP_DIR"

cp windows_installation/*.ps1 "$ZIP_TEMP_DIR"

ZIP_PATH="$(pwd)/windows_installation/ow-forecasting-install.zip"

pushd "$ZIP_TEMP_DIR" > /dev/null
  python -m zipfile -c "${ZIP_PATH}" .
popd > /dev/null

rm -r "$ZIP_TEMP_DIR"  # Cleanup temporary directory

echo "Created installation archive at: ${ZIP_PATH}"
