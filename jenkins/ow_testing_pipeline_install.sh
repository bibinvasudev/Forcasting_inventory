#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cd "$(dirname "$0")/.."  # Change to repository root directory

# Custom echo to include timestamps
echo_time() {
    command echo $(date) "$@"
}

# Set variables
ssh_key_path="${PAP02_SSH_KEY_PATH:?Environment variable PAP02_SSH_KEY_PATH must be provided and not empty}"
build_agent=PAP_service_fsc_tst@OWGAWEUW1PAP02

# Download remote files
echo_time "[Installation] Downloading files ..."

mkdir -p "03 Processed data/monthly_process/202002/anonymized_data_dsx/"
mkdir -p Downloads

./bb-pipeline-download-file.sh expected_results_v27.zip Downloads
unzip Downloads/expected_results_v27.zip -d .

./bb-pipeline-download-file.sh DSX_anonymized_input_v2.zip Downloads
unzip "Downloads/DSX_anonymized_input_v2.zip" -d "01 Raw data"

# Create Artifacts
echo_time "[Installation] Checking out owforecasting package ..."
./setup_owforecasting.sh

echo_time "[Installation] Creating artifacts ..."
./windows_installation/create_zip.sh

# Deploy to remote server
echo_time "[Installation] Deploying to remote server ..."

echo_time "[Installation] Remote server configuration ..."
ssh -i "${ssh_key_path}" -o ConnectTimeout=10 ${build_agent} whoami
ssh -i "${ssh_key_path}" -o ConnectTimeout=10 ${build_agent} python -V
ssh -i "${ssh_key_path}" -o ConnectTimeout=10 ${build_agent} java -version

echo_time "[Installation] Copying artifacts ..."
scp -i "${ssh_key_path}" ./windows_installation/install_forecasting_platform.ps1 ${build_agent}:.
scp -i "${ssh_key_path}" ./windows_installation/ow-forecasting-install.zip ${build_agent}:.

echo_time "[Installation] Installing forecasting platform ..."
ssh -i "${ssh_key_path}" ${build_agent} "powershell -File install_forecasting_platform.ps1"

echo_time "[Installation] Finished"
