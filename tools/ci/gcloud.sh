#!/usr/bin/env bash
set -euo pipefail

# change current working directory to ./tools/ci
CWD=./tools/ci
cd $CWD

# Latest packages can be found here: https://cloud.google.com/sdk/docs/install
GOOGLE_CLOUD_CLI_PKG=google-cloud-cli-482.0.0-linux-x86_64.tar.gz

# install gcloud
sudo apt-get install apt-transport-https ca-certificates gnupg curl -y
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GOOGLE_CLOUD_CLI_PKG
tar -xf $GOOGLE_CLOUD_CLI_PKG

./google-cloud-sdk/install.sh -q
cd ./google-cloud-sdk/bin/ && ln -s $PWD/gcloud /usr/local/bin/gcloud && cd -

gcloud --version

# decode key file
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_PASSPHRASE" --output sa.json sa.json.gpg

# configure project
gcloud auth activate-service-account --key-file sa.json
gcloud config set project $GCP_PROJECT
