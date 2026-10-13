#!/usr/bin/env bash
set -euo pipefail

# change current working directory to ./tools/ci
CWD=./tools/ci
cd $CWD

# install gcloud
sudo apt-get install apt-transport-https ca-certificates gnupg curl jq -y

if command -v gcloud >/dev/null 2>&1; then
  echo "using preinstalled gcloud: $(command -v gcloud)"
else
  echo "fetching latest gcloud version"
  GOOGLE_CLOUD_CLI_VERSION=$(curl -fsSL https://dl.google.com/dl/cloudsdk/channels/rapid/components-2.json | jq -r '.version')
  GOOGLE_CLOUD_CLI_PKG=google-cloud-cli-${GOOGLE_CLOUD_CLI_VERSION}-linux-x86_64.tar.gz

  echo "downloading $GOOGLE_CLOUD_CLI_PKG"
  curl -fsSLO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GOOGLE_CLOUD_CLI_PKG
  tar -xf $GOOGLE_CLOUD_CLI_PKG

  ./google-cloud-sdk/install.sh -q
  cd ./google-cloud-sdk/bin/ && ln -s $PWD/gcloud /usr/local/bin/gcloud && cd -
fi

gcloud --version

# decode key file
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_PASSPHRASE" --output sa.json sa.json.gpg

# configure project
gcloud auth activate-service-account --key-file sa.json
gcloud config set project $GCP_PROJECT
