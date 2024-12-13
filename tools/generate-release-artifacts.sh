#!/usr/bin/env bash

# NOTES:
# 1. This script is used only in case of emergencies when our CI pipelines fail to generate release binaries.
# 2. This script generates only linux binaries. We use different script for Mac binaries (`tools/dev/goreleaser_and_sign.sh`)

set -eou pipefail

OS=${GOOS:-"linux"}

BUILD_ARTIFACTS_DIR="build_artifacts"
GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

VERSION="v$(jq -r '.info.version' openapi-specs/schema.json)"
VPREFIX="github.com/weaviate/weaviate/usecases/build"

BUILD_TAGS="-X ${VPREFIX}.Branch=${GIT_BRANCH} -X ${VPREFIX}.Revision=${GIT_REVISION} -X ${VPREFIX}.BuildUser=$(whoami)@$(hostname) -X ${VPREFIX}.BuildDate=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

function main() {
  cd ./cmd/weaviate-server

  if [ -d $BUILD_ARTIFACTS_DIR ]; then
    rm -fr $BUILD_ARTIFACTS_DIR
  fi

  build_binary_arm64
  build_binary_amd64

  echo_purp_bold "${VERSION} artifacts available here: $(pwd)/${BUILD_ARTIFACTS_DIR}"
}

function echo_green() {
  green='\033[0;32m'
  nc='\033[0m'
  echo -e "${green}${*}${nc}"
}

function echo_purp_bold() {
    purp='\033[1;35m'
    nc='\033[0m'
    echo -e "${purp}${*}${nc}"
}

function step_complete() {
    echo_green "==> Done!"
}

function build_binary_arm64() {
  build_binary "arm64"
}

function build_binary_amd64() {
  build_binary "amd64"
}

function build_binary() {
  arch=$1
  arch_dir="${BUILD_ARTIFACTS_DIR}/${arch}"

  echo_green "Building linux/${arch} binary..."
  GOOS=linux GOARCH=$arch go build -o $BUILD_ARTIFACTS_DIR/$arch/weaviate -ldflags "-w -extldflags \"-static\" ${BUILD_TAGS}"
  step_complete

  cd $arch_dir

  echo_green "Copy README.md and LICENSE file..."
  cp ../../../../README.md .
  cp ../../../../LICENSE .

  echo_green "Packing linux/${arch} distribution..."
  LINUX_DIST="weaviate-${VERSION}-linux-${arch}.tar.gz"
  tar cvfz "$LINUX_DIST" weaviate LICENSE README.md
  step_complete

  echo_green "Calculating linux/${arch} checksums..."
  shasum -a 256 "$LINUX_DIST" | cut -d ' ' -f 1 > "${LINUX_DIST}.sha256"
  md5 "$LINUX_DIST" | cut -d ' ' -f 4 > "${LINUX_DIST}.md5"
  step_complete

  echo_green "Move linux/${arch} artifacts to ${BUILD_ARTIFACTS_DIR} directory..."
  mv $LINUX_DIST* ../
  step_complete

  echo_green "Clean up ${arch} directory"
  cd ../..
  rm -fr $arch_dir
  step_complete
}

main "$@"
