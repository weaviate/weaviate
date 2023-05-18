#!/bin/bash

set -eo pipefail

GIT_HASH=$(git rev-parse --short HEAD)
VERSION="v$(jq -r '.info.version' openapi-specs/schema.json)"

function main() {
  cd ./cmd/weaviate-server

  echo_green "Building linux/arm64 binary..."
  GOOS=linux GOARCH=arm64 go build -o build_artifacts/weaviate_linux_arm64 -ldflags "-w -extldflags \"-static\" -X github.com/weaviate/weaviate/usecases/config.GitHash='${GIT_HASH}'"
  step_complete

  echo_green "Building linux/amd64 binary..."
  GOAMD64=v1 GOOS=linux GOARCH=amd64 go build -o build_artifacts/weaviate_linux_amd64 -ldflags "-w -extldflags \"-static\" -X github.com/weaviate/weaviate/usecases/config.GitHash='${GIT_HASH}'"
  step_complete

  cd build_artifacts

  cp ../../../README.md .
  cp ../../../LICENSE .

  echo_green "Packing linux/arm64 distribution..."
  LINUX_ARM_DIST="weaviate-${VERSION}-linux-arm64.tar.gz"
  tar cvfz "$LINUX_ARM_DIST" weaviate_linux_arm64 LICENSE README.md
  step_complete

  echo_green "Calculating linux/arm64 checksums..."
  shasum -a 256 "$LINUX_ARM_DIST" | cut -d ' ' -f 1 > "${LINUX_ARM_DIST}.sha256"
  md5 "$LINUX_ARM_DIST" | cut -d ' ' -f 4 > "${LINUX_ARM_DIST}.md5"
  step_complete

  echo_green "Packing linux/amd64 distribution..."
  LINUX_AMD_DIST="weaviate-${VERSION}-linux-amd.tar.gz"
  tar cvfz "$LINUX_AMD_DIST" weaviate_linux_amd64 LICENSE README.md
  step_complete

  echo_green "Calculating linux/amd64 checksums..."
  shasum -a 256 "$LINUX_AMD_DIST" | cut -d ' ' -f 1 > "${LINUX_AMD_DIST}.sha256"
  md5 "$LINUX_AMD_DIST" | cut -d ' ' -f 4 > "${LINUX_AMD_DIST}.md5"
  step_complete

  echo_green "Cleaning up workspace..."
  rm -v weaviate_linux_arm64
  rm -v weaviate_linux_amd64
  rm -v LICENSE
  rm -v README.md
  step_complete

  echo_purp_bold "${VERSION} artifacts available here: $(pwd)"
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

main "$@"
