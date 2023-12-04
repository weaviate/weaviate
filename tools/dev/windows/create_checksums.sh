#!/usr/bin/env bash

set -eou pipefail

VERSION=${1-}

function echo_green() {
  green='\033[0;32m'
  nc='\033[0m'
  echo -e "${green}${*}${nc}"
}

function echo_red() {
  red='\033[0;31m'
  nc='\033[0m' 
  echo -e "${red}${*}${nc}"
}

function echo_purp_bold() {
    purp='\033[1;35m'
    nc='\033[0m'
    echo -e "${purp}${*}${nc}"
}

function check_binary_files_existance() {
  FILES=("binaries-windows-amd64.zip" "binaries-windows-arm64.zip")
  for f in "${FILES[@]}"
  do
    if [ ! -f "$f" ]; then
      echo_red "File $f does not exist."
      echo_red "This script expects binaries-windows-amd64.zip and binaries-windows-arm64.zip files to be present."
      echo_red "Before running it please first download windows binaries files from github CI pipeline"
      echo_red "so that this script can properly modify the names of the files and generate checksums."
      exit 1
    fi
  done
}

function calculate_checksums() {
  shasum -a 256 $1 | cut -d ' ' -f 1 > $1.sha256
  md5 $1 | cut -d ' ' -f 4 > $1.md5
}

function prepare_binary() {
  arch=$1
  arch_dir=$arch
  mkdir $arch_dir && mv binaries-windows-$arch.zip $arch_dir && cd $arch_dir
  
  unzip binaries-windows-$arch.zip
  cp ../../../../README.md .
  cp ../../../../LICENSE .
  zip weaviate-$VERSION-windows-$arch.zip weaviate.exe LICENSE README.md
  calculate_checksums weaviate-$VERSION-windows-$arch.zip

  rm weaviate.exe
  mv weaviate* ../
  cd .. && rm -fr $arch_dir
}

function main() {
  if test -z "$VERSION"; then
    echo "Missing version parameter. Usage: $0 VERSION"
    exit 1
  fi

  if case $VERSION in v*) false;; esac; then
    VERSION="v$VERSION"
  fi

  check_binary_files_existance

  OUT_DIR="dist"
  if [ -d $OUT_DIR ]; then
    rm -fr $OUT_DIR
  fi

  ARCHITECTURES=("amd64" "arm64")
  for arch in "${ARCHITECTURES[@]}"
  do
    prepare_binary $arch
  done

  mkdir $OUT_DIR && mv weaviate* $OUT_DIR

  echo_purp_bold "${VERSION} windows artifacts available here: $(pwd)/$OUT_DIR"
}

main "$@"
