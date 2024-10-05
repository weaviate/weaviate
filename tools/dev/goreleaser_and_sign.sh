#!/bin/bash
set -eou pipefail

# run from Weaviate Root!
#
# This script calls goreleaser and signs + notarize the binaries for macos. Thus needed to export few ENV variables
# to be used in .goreleaser.yaml in the root of the repository.

export WEAVIATE_GIT_REVISION=$(git rev-parse --short HEAD)
export WEAVIATE_GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

# detect if commit is tagged or not (format is "vA.B.Z" with tag and "vA.B.Z-commit" without tag)
export WEAVIATE_VERSION="$(git describe --tag)"
export WEAVIATE_BUILD_USER="$(whoami)@$(hostname)"
export WEAVIATE_BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"


if [[ "$WEAVIATE_VERSION" == *"-"* ]]; then
  goreleaser build --clean --snapshot
else
  goreleaser build --clean
fi

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" dist/weaviate_darwin_all/weaviate

DARWIN_DIST="dist/weaviate-${VERSION}-darwin-all.zip"
zip -j "$DARWIN_DIST" dist/weaviate_darwin_all/weaviate LICENSE README.md

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" "$DARWIN_DIST"

xcrun notarytool submit "$DARWIN_DIST" --keychain-profile "AC_PASSWORD" --wait


# add checksums
shasum -a 256 "$DARWIN_DIST" | cut -d ' ' -f 1 > "${DARWIN_DIST}.sha256"
md5 "$DARWIN_DIST" | cut -d ' ' -f 4 > "${DARWIN_DIST}.md5"
