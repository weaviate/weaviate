#!/bin/bash
set -eou pipefail

# run from Weaviate Root!
#
# This script calls goreleaser and signs + notarize the binaries for macos. Thus needed to export few ENV variables
# to be used in .goreleaser.yaml in the root of the repository.

GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BUILD_USER="$(whoami)@$(hostname)"
BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

extra_args=

if [[ "$WEAVIATE_VERSION" == *"-"* ]]; then
    extra_args+=("--snapshot")

GIT_REVISION="$GIT_REVISION" GIT_BRANCH="$GIT_BRANCH" BUILD_USER="$BUILD_USER" BUILD_DATE="$BUILD_USER" goreleaser build --clean "$extra_args"

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" dist/weaviate_darwin_all/weaviate

DARWIN_DIST="dist/weaviate-${VERSION}-darwin-all.zip"
zip -j "$DARWIN_DIST" dist/weaviate_darwin_all/weaviate LICENSE README.md

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" "$DARWIN_DIST"

xcrun notarytool submit "$DARWIN_DIST" --keychain-profile "AC_PASSWORD" --wait


# add checksums
shasum -a 256 "$DARWIN_DIST" | cut -d ' ' -f 1 > "${DARWIN_DIST}.sha256"
md5 "$DARWIN_DIST" | cut -d ' ' -f 4 > "${DARWIN_DIST}.md5"
