#!/bin/bash
set -eou pipefail

# run from Weaviate Root!
#
# This script calls goreleaser and signs + notarize the binaries for macos

export GIT_HASH=$(git rev-parse --short HEAD)
VERSION="v$(jq -r '.info.version' openapi-specs/schema.json)"

goreleaser build --clean --snapshot # add --snapshot to this commandline to build from non-tagged commit or with unclean directory

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" dist/weaviate_darwin_all/weaviate

DARWIN_DIST="dist/weaviate-${VERSION}-darwin-all.zip"
zip "$DARWIN_DIST" dist/weaviate_darwin_all/weaviate LICENSE README.md

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" "$DARWIN_DIST"

xcrun notarytool submit "$DARWIN_DIST" --keychain-profile "AC_PASSWORD" --wait


# add checksums
shasum -a 256 "$DARWIN_DIST" | cut -d ' ' -f 1 > "${DARWIN_DIST}.sha256"
md5 "$DARWIN_DIST" | cut -d ' ' -f 4 > "${DARWIN_DIST}.md5"