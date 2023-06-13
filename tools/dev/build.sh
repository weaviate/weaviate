#!/bin/bash
#
# run from weaviate Root

export GIT_HASH=$(git rev-parse --short HEAD)
goreleaser build  --clean --snapshot

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" dist/weaviate_darwin_all/weaviate

zip dist/weaviate_darwin_all.zip dist/weaviate_darwin_all/weaviate

codesign -f -o runtime --timestamp -s "Developer ID Application: Weaviate B.V. (QUZ8SKLS6R)" dist/weaviate_darwin_all.zip

xcrun notarytool submit dist/weaviate_darwin_all.zip --keychain-profile "AC_PASSWORD_PRIVAT" --wait
