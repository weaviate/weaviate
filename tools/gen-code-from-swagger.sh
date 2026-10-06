#!/usr/bin/env bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.30.4

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

SERVER_TEMPLATE="$DIR/swagger-templates/server/server.gotmpl"
if ! head -1 "$SERVER_TEMPLATE" | grep -q "go-swagger $version "; then
  echo "$SERVER_TEMPLATE was not copied from go-swagger $version; re-copy it and re-apply the WEAVIATE OVERRIDE block" >&2
  exit 1
fi

GOARCH=$(go env GOARCH)
GOOS=$(go env GOOS)
if [ ! -f "$SWAGGER" ]; then
  if [ "$GOOS" = "linux" ]; then
    curl -o "$SWAGGER" -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_"$(echo `uname`|tr '[:upper:]' '[:lower:]')"_"$GOARCH"
  else
    curl -o "$SWAGGER" -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_"$(echo `uname`|tr '[:upper:]' '[:lower:]')"_amd64
  fi
  chmod +x "$SWAGGER"
fi

# Install golangci-lint if it's not istalled
if ! command -v golangci-lint >/dev/null 2>&1; then
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
fi

# Always install goimports to ensure that all parties use the same version
go install golang.org/x/tools/cmd/goimports@v0.1.12

# Explicitly get yamplc package
(go get github.com/go-openapi/runtime/yamlpc@v0.29.2)

# Remove old stuff.
(cd "$DIR"/..; rm -rf entities/models client adapters/handlers/rest/operations/)

# swagger-templates/server/server.gotmpl is a modified copy of go-swagger's template. When bumping
# version, re-copy it from the new tag and re-apply the WEAVIATE OVERRIDE block.
(cd "$DIR"/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json --template-dir="$DIR/swagger-templates" --allow-template-override)
(cd "$DIR"/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
(cd "$DIR"/..; GO111MODULE=on GOWORK=off go generate ./deprecations)

echo Now add custom UnmarmarshalJSON code to models.Vectors swagger generated file.
(cd "$DIR"/..; GO111MODULE=on go run ./tools/swagger_custom_code/main.go)

echo Now add the header to the generated code too.
(cd "$DIR"/..; GO111MODULE=on go run ./tools/license_headers/main.go)
# goimports and exclude hidden files and proto auto generated files, do this process in steps:
# 1. regular go files (without test files) excluding test folder
# 2. regular go files (without test files) only in test folder
# 3. only *_test.go files

echo Fix imports with goimports
(cd "$DIR"/..; find . -type f -name '*.go' \
  -not -name '*pb.go' \
  -not -path './vendor/*' \
  -not -path './.*/*' \
  -exec goimports -w {} +)

echo Run the code formatter
(cd "$DIR"/..; golangci-lint fmt)

CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "There are changes in the files that need to be committed:"
  git status -s
fi

echo Success
