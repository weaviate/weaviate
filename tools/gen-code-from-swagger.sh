#!/bin/bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.30.4

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

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

# Always install goimports to ensure that all parties use the same version
go install golang.org/x/tools/cmd/goimports@v0.1.12

# Explicitly get yamplc package
(go get github.com/go-openapi/runtime/yamlpc@v0.24.2)

# Remove old stuff.
(cd "$DIR"/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd "$DIR"/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json)
(cd "$DIR"/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
(cd "$DIR"/..; GO111MODULE=on go generate ./deprecations)

echo Now add the header to the generated code too.
(cd "$DIR"/..; GO111MODULE=on go run ./tools/license_headers/main.go)
# goimports and exlucde hidden files and proto auto generate files
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*pb.go' -not -path './vendor/*'  -not -path "./.*/*"))

CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "There are changes in the files that need to be committed:"
  git status -s
fi

echo Success
