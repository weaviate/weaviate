#!/usr/bin/env bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.25.0

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

GOARCH=$(go env GOARCH)
GOOS=$(go env GOOS)
if [ ! -f $SWAGGER ]; then
  if [ GOOS = "linux" ]; then
    curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_$GOARCH
  else
    curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_amd64
  fi
  chmod +x $SWAGGER
fi

# Always install goimports to ensure that all parties use the same version
go install golang.org/x/tools/cmd/goimports@v0.1.12

# Explictly get yamplc package
(go get -u github.com/go-openapi/runtime/yamlpc)

# Remove old stuff.
(cd $DIR/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd $DIR/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json)
(cd $DIR/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
(cd $DIR/..; GO111MODULE=on go generate ./deprecations)

echo Now add the header to the generated code too.
(cd $DIR/..; GO111MODULE=on go run ./tools/license_headers/main.go)

(cd $DIR/..; goimports -w . )

# echo Add licenses to file.
# $DIR/create-license-dependency-file.sh

echo Success
