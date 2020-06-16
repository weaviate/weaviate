#!/usr/bin/env bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.24.0

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

if [ ! -f $SWAGGER ]; then
  curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_amd64
  chmod +x $SWAGGER
fi

if ! hash goimports >/dev/null 2>&1; then
  go get golang.org/x/tools/cmd/goimports
fi

# Explictly get yamplc package
(go get -u github.com/go-openapi/runtime/yamlpc)

# Remove old stuff.
(cd $DIR/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd $DIR/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=foobar)
(cd $DIR/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
(cd $DIR/..; go generate ./deprecations)

echo Now add the header to the generated code too.
(cd $DIR/..; GO111MODULE=on go run ./tools/license_headers/main.go)

(cd $DIR/..; goimports -w . )

# echo Add licenses to file.
# $DIR/create-license-dependency-file.sh
