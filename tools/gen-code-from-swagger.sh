#!/usr/bin/env bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.21.0

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

if [ ! -f $SWAGGER ]; then
  curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_amd64
  chmod +x $SWAGGER
fi

# Remove old stuff.
(cd $DIR/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd $DIR/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)
(cd $DIR/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Now add the header to the generated code too.
(cd $DIR/..; GO111MODULE=on go run ./tools/license_headers/main.go)

# echo Add licenses to file.
# $DIR/create-license-dependency-file.sh
