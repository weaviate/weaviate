#!/usr/bin/env bash

# Version of go-swagger to use.
version=v0.24.0

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

if [ ! -f $SWAGGER ]; then
  curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_amd64
  chmod +x $SWAGGER
fi

(cd $DIR/..; $SWAGGER generate server --name=weaviate-genesis --spec=openapi-spec.json --default-scheme=https)
(cd $DIR/..; $SWAGGER generate client --spec=openapi-spec.json --default-scheme=https)
