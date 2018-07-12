#!/usr/bin/env bash

# Version of go-swagger to use.
version=0.12.0

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger

if [ ! -f $SWAGGER ]; then
  curl -o $SWAGGER -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_$(echo `uname`|tr '[:upper:]' '[:lower:]')_amd64
  chmod +x $SWAGGER
fi

(cd $DIR/..; $SWAGGER generate server --name=weaviate --spec=openapi-specs/schema.json --default-scheme=https)

# Now add the header to the generated code too.
$DIR/add_header.sh
