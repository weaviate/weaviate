#!/usr/bin/env bash

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
(go get github.com/go-openapi/runtime/yamlpc@v0.29.2)

# Generate from a copy of the spec with documentation-only entries removed.
# Entries marked `"x-doc-only": true` stay in openapi-specs/schema.json for the
# docs site but are excluded from code generation — they document the REST
# query/aggregate endpoints, which are served by custom middleware over the gRPC
# pipeline rather than by generated handlers.
CODEGEN_DIR="$(mktemp -d)"
trap 'rm -rf "$CODEGEN_DIR"' EXIT
CODEGEN_SPEC="$CODEGEN_DIR/schema.json"
(cd "$DIR"/..; GO111MODULE=on go run ./tools/swagger_strip_doc_only openapi-specs/schema.json "$CODEGEN_SPEC")

# Remove old stuff.
(cd "$DIR"/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd "$DIR"/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec="$CODEGEN_SPEC" -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json)
(cd "$DIR"/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec="$CODEGEN_SPEC" -P models.Principal --default-scheme=https)

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
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*_test.go' -not -path './test/*' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*_test.go' -path './test/*' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))
(cd "$DIR"/..; goimports -w $(find . -type f -name '*_test.go' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))

CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "There are changes in the files that need to be committed:"
  git status -s
fi

echo Success
