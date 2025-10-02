#!/usr/bin/env bash

set -eou pipefail

# Version of go-swagger to use.
version=v0.30.4

# Always points to the directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWAGGER=$DIR/swagger-${version}

# We use custom templates in swagger/templates/generator/templates/ (see `--template-dir` below) to override
# generated code (e.g., fixing `handleShutdown` in server.go). Templates are version-specific,
# so if the go-swagger version changes, all custom templates must be updated from the new
# version and modifications reapplied. This check prevents generating broken code with mismatched versions.
TEMPLATE_COMPATIBLE_VERSION="v0.30.4"
if [ "$version" != "$TEMPLATE_COMPATIBLE_VERSION" ]; then
  echo "ERROR: go-swagger version changed!"
  echo "  Current version: $version"
  echo "  Template is compatible with: $TEMPLATE_COMPATIBLE_VERSION"
  echo ""
  echo "Please update the custom template in: swagger/templates/generator/templates/"
  echo "Download compatible gotmpl files from: https://github.com/go-swagger/go-swagger/tree/$version/generator/templates"
  echo "Then reapply all custom changes and update TEMPLATE_COMPATIBLE_VERSION in this script."
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

# Always install goimports to ensure that all parties use the same version
go install golang.org/x/tools/cmd/goimports@v0.1.12

# Explicitly get yamplc package
(go get github.com/go-openapi/runtime/yamlpc@v0.24.2)

# Remove old stuff.
(cd "$DIR"/..; rm -rf entities/models client adapters/handlers/rest/operations/)

(cd "$DIR"/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json --template-dir=swagger/templates/generator/templates)
(cd "$DIR"/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
(cd "$DIR"/..; GO111MODULE=on GOWORK=off go generate ./deprecations)

echo Now add custom UnmarmarshalJSON code to models.Vectors swagger generated file.
(cd "$DIR"/..; GO111MODULE=on go run ./tools/swagger_custom_code/main.go)

echo Now add the header to the generated code too.
(cd "$DIR"/..; GO111MODULE=on go run ./tools/license_headers/main.go)
# goimports and exclude hidden files and proto auto generated files, do this process in steps, first for regular go files, then only for test go files
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*_test.go' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))
(cd "$DIR"/..; goimports -w $(find . -type f -name '*_test.go' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))

CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "There are changes in the files that need to be committed:"
  git status -s
fi

echo Success
