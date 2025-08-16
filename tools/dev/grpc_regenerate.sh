#!/usr/bin/env bash
set -euo pipefail

GEN_DIR=./grpc/generated
OUT_DIR="$GEN_DIR/protocol"

echo "Installing latest gRPC libs..."

if command -v brew >/dev/null 2>&1; then
    brew update && brew upgrade protobuf protolint
fi

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "Generating Go protocol stubs..."

rm -fr $OUT_DIR && mkdir -p $OUT_DIR && cd $GEN_DIR && protoc \
    --proto_path=../proto \
    --go_out=paths=source_relative:protocol \
    --go-grpc_out=paths=source_relative:protocol \
    ../proto/v0/*.proto && protoc \
    --proto_path=../proto \
    --go_out=paths=source_relative:protocol \
    --go-grpc_out=paths=source_relative:protocol \
    ../proto/v1/*.proto

cd - && sed -i '' '/versions:/, /source: .*/d' ./grpc/generated/protocol/**/*.go

go run ./tools/license_headers/main.go

goimports -w $OUT_DIR

# running gofumpt twice is on purpose
# it doesn't work for the first time only after second run the formatting is proper
gofumpt -w $OUT_DIR
gofumpt -w $OUT_DIR

echo "Success"
