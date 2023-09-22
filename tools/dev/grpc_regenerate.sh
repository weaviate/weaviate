#!/usr/bin/env bash

OUT_DIR=./grpc/generated/protocol

echo "Generating Go protocol stubs..."

rm -fr $OUT_DIR && mkdir -p $OUT_DIR && protoc \
    --go_out=./grpc/generated/protocol \
    --go_opt=module=github.com/weaviate/weaviate/grpc/generated/protocol \
    --go-grpc_out=./grpc/generated/protocol \
    --go-grpc_opt=module=github.com/weaviate/weaviate/grpc/generated/protocol \
    ./grpc/*.proto

go run ./tools/license_headers/main.go

goimports -w $OUT_DIR

# running gofumpt twice is on purpose
# it doesn't work for the first time only after second run the formatting is proper
gofumpt -w $OUT_DIR
gofumpt -w $OUT_DIR

echo "Success"
