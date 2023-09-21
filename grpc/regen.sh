protoc --go_out=./grpc/generated --go_opt=paths=source_relative \
    --go-grpc_out=./grpc/generated --go-grpc_opt=paths=source_relative \
    ./grpc/weaviate.proto ./grpc/base.proto ./grpc/batch.proto ./grpc/search_get.proto

gofumpt -w .