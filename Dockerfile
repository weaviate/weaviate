# Dockerfile for development purposes.
# Read docs/development.md for more information
# vi: ft=dockerfile

###############################################################################
# Base build image
FROM golang:1.22-alpine AS build_base
RUN apk add bash ca-certificates git gcc g++ libc-dev
WORKDIR /go/src/github.com/weaviate/weaviate
ENV GO111MODULE=on
# Populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .
RUN go mod download

###############################################################################
# This image builds the weaviate server
FROM build_base AS server_builder
ARG TARGETARCH
ARG GITHASH="unknown"
ARG DOCKER_IMAGE_TAG="unknown"
ARG EXTRA_BUILD_ARGS=""
COPY . .
RUN GOOS=linux GOARCH=$TARGETARCH go build $EXTRA_BUILD_ARGS \
      -ldflags '-w -extldflags "-static" \
      -X github.com/weaviate/weaviate/usecases/config.GitHash='"$GITHASH"' \
      -X github.com/weaviate/weaviate/usecases/config.ImageTag='"$DOCKER_IMAGE_TAG"'' \
      -o /weaviate-server ./cmd/weaviate-server

###############################################################################
# This creates an image that can be used to fake an api for telemetry acceptance test purposes
FROM build_base AS telemetry_mock_api
COPY . .
ENTRYPOINT ["./tools/dev/telemetry_mock_api.sh"]

###############################################################################
# This image gets grpc health check probe
FROM golang:1.22-alpine AS grpc_health_probe_builder
RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.34
RUN GOBIN=/go/bin && chmod +x ${GOBIN}/grpc-health-probe && mv ${GOBIN}/grpc-health-probe /bin/grpc_health_probe

###############################################################################
# Weaviate (no differentiation between dev/test/prod - 12 factor!)
FROM alpine AS weaviate
ENTRYPOINT ["/bin/weaviate"]
COPY --from=grpc_health_probe_builder /bin/grpc_health_probe /bin/
COPY --from=server_builder /weaviate-server /bin/weaviate
RUN mkdir -p /go/pkg/mod/github.com/go-ego
COPY --from=server_builder /go/pkg/mod/github.com/go-ego /go/pkg/mod/github.com/go-ego
RUN apk add --no-cache --upgrade bc ca-certificates openssl
RUN mkdir ./modules
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http"]
