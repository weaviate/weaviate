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
# This image builds the weaviate server with debug enabled and optimization disabled
FROM build_base AS server_builder_debug
ARG TARGETARCH
ENV CGO_ENABLED=0
COPY . .
RUN go install github.com/go-delve/delve/cmd/dlv@latest
RUN GOOS=linux GOARCH=$TARGETARCH go build \
      -gcflags "all=-N -l" \
      -o /weaviate-server ./cmd/weaviate-server

###############################################################################
# This creates an image that can be used to fake an api for telemetry acceptance test purposes
FROM build_base AS telemetry_mock_api
COPY . .
ENTRYPOINT ["./tools/dev/telemetry_mock_api.sh"]

###############################################################################
# This image gets grpc health check probe
FROM build_base AS grpc_health_probe_builder
ARG TARGETARCH
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.22 && \
      wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH} && \
      chmod +x /bin/grpc_health_probe

###############################################################################
# Weaviate with debug enabled and ready for remote debugging. Use for debugging/dev purpose only!
FROM alpine AS weaviate_debug
ENTRYPOINT [ "/bin/dlv" ]
COPY --from=server_builder_debug /go/bin/dlv /bin/dlv
COPY --from=grpc_health_probe_builder /bin/grpc_health_probe /bin/
COPY --from=server_builder_debug /weaviate-server /bin/weaviate
RUN apk add --no-cache --upgrade ca-certificates openssl
RUN mkdir ./modules
CMD ["--listen=:2345", "--headless=true", "--check-go-version=false", "--log=true", "--accept-multiclient", "--api-version=2", "exec", "/bin/weaviate", "--", "--host", "0.0.0.0", "--port", "8080", "--scheme", "http"]
