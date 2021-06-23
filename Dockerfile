# Dockerfile for development purposes.
# Read docs/development.md for more information

# vi: ft=dockerfile

###############################################################################
# Base build image
FROM golang:1.16-alpine AS build_base
RUN apk add bash ca-certificates git gcc g++ libc-dev
WORKDIR /go/src/github.com/semi-technologies/weaviate
ENV GO111MODULE=on
# Populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .
RUN go mod download

###############################################################################
# This image builds the weavaite server
FROM build_base AS server_builder
ARG TARGETARCH
COPY . .
RUN GOOS=linux GOARCH=$TARGETARCH go build  -ldflags '-w -extldflags "-static"' -o /weaviate-server ./cmd/weaviate-server

###############################################################################
# This creates an image that can be used to fake an api for telemetry acceptance test purposes
FROM build_base AS telemetry_mock_api
COPY . .
ENTRYPOINT ["./tools/dev/telemetry_mock_api.sh"]

###############################################################################
# Weaviate (no differentiation between dev/test/prod - 12 factor!)
FROM alpine AS weaviate
ENTRYPOINT ["/bin/weaviate"]
COPY --from=server_builder /weaviate-server /bin/weaviate
COPY --from=build_base /etc/ssl/certs /etc/ssl/certs
RUN mkdir ./modules
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http"]
