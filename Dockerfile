# Dockerfile for development purposes.
# Read docs/development.md for more information

# vi: ft=dockerfile

###############################################################################
# Base build image
FROM golang:1.13-alpine AS build_base
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
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go install -a -tags netgo -ldflags '-w -extldflags "-static"' ./cmd/weaviate-server

###############################################################################
# This image builds the genesis
FROM build_base AS genesis
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go install -a -tags netgo -ldflags '-w -extldflags "-static"' ./genesis/cmd/weaviate-genesis-server/
ENTRYPOINT ["/go/bin/weaviate-genesis-server"]

# ###############################################################################
# # This creates an image that can be run to import the demo dataset for development
# FROM build_base AS data_importer
# COPY . .
# ENTRYPOINT ["./tools/dev/import_demo_data.sh"]

###############################################################################
# This creates an image that can be used to fake a genesis for a local network setup
FROM build_base AS genesis_fake
COPY . .
ENTRYPOINT ["./tools/dev/genesis_fake.sh"]

###############################################################################
# This creates an image that can be used to fake a genesis for a local network setup
FROM build_base AS remote_weaviate_fake
COPY . .
ENTRYPOINT ["./tools/dev/remote_weaviate_fake.sh"]

###############################################################################
# This creates an image that can be used to fake an api for telemetry acceptance test purposes
FROM build_base AS telemetry_mock_api
COPY . .
ENTRYPOINT ["./tools/dev/telemetry_mock_api.sh"]

###############################################################################
# Weaviate (no differentiation between dev/test/prod - 12 factor!)
# It has a development-friendly config file by default, but the config
# can of course be overwritten through any mounted config file.
FROM alpine AS weaviate
ENTRYPOINT ["/bin/weaviate"]
COPY ./tools/dev/config.docker.yaml /weaviate.conf.yaml
COPY --from=server_builder /go/bin/weaviate-server /bin/weaviate
COPY --from=build_base /etc/ssl/certs /etc/ssl/certs
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http", "--config-file", "./weaviate.conf.yaml" ]
