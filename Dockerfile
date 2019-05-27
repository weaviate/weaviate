# Dockerfile for development purposes.
# Read docs/development.md for more information

# vi: ft=dockerfile

###############################################################################
# Base build image
FROM golang:1.11-alpine AS build_base
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
# This image builds the contextionary fixtures FOR DEV OR TEST.
FROM build_base AS contextionary_fixture_builder
RUN apk add -U jq
COPY go.mod go.sum ./
COPY ./test/contextionary ./test/contextionary
COPY ./contextionary ./contextionary
RUN ./test/contextionary/gen_simple_contextionary.sh

###############################################################################
# This is the base image for running waviates configurations IN DEV OR TEST; contains the executable & contextionary
FROM alpine AS weaviate_base
COPY --from=server_builder /go/bin/weaviate-server /bin/weaviate
COPY --from=build_base /etc/ssl/certs /etc/ssl/certs
COPY --from=contextionary_fixture_builder /go/src/github.com/semi-technologies/weaviate/test/contextionary/example.idx /contextionary/contextionary.idx
COPY --from=contextionary_fixture_builder /go/src/github.com/semi-technologies/weaviate/test/contextionary/example.knn /contextionary/contextionary.knn
COPY --from=contextionary_fixture_builder /go/src/github.com/semi-technologies/weaviate/test/contextionary/stopwords.json /contextionary/stopwords.json
ENTRYPOINT ["/bin/weaviate"]

###############################################################################
# Development configuration with demo dataset
FROM weaviate_base AS development
COPY ./tools/dev/schema /schema
COPY ./tools/dev/config.docker.yaml /weaviate.conf.yaml
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http", "--config-file", "./weaviate.conf.yaml" ]

###############################################################################
# Configuration used for the acceptance tests.
FROM weaviate_base AS test
COPY ./test/schema/test-action-schema.json /schema/actions_schema.json
COPY ./test/schema/test-thing-schema.json /schema/things_schema.json
COPY ./tools/dev/config.docker.yaml /weaviate.conf.yaml
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http", "--config-file", "./weaviate.conf.yaml" ]

###############################################################################
# This is the production image for running waviates configurations; contains the executable & contextionary
FROM alpine as weaviate_prod
RUN apk add --no-cache curl jq
COPY --from=server_builder /go/bin/weaviate-server /bin/weaviate
COPY --from=build_base /etc/ssl/certs /etc/ssl/certs

RUN mkdir /contextionary
ARG CONTEXTIONARY_VERSION
ARG CONTEXTIONARY_LOC

RUN if [ -z "$CONTEXTIONARY_LOC" ]; \
	then if [ -z "$CONTEXTIONARY_VERSION" ]; \
		then export CONTEXTIONARY_VERSION=$(curl -sS https://c11y.semi.technology/contextionary.json | jq -r ".latestVersion"); \
		fi; \
	export CONTEXTIONARY_LOC=https://c11y.semi.technology/$CONTEXTIONARY_VERSION/en; \
	wget -O /contextionary/contextionary.vocab $CONTEXTIONARY_LOC/contextionary.vocab; \
	wget -O /contextionary/contextionary.idx $CONTEXTIONARY_LOC/contextionary.idx; \
	wget -O /contextionary/contextionary.knn $CONTEXTIONARY_LOC/contextionary.knn; \
	fi

COPY tmp.txt $CONTEXTIONARY_LOC/contextionary.vocab* $CONTEXTIONARY_LOC/contextionary.idx* $CONTEXTIONARY_LOC/contextionary.knn* /contextionary/

ENTRYPOINT ["/bin/weaviate"]
