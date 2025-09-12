# syntax=docker/dockerfile:1.6
# Dockerfile for development purposes.
# Read docs/development.md for more information
# vi: ft=dockerfile

###############################################################################
# Base build image
FROM golang:1.24-alpine AS build_base
RUN apk add --no-cache bash ca-certificates git gcc g++ libc-dev
WORKDIR /go/src/github.com/weaviate/weaviate
ENV GO111MODULE=on
# Populate the module cache based on the go.{mod,sum} files.
ENV GOMODCACHE=/go/pkg/mod
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build,sharing=locked \
    go mod download

###############################################################################
# This image builds the weaviate server
FROM build_base AS server_builder
ARG TARGETARCH
ARG GIT_BRANCH="unknown"
ARG GIT_REVISION="unknown"
ARG BUILD_USER="unknown"
ARG BUILD_DATE="unknown"
ARG EXTRA_BUILD_ARGS=""
ARG CGO_ENABLED=1
ENV CGO_ENABLED=$CGO_ENABLED
COPY . .

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build,sharing=locked \
    GOOS=linux GOARCH=$TARGETARCH go build $EXTRA_BUILD_ARGS \
      -ldflags '-w -extldflags "-static" \
      -X github.com/weaviate/weaviate/usecases/build.Branch='"$GIT_BRANCH"' \
      -X github.com/weaviate/weaviate/usecases/build.Revision='"$GIT_REVISION"' \
      -X github.com/weaviate/weaviate/usecases/build.BuildUser='"$BUILD_USER"' \
      -X github.com/weaviate/weaviate/usecases/build.BuildDate='"$BUILD_DATE"'' \
      -o /weaviate-server ./cmd/weaviate-server

RUN mkdir -p /runtime/go-ego && \
    if [ -d /go/pkg/mod/github.com/go-ego ]; then \
      cp -a /go/pkg/mod/github.com/go-ego/* /runtime/go-ego/; \
    fi



###############################################################################

# This creates an image that can be used to fake an api for telemetry acceptance test purposes
FROM build_base AS telemetry_mock_api
COPY . .
ENTRYPOINT ["./tools/dev/telemetry_mock_api.sh"]

###############################################################################
# Weaviate (no differentiation between dev/test/prod - 12 factor!)
FROM alpine AS weaviate
RUN apk add --no-cache bc ca-certificates openssl && mkdir ./modules
COPY --from=server_builder /weaviate-server /bin/weaviate
COPY --from=server_builder /runtime/go-ego/ /go/pkg/mod/github.com/go-ego/
ENTRYPOINT ["/bin/weaviate"]
CMD ["--host","0.0.0.0","--port","8080","--scheme","http"]
