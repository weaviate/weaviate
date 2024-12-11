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
ARG GIT_BRANCH="unknown"
ARG GIT_REVISION="unknown"
ARG BUILD_USER="unknown"
ARG BUILD_DATE="unknown"
ARG EXTRA_BUILD_ARGS=""
COPY . .
RUN GOOS=linux GOARCH=$TARGETARCH go build $EXTRA_BUILD_ARGS \
      -ldflags '-w -extldflags "-static" \
      -X github.com/weaviate/weaviate/usecases/build.Branch='"$GIT_BRANCH"' \
      -X github.com/weaviate/weaviate/usecases/build.Revision='"$GIT_REVISION"' \
      -X github.com/weaviate/weaviate/usecases/build.BuildUser='"$BUILD_USER"' \
      -X github.com/weaviate/weaviate/usecases/build.BuildDate='"$BUILD_DATE"'' \
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

################################################################################
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

################################################################################
# Weaviate CUDA builder
# Build stage
FROM nvcr.io/nvidia/cuda:12.5.1-devel-ubuntu22.04 AS cuvs_server_builder

# Arguments for build stage
ARG CONDA_PACKAGE=""

# For local binary case, directly copy the files from named context
COPY --from=libs /. /opt/cuvs/lib/
COPY --from=include /. /opt/cuvs/include/


# Install the library using mamba if build context files aren't present
RUN if [ ! -d "/opt/cuvs/lib" ] || [ -z "$(ls -A /opt/cuvs/lib)" ]; then \
    echo "Installing from mamba" && \
    wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba && \
    ./bin/micromamba shell init -s bash -p /opt/conda && \
    mkdir -p /opt/conda/conda-meta && \
    eval "$(./bin/micromamba shell hook -s bash)" && \
    micromamba install -y -c conda-forge -c nvidia -c rapidsai ${CONDA_PACKAGE} && \
    mkdir -p /opt/lib /opt/include && \
    cp -r /opt/conda/lib/* /opt/lib/ && \
    cp -r /opt/conda/include/* /opt/include/ ; \
    fi


# Install Go 1.22
RUN apt-get update && apt-get install -y wget && \
    wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz && \
    rm go1.22.0.linux-amd64.tar.gz

# Add Go to PATH
ENV PATH=$PATH:/usr/local/go/bin



# Build your DB software
COPY . .

# install cgo-related dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential
    # libc6-dev \
    # make \
    # pkg-config \
    # && rm -rf /var/lib/apt/lists/*

RUN dpkg -S /usr/include/asm-generic
RUN dpkg -S /usr/include/x86_64-linux-gnu


ENV CGO_CFLAGS="-I/usr/local/cuda/include -I/opt/cuvs/include"
ENV CGO_LDFLAGS="-L/usr/local/cuda/lib64 -L/opt/cuvs/lib -lcudart -lcuvs -lcuvs_c"
RUN sed -i '/#include <time.h>/a #include <asm-generic/errno.h>' /usr/local/go/src/runtime/cgo/gcc_libinit.c
RUN find /usr -name cuda_runtime.h
ENV CC=x86_64-linux-gnu-gcc

ARG TARGETARCH
ARG GIT_BRANCH="unknown"
ARG GIT_REVISION="unknown"
ARG BUILD_USER="unknown"
ARG BUILD_DATE="unknown"
ARG EXTRA_BUILD_ARGS=""

RUN echo $CGO_LDFLAGS
RUN echo $CGO_CFLAGS 

RUN ls .
RUN ls /

# WORKDIR /go/src/github.com/weaviate/weaviate  # Add this line

# Fix the build command - note the quotes and flags organization
RUN  GOOS=linux GOARCH=$TARGETARCH CGO_ENABLED=1 go build $EXTRA_BUILD_ARGS \
    -tags cuvs \
    -ldflags "-w \
    -X github.com/weaviate/weaviate/usecases/build.Branch=$GIT_BRANCH \
    -X github.com/weaviate/weaviate/usecases/build.Revision=$GIT_REVISION \
    -X github.com/weaviate/weaviate/usecases/build.BuildUser=$BUILD_USER \
    -X github.com/weaviate/weaviate/usecases/build.BuildDate=$BUILD_DATE" \
    -o /weaviate /cmd/weaviate-server

RUN ls /weaviate

# Runtime stage
FROM nvcr.io/nvidia/cuda:12.5.1-runtime-ubuntu22.04 as weaviate_cuvs

# Don't need Go in runtime stage if you're just running the binary
# Copy just the built binary and libraries
COPY --from=cuvs_server_builder /weaviate /bin/weaviate
COPY --from=cuvs_server_builder /opt/cuvs/lib /opt/cuvs/lib

# Set environment variables
ENV LD_LIBRARY_PATH=/opt/cuvs/lib:$LD_LIBRARY_PATH
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

# These commands from alpine won't work on ubuntu
# RUN apk add --no-cache --upgrade bc ca-certificates openssl
RUN apt-get update && apt-get install -y ca-certificates openssl

ENTRYPOINT ["/bin/weaviate"]
CMD [ "--host", "0.0.0.0", "--port", "8080", "--scheme", "http"]
