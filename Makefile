SHELL:=/usr/bin/env bash -o pipefail

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
# .PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-45s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := weaviate

GO_VERSION         := 1.24.0

# Git tags
GIT_REVISION       := $(shell git rev-parse --short HEAD)
GIT_BRANCH         := $(shell git rev-parse --abbrev-ref HEAD)

# Golang environment
GOOS               ?= $(shell go env GOOS)
GOHOSTOS           ?= $(shell go env GOHOSTOS)
GOARCH             ?= $(shell go env GOARCH)
GOARM              ?= $(shell go env GOARM)
GOEXPERIMENT       ?= $(shell go env GOEXPERIMENT)
CGO_ENABLED        := 0
GO_ENV             := GOEXPERIMENT=$(GOEXPERIMENT) GOOS=$(GOOS) GOARCH=$(GOARCH) GOARM=$(GOARM) CGO_ENABLED=$(CGO_ENABLED)
GOTEST             ?= go test

# Golang Build flags
VPREFIX            := github.com/weaviate/weaviate/usecases/build
GO_LDFLAGS         := -X $(VPREFIX).Branch=$(GIT_BRANCH) \
                      -X $(VPREFIX).Revision=$(GIT_REVISION) \
                      -X $(VPREFIX).BuildUser=$(shell whoami)@$(shell hostname) \
                      -X $(VPREFIX).BuildDate=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_FLAGS           := -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS)" -tags netgo
DYN_GO_FLAGS       := -ldflags "-s -w $(GO_LDFLAGS)" -tags netgo

# Debug build flags
DEBUG_GO_FLAGS     := -gcflags "all=-N -l" -ldflags "-extldflags \"-static\" $(GO_LDFLAGS)" -tags netgo
DEBUG_DYN_GO_FLAGS := -gcflags "all=-N -l" -ldflags "$(GO_LDFLAGS)" -tags netgo

# Docker images
IMAGE_PREFIX           ?= semitechnologies
IMAGE_TAG              ?= $(shell ./tools/dev/image-tag.sh)
WEAVIATE_IMAGE         ?= $(IMAGE_PREFIX)/weaviate:$(IMAGE_TAG)

# OCI (Docker) setup
OCI_PLATFORMS  := --platform=linux/amd64,linux/arm64
OCI_BUILD_ARGS := --build-arg GO_VERSION=$(GO_VERSION) --build-arg BUILD_IMAGE=$(BUILD_IMAGE)
OCI_PUSH_ARGS  := -o type=registry
OCI_PUSH       := docker push
OCI_TAG        := docker tag

ifeq ($(CI),true)
  # buildx is used on the CI for cross-platform builds
	_               := $(shell ./tools/dev/ensure-buildx-builder.sh)
	OCI_BUILD       := DOCKER_BUILDKIT=1 docker buildx build --load $(OCI_PLATFORMS) $(OCI_BUILD_ARGS)
else
	OCI_BUILD       := DOCKER_BUILDKIT=1 docker build $(OCI_BUILD_ARGS)
endif


# Weaviate binary
.PHONY: cmd/weaviate-server/weaviate

weaviate: cmd/weaviate-server/weaviate ## Build weaviate binary (Default)
weaviate-debug: cmd/weaviate-server/weaviate-debug ## Build weaviate-debug binary

cmd/weaviate-server/weaviate:
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)

cmd/weaviate-server/weaviate-debug:
	CGO_ENABLED=0 go build $(DEBUG_GO_FLAGS) -o $@ ./$(@D)

# Weaviate OCI (Docker) images
weaviate-image: ## Build weaviate OCI (Docker) image
	$(OCI_BUILD) -t $(WEAVIATE_IMAGE) -f Dockerfile .

# Run tests
test: weaviate ## Run all unit test cases
	./test/run.sh -u

# ideally we need to separate everything except unit tests cases with build tags `go:build integration` or something similar. But not there yet. We can do it incrementally.
test-integration: ## Run all the integration tests
	./test/run.sh -i

contextionary: ## Run the contextionary embedding server
	./tools/dev/restart_dev_environment.sh

monitoring: ## Run the prometheus and grafana for monitoring
	./tools/dev/restart_dev_environment.sh --prometheus

local: ## Run the local development setup with single node
	./tools/dev/run_dev_server.sh local-single-node

local-oidc:
	./tools/dev/run_dev_server.sh local-wcs-oidc-and-apikey

debug: ## Connect local weaviate server via delv for debugging
	./tools/dev/run_dev_server.sh debug

banner: ## Add Weaviate banner with license details
	./tools/gen-code-from-swagger.sh

.PHONY: mocks
mocks:
	docker run --rm -v $(PWD):/src -w /src vektra/mockery:v2.53.2
	$(MAKE) banner
