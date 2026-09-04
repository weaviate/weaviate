SHELL:=/usr/bin/env bash -o pipefail

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
# .PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-45s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := weaviate

GO_VERSION         := $(shell go version | cut -d' ' -f3 | sed 's/^go//')

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
# -trimpath replaces absolute source paths with module-relative ones. The image
# build already does this. Matching it here means two checkouts of the same
# commit at different paths (worktrees) produce identical build-cache entries
# instead of two full copies. See "Build cache hygiene" in CLAUDE.md.
GO_FLAGS           := -trimpath -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS)" -tags netgo
DYN_GO_FLAGS       := -trimpath -ldflags "-s -w $(GO_LDFLAGS)" -tags netgo

# Debug build flags. Deliberately no -trimpath: delve needs the real source
# paths to map frames back to files.
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

local-oidc: ## Run the local development setup on single node with oidc enabled
	./tools/dev/run_dev_server.sh local-wcs-oidc-and-apikey

local-rbac: ## Run the local development setup on single node with rbac enabled
	./tools/dev/run_dev_server.sh local-single-node-rbac

debug: ## Connect local weaviate server via delv for debugging
	./tools/dev/run_dev_server.sh debug

banner: ## Add Weaviate banner with license details
	./tools/gen-code-from-swagger.sh

.PHONY: mocks
mocks: ## Regenerate test mocks
	docker run --rm -v $(PWD):/src -w /src vektra/mockery:v2.53.6
	$(MAKE) banner

.PHONY: grpc
grpc:
	./tools/dev/grpc_regenerate.sh

deps:
	@echo "Sync go deps in Weaviate, e2e with Go client and benchmark_bm25" && go mod tidy && go mod vendor && cd test/acceptance_with_go_client/ && go mod tidy && go mod vendor && cd ../benchmark_bm25/ && go mod tidy && go mod vendor && cd ../.. && echo "Success" || echo "Failed"

# Build cache hygiene
#
# The Go build cache is content-addressed but NOT path-independent: without
# -trimpath the absolute source path is part of a package's cache key, so each
# worktree gets its own full copy of the compiled dependency graph. Measured on
# this repo, a second worktree at the same commit adds ~672 MB for a plain
# `go build ./...` and ~6 MB once -trimpath is set. Test binaries and -race
# variants multiply that further. Roughly 20 worktrees running builds and test
# sweeps is enough to reach hundreds of GB.
#
# `make weaviate` already passes -trimpath. Bare `go test` / `go build` do not,
# so set it for the whole toolchain if you work across many worktrees.

.PHONY: cache-report
cache-report: ## Show Go build/module cache sizes and whether -trimpath is on
	@echo "GOCACHE     $$(go env GOCACHE)"
	@du -sh "$$(go env GOCACHE)" 2>/dev/null || true
	@echo "GOMODCACHE  $$(go env GOMODCACHE)"
	@du -sh "$$(go env GOMODCACHE)" 2>/dev/null || true
	@echo "GOFLAGS     '$$(go env GOFLAGS)'"
	@case "$$(go env GOFLAGS)" in \
	  *-trimpath*) echo "  -trimpath is set: worktrees share cache entries." ;; \
	  *) echo "  -trimpath is NOT set. Every worktree keeps its own copy of the" ; \
	     echo "  compiled dependency graph. To share them:" ; \
	     echo "    go env -w GOFLAGS=-trimpath" ;; \
	esac

.PHONY: clean-caches
clean-caches: ## Delete the Go build cache (frees disk, next build is cold)
	@echo "About to delete $$(go env GOCACHE)"
	@du -sh "$$(go env GOCACHE)" 2>/dev/null || true
	@echo "The next build in every worktree will be cold. Ctrl-C within 5s to abort."
	@sleep 5
	go clean -cache
	@echo "Done. Module cache left intact; use 'go clean -modcache' to drop that too."
