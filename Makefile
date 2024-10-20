# Version info for binaries
IMAGE_TAG_EXP := "kavirajk/weaviate-exp"
IMAGE_TAG := "kavirajk/weaviate"

VPREFIX := "github.com/weaviate/weaviate/usecases/build"
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

GO_LDFLAGS   := -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION) -X $(VPREFIX).BuildUser=$(shell whoami)@$(shell hostname) -X $(VPREFIX).BuildDate=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_LDFLAGS_EXP   := -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG_EXP) -X $(VPREFIX).Revision=$(GIT_REVISION) -X $(VPREFIX).BuildUser=$(shell whoami)@$(shell hostname) -X $(VPREFIX).BuildDate=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

GO_FLAGS     := -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS)" -tags netgo
GO_FLAGS_EXP     := -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS_EXP)" -tags netgo

build:
	CGO_ENABLED=0 go build $(GO_FLAGS) -o weaviate-server ./cmd/weaviate-server

build-exp:
	CGO_ENABLED=0 go build $(GO_FLAGS) -o weaviate ./cmd/weaviate


weaviate-image: build
	docker build --build-arg=GO_VERSION=$(GO_VERSION) -t $(IMAGE_TAG):$(GIT_REVISION) . --target=weaviate
	docker push $(IMAGE_TAG):$(GIT_REVISION)

weaviate-exp-image: build-exp
	docker build --build-arg=GO_VERSION=$(GO_VERSION) -t $(IMAGE_TAG_EXP):$(GIT_REVISION) . --target=weaviate_experimental
	docker push $(IMAGE_TAG_EXP):$(GIT_REVISION)

images: weaviate-image weaviate-exp-image
