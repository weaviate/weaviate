#!/bin/bash

#go test -race -coverprofile=coverage-unit.txt -covermode=atomic -count 1 github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi 

# Golang env vars
export GOPATH=$HOME/go
export GOROOT=/usr/local/opt/go/libexec
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH

which dlv

#dlv test -- adapters/handlers/rest/clusterapi/schema_component_test.go 
#dlv test github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi -- -test.run ^TestComponentCluster

# b github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi_test.TestComponentCluster

# dlv test github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi 
# dlv test github.com/weaviate/weaviate/usecases/schema
# dlv test github.com/weaviate/weaviate/usecases/config
# dlv test github.com/weaviate/weaviate/usecases/modules
dlv test github.com/weaviate/weaviate/usecases/traverser
