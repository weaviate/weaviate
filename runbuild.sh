#!/bin/bash

cp go.mod.last go.mod
go build -o ./weaviate-server ./cmd/weaviate-server
