#!/usr/bin/env bash
docker build -t graphql_prototype .
docker run --rm -ti -p 8081:8081 graphql_prototype
