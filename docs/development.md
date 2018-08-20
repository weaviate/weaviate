# Development of Weaviate

## FAQ
- Based on `go-swagger` tool.
- The following files are completely generated.
  - `models`/
  - `restapi/`
  - `restapi/server.go`
  - `cmd/weaviate-server/main.go`
- The file `restapi/configure_weaviate.go` is partially automatically generated, partially hand-edited.

## Dockerized development environment

### Build and run Weaviate

```
docker build -f Dockerfile.dev -t weaviate/development .
docker run --rm -p 8080:8080 weaviate/development
```


### Build and run the acceptance tests

```
docker build -f Dockerfile.dev --target acceptance_test -t weaviate/acceptance_test .
docker run --net=host --rm weaviate/acceptance_test -args -server-port=8080 -server-host=localhost -api-token=blah -api-key=blah
```
