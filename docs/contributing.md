---
publishedOnWebsite: false
title: Contributing
subject: OSS
---

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

Want to quickly build & run the currently checked out version of weaviate?
Check the instructions in the [docker-compose-dev.yml](../docker-compose-dev.yml) file.

### Build and run the acceptance tests in Docker
Be sure to run a weaviate instance backed by the schema in test/schema.

Then run the acceptance tests:

```
docker build -f Dockerfile.dev --target acceptance_test -t weaviate/acceptance_test .
docker run --net=host --rm weaviate/acceptance_test -args -server-port=8080 -server-host=localhost -api-token=blah -api-key=blah
```

and the refactored (and faster, but incomplete) acceptance tests:

```
docker build -f Dockerfile.dev --target new_acceptance_test -t weaviate/new_acceptance_test .
docker run --net=host --rm weaviate/new_acceptance_test -args -server-port=8080 -server-host=localhost -api-token=blah -api-key=blah
```

# Contribute to documentation

You can contribute to the documentation by add a markdown file to the `/docs` folder. If the doc is intended for users of Weaviate make sure to add the following header to the file:

```
---
publishedOnWebsite: true
title: Foobar
subject: OSS|Enterprise
---
```

_note: `OSS` = topic open source and `Enterprise` is for the enterprise version of Weaviate._