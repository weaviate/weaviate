# Contribution Guide Lines

> An overview of general contribution guide lines.

## FAQ

- Based on `go-swagger` tool.
- The following files are completely generated.
  - `models`/
  - `restapi/`
  - `restapi/server.go`
  - `cmd/weaviate-server/main.go`
- The file `restapi/configure_weaviate.go` is partially automatically generated, partially hand-edited.

## Getting Started with Contributing

Great that you want to help out with Weaviate!

The best way of getting started is to look at the [getting started guide](./getting-started.md) for end-users. It explains how to run Weaviate and how to consume the APIs. Next, it would be good to get indepth knowledge about the API's and especially GraphQL. All interfaces exposes the innerworkings of GQL.

## Pull Requests and Commits

1. Regardless of what you are working on, always create a Github issue and label it properly.
2. Always use the issue when creating a pull request or when committing.

#### Smart Commits

Please use [Github's autolink-urls](https://help.github.com/articles/autolinked-references-and-urls/) (aka _smart commits_) when committing an issue or creating a pull request. In case of a PR without smart commits, they will be [squashed]() into one single commit with the reference.

- For example: `$ git commit -m "gh-123: MESSAGE"`
- For automated commits, for example through Travis-ci, you can use: `git commit -m "🤖 MESSAGE"`

## Versioning

We use [semver](https://semver.org/) for versioning. The version number can be found in the [API specs](../openapi-specs/schema.json) under: `.info.version`. This version number will also used to publish binaries.

To see the latest version, run on of the following [jq](https://stedolan.github.io/jq/) functions;

| Description | Command |
| ----------- | ------- |
| From current local branch | `$ jq -r '.info.version' ./openapi-specs/schema.json` |
| Get current `tree/develop` version | `$ curl -sS https://raw.githubusercontent.com/semi-technologies/weaviate/develop/openapi-specs/schema.json \| jq -r ".info.version"` |
| Get current `tree/master` |  version `$ curl -sS https://raw.githubusercontent.com/semi-technologies/weaviate/develop/openapi-specs/schema.json \| jq -r '.info.version' ` |

### Using Git

Weaviate uses the following Git rules;
- The version will be taken from the schema.json file.
- For releasing, tag a release following the semver rules.
- Travis-ci will publish a release: `weaviate:x.y.z` and a pre-release: `weaviate:x.y.z-gitHash`

## API's

### RESTful https

The complete open API specs document is available [here](../openapi-specs/schema.json). It contains all end-points and descriptions.

### GraphQL

We rely on GraphQL to expose the graphs both locally and on the network. We have extensive documentation available on this which you can find [here](./) prefixed with `graphql-*.md`.

### Error handling

In the package `restapi` you can use the function `createErrorResponseObject(message ...string)`. This will return an error object as defined in the open api schema (`ErrorResponse`).

Example usage:

```golang
createErrorResponseObject("Error 1", "Error 2", "etc")
```

## Dockerized development environment

Want to quickly build & run the currently checked out version of weaviate?
Check the instructions in the [docker-compose.yml](../docker-compose.yml) file.

### Build and run the acceptance tests in Docker

Be sure to run a weaviate instance backed by the schema in test/schema.

Then run the acceptance tests:

```
docker build -f Dockerfile --target acceptance_test -t weaviate/acceptance_test .
docker run --net=host --rm weaviate/acceptance_test -args -server-port=8080 -server-host=localhost -api-token=blah -api-key=foobar
```

and the refactored (and faster, but incomplete) acceptance tests:

```
docker build -f Dockerfile --target new_acceptance_test -t weaviate/new_acceptance_test .
docker run --net=host --rm weaviate/new_acceptance_test -args -server-port=8080 -server-host=localhost -api-token=blah -api-key=foobar
```

### Docker Images

The Docker image naming structure works as follows:

- `semi-technologies/weaviate-dev-server:latest` = Master branch.
- `semi-technologies/weaviate-dev-server:unstable` = Develop branch (note our [Gitflow](#gitflow) process).
- `semi-technologies/weaviate-dev-server:vx.y.z` = Specific [release](https://github.com/semi-technologies/weaviate/releases).

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
