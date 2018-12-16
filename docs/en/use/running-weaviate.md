# Running Weaviate

> Documentation on how to run Weaviate with Docker-compose, Docker or stand-alone.

This document describes how to run Weaviate for users. If you want to run a development version of Weaviate for contributors, click [here](../contribute/running-weaviate.md).

## Run full stack with Docker-compose

A complete Weaviate stack based on Janusgraph (with; Elasticsearch and Cassandra) can be directly run with the Docker compose files available in this repo.

#### Running the latest stable version

```sh
$ wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/docker-compose/runtime-stable/docker-compose.yml
$ docker-compose up
```

- Releases can be found [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Based on `tree/master` on Github
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

#### Running the latest unstable version

```sh
$ wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/docker-compose/runtime-unstable/docker-compose.yml
$ docker-compose up
```

- Based on `tree/develop` on Github
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080.
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

#### Running a specific version

```sh
$ wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/docker-compose/runtime-stable/docker-compose.yml
```

Open docker-compose.yml and replace `stable` in the image (`image: creativesoftwarefdn/weaviate:stable`) with the prefered version number.

```sh
$ docker-compose up
```

- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

## Run Weaviate stand alone with Docker

Weaviate can also be run stand-alone.

#### Stable

```sh
$ docker run creativesoftwarefdn/weaviate:stable
```

- Based on `tree/master` on Github
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

#### Specific Stable version

```sh
$ docker run creativesoftwarefdn/weaviate:$VERSION
```

- Releases can be found [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

#### Unstable

```sh
$ docker run creativesoftwarefdn/weaviate:unstable
```

- Based on `tree/davelop` on Github
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

## Running with Custom Contextionary

More information about running Weaviate with a custom Contextionary can be found in the [`docs/en/contribute/running-weaviate.md`](docs/en/contribute/running-weaviate.md) docs.

## Running with custom server configuration

If you want to run Weaviate with a specific configuration (for example over SSL or a different port) you can take the following steps.

```sh
# Clone the repo
$ git clone https://github.com/creativesoftwarefdn/weaviate
# Select the correct branch, 
```