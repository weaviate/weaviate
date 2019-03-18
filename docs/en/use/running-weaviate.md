# Running Weaviate

> How to run Weaviate with Docker-compose, Docker or stand-alone.

> Note for developers: the whole Weaviate stack needs quite some power, but it should run on a decent laptop for dev purposes.

This document describes how to run Weaviate for users. If you want to run a development version of Weaviate for contributors, click [here](../contribute/running-weaviate.md). Encountering issues? See the [overview of known issues](https://github.com/creativesoftwarefdn/weaviate/issues?utf8=%E2%9C%93&q=label%3Adocker+label%3Abug+) or ask [here](https://github.com/creativesoftwarefdn/weaviate#questions).

## Run full stack with Docker-compose

A complete Weaviate stack based on Janusgraph (with Elasticsearch and Cassandra) can be directly run with the Docker compose files available in this repo. This setup will also include the Weaviate Playground and ideal for development purposes.

#### Running the latest version

```sh
$ source <(curl -s https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/tools/start-docker-compose.sh)
```

- Releases can be found [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Docker tags can be found [here](https://hub.docker.com/r/creativesoftwarefdn/weaviate/tags).
- Based on `tree/master` on Github
- Runs with the latest open source Contextionary. More in-depth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as an HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.
- The Weaviate Playground becomes available as an HTTP service on port 80 on `://{IP}`.
- If you want to manually download the files, download all files from [here](https://github.com/creativesoftwarefdn/weaviate/tree/master/docker-compose/runtime) and run `docker-compose up`.

#### Running a specific version

```sh
$ wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/docker-compose/runtime/docker-compose.yml && \
  wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/docker-compose/runtime/config.json && \
  wget https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/docker-compose/runtime/janusgraph.properties
```

Open docker-compose.yml and replace `latest` in the image (`image: creativesoftwarefdn/weaviate:latest`) with the preferred version number. An overview can be found on [Dockerhub](https://hub.docker.com/r/creativesoftwarefdn/weaviate/tags).

```sh
$ docker-compose up
```

- Runs with the latest open source Contextionary. More in-depth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

## Run Weaviate stand-alone with Docker

Weaviate can also be run stand-alone.

#### Specific version

```sh
$ docker run creativesoftwarefdn/weaviate:$VERSION
```

- Releases can be found [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Runs with the latest open source Contextionary. More in-depth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as an HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

## Running with Custom Contextionary

The contextionary files are built into the Docker image. To use a custom contextionary, you will need to build a custom Docker image. This can be done easily using the build argument `CONTEXTIONARY_LOC`. This argument can point either to a local folder or to a URL. This location must contain the following three files:

* contextionary.vocab
* contextionary.knn
* contextionary.idx

This argument can be specified as shown in the examples below:

```sh
$ export CONTEXTIONARY_LOC=https://example.com/my_contextionary_location/
$ docker build -t my-weaviate-image --build-arg CONTEXTIONARY_LOC .
```

Or

```sh
$ export CONTEXTIONARY_LOC=/home/user/custom-contextionary/
$ docker build -t my-weaviate-image --build-arg CONTEXTIONARY_LOC .
```

## Running with a custom server configuration

If you want to run Weaviate with a specific configuration (for example over SSL or a different port) you can take the following steps.

```sh
# Clone the repo
$ git clone https://github.com/creativesoftwarefdn/weaviate
# Select the correct branch, 
```

- You can set the environment variable `SCHEME` to override the default (`http`) E.g. `SCHEME=https docker-compose up -d`
- You can set the environment variables `HOST` and `PORT` to override the defaults. E.g. `HOST=0.0.0.0 PORT=1337 docker-compose up -d`	

## Running Kubernetes Setup

_Soon online_