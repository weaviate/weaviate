# Running Weaviate

> Documentation on how to run Weaviate with Docker-compose, Docker or stand-alone.

> Note: Weaviate is currently only available as an unstable release. We hope to release a first stable version in the coming months.

This document describes how to run Weaviate for users. If you want to run a development version of Weaviate for contributors, click [here](../contribute/running-weaviate.md).

## Run full stack with Docker-compose

A complete Weaviate stack based on Janusgraph (with; Elasticsearch and Cassandra) can be directly run with the Docker compose files available in this repo.

_Note: make sure to collect your `ROOTTOKEN` and `ROOTKEY` after starting a Weaviate. They will show up in the log files. When running Docker-compose you can [follow these steps](#user-content-getting-rootkey-and-roottoken-using-docker-compose) to collect them._

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

- Based on `tree/develop` on Github
- Runs with the latest open source Contextionary. More indepth information about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on `://{IP}/weaviate/v1/{COMMAND}`.

## Running with Custom Contextionary

The contextionary files are build into the Docker image. To use a custom contextionary, you will need to build a custom Docker image. This can be done easily using the build argument `CONTEXTIONARY_LOC`. This argument can point either to a local folder or a URL. This location must contain the following three files:

* contextionary.vocab
* contextionary.knn
* contextionary.idx

This argument can be specified as shown in the examples below:

```sh
$ export CONTEXTIONARY_LOC=https://example.com/my_contextionary_location/
$ docker build -t my-weaviate-image --build-arg CONTEXTIONARY_LOC .
```

OR

```sh
$ export CONTEXTIONARY_LOC=/home/user/custom-contextionary/
$ docker build -t my-weaviate-image --build-arg CONTEXTIONARY_LOC .
```

## Running with custom server configuration

If you want to run Weaviate with a specific configuration (for example over SSL or a different port) you can take the following steps.

```sh
# Clone the repo
$ git clone https://github.com/creativesoftwarefdn/weaviate
# Select the correct branch, 
```

- You can set the environment variable `SCHEME` to override the default (`http`) E.g. `SCHEME=https docker-compose up -d`
- You can set the environment variables `HOST` and `PORT` to override the defaults. E.g. `HOST=0.0.0.0 PORT=1337 docker-compose up -d`	

## Getting ROOTKEY and ROOTTOKEN using Docker-compose

To authenticate, you will need the `ROOTKEY` and `ROOTTOKEN`. When using Docker-compose, you'll be able to find them in the log files.

```sh
# Find the log file, look for creativesoftwarefdn/weaviate:${VERSION}. Copy the `CONTAINER ID`
$ docker ps
# Find the ROOTKEY
$ cat $(docker inspect --format='{{.LogPath}}' $CONTAINER_ID) | grep -sPo '(?<=ROOTKEY=)[-a-f0-9]+'
# Find the ROOTTOKEN
$ cat $(docker inspect --format='{{.LogPath}}' $CONTAINER_ID) | grep -sPo '(?<=ROOTTOKEN=)[-a-f0-9]+'
```

_Note: you might need to run with `sudo` depending on how your Docker deamon runs._
