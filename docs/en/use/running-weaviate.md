# Running Weaviate

> How to run Weaviate with Docker-compose, Docker or stand-alone.

> Note for developers: the whole Weaviate stack needs quite some power, but it
> should run on a decent laptop for dev purposes.

This document describes how to run Weaviate for users. If you want to run a
development version of Weaviate for contributors, click
[here](../contribute/running-weaviate.md). Encountering issues? See the
[overview of known issues](https://github.com/creativesoftwarefdn/weaviate/issues?utf8=%E2%9C%93&q=label%3Adocker+label%3Abug+)
or ask [here](https://github.com/creativesoftwarefdn/weaviate#questions).

## Run full stack with Docker-compose

A complete Weaviate stack based on Janusgraph (with Elasticsearch and
Cassandra) can be directly run with the Docker compose files available in this
repo. This setup will also include the Weaviate Playground and ideal for
development purposes.

### Important information
1. The docker-compose setup contains the entire weaviate stack, including a
   Janusgraph/Elasticsearch connector. To run this stack you should have
   at least **1 CPU and 3 GB of memory available**. If you are planning to
   import large amounts of data, a larger setup is recommended.
2. It takes some time to start up the whole infrastructure. During this time,
   the backing databases will produce plenty of log output. We recommend to not
   attach to the log-output of the entire setup, but only to those of weaviate
   as described here. Weaviate will will wait up to 2 minutes for the backing
   databases to come up. This is indicated by logging `waiting to establish
   database connection, this can take some time`. You will know that the entire
   stack is ready when weaviate logs the bind address and port it is listenting
   on.
3. The configuration values used in the docker-compose setup reflect a "Try it
   out" or development setup. Production usage requires considerably more
   resources. Do not use the docker-compose setup below in production. For
   production setups, we recommend running the weaviate stack on Kubernetes.

### Running the latest version

#### Attaching to the log output of all containers

Warning: The output is quite verbose, for an alternative see [attaching to only
the log output of weaviate](#attaching-to-the-log-output-of-only-weaviate).

```sh
$ curl -s https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/tools/download-docker-compose-deps.sh | bash
$ docker-compose up
```

#### Attaching to the log output of only weaviate
The log output of weaviate's backing databases can be quite verbose. We instead
recommend to attach only to weaviate itself. In this case run `docker-compose
up` like so:

```sh
$ curl -s https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/tools/download-docker-compose-deps.sh | bash
$ docker-compose up -d && docker-compose logs -f weaviate
```

Alternatively you can run docker-compose entirely detached with `docker-compose
up -d` and poll `{bind_address}:{port}/weaviate/v1/meta` until you receive
status `200 OK`.

#### Additional Information

_Note I: This Docker compose setup uses the `:latest` tag to ensure you always
have the latest version. For production usage always use [a fixed version
tag](#running-a-specific-version)_

_Note II: You can always enforce the latest `:latest` version by re-pulling
`creativesoftwarefdn/weaviate:latest` and running `$docker-compose up -d --force-recreate`_

- Releases can be found
  [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Docker tags can be found
  [here](https://hub.docker.com/r/creativesoftwarefdn/weaviate/tags).
- Based on `tree/master` on Github
- Runs with the latest open source Contextionary. More in-depth information
  about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as an HTTP service on port 8080 on
  `http://{IP}/weaviate/v1/{RESOURCE}`.
- The Weaviate Playground becomes available as an HTTP service on port 80 on
  `http://{IP}`.
- If you want to manually download the files, download all files from
  [here](https://github.com/creativesoftwarefdn/weaviate/tree/master/docker-compose/runtime)
  and run `docker-compose up -d`.

### Running a specific version

```sh
$ curl -s https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/master/tools/download-docker-compose-deps.sh | bash
```

Open `docker-compose.yml` and replace `latest` in the image (`image:
creativesoftwarefdn/weaviate:latest`) with the preferred version number. An
overview can be found on
[Dockerhub](https://hub.docker.com/r/creativesoftwarefdn/weaviate/tags).

```sh
$ docker-compose up -d
```

- Runs with the latest open source Contextionary. More in-depth information
  about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as HTTP service on port 8080 on
  `://{IP}/weaviate/v1/{COMMAND}`.

## Run Weaviate stand-alone with Docker

Weaviate can also be run stand-alone.

### Specific version

```sh
$ docker run creativesoftwarefdn/weaviate:$VERSION
```

- Releases can be found
  [here](https://github.com/creativesoftwarefdn/weaviate/releases).
- Runs with the latest open source Contextionary. More in-depth information
  about the contextionary can be found [here](../contribute/contextionary.md).
- Weaviate becomes available as an HTTP service on port 8080 on
  `://{IP}/weaviate/v1/{COMMAND}`.

## Running with Custom Contextionary

The contextionary files are built into the Docker image. To use a custom
contextionary, you will need to build a custom Docker image. This can be done
easily using the build argument `CONTEXTIONARY_LOC`. This argument can point
either to a local folder or to a URL. This location must contain the following
three files:

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

If you want to run Weaviate with a specific configuration (for example over SSL
or a different port) you can take the following steps.

```sh
# Clone the repo
$ git clone https://github.com/creativesoftwarefdn/weaviate
# Select the correct branch,
```

- You can set the environment variable `SCHEME` to override the default
  (`http`) E.g. `SCHEME=https docker-compose up -d`
- You can set the environment variables `HOST` and `PORT` to override the
  defaults. E.g. `HOST=0.0.0.0 PORT=1337 docker-compose up -d`

## Running Kubernetes Setup

_Soon online_
