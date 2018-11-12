---
publishedOnWebsite: true
title: Running with Docker
subject: OSS
---

# Running with Docker Compose & Data storage

The easiest way to get Weaviate running is through Docker Compose. Below is a list of commands that can be used to get the service running. In case of issues, please report them [here](https://github.com/creativesoftwarefdn/weaviate/issues) in case of questions please ask them [here](https://stackoverflow.com/questions/tagged/weaviate).

### simple-http-service

- Build Weaviate: `docker-compose -f docker-compose.yml build weaviate`
- Run Weaviate: `TBD`
- Directly builds and runs Weaviate, JanusGraph, Cassandra and Elastic with the [standard contextionary](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/use/FAQ.md#q-what-does-the-standard-contextionary-consists-of). The docker compose files are available [here](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docker-compose.yml)
- Weaviate will become available on: http://test

### simple-https-service

- `TBD`
- Runs like the `simple-http-service` but with certificates.
- Weaviate will become available on: http://test

### custom-contextionary

- `TBD`

### unstable-image

- `TBD`
- The Weaviate setup runs standard based on the stable branch. In case you want to run the unstable (develop) branch. Click [here](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/contribute/contributing.md#docker-images) for more information regarding the Docker image structure.
- Same as `simple-http-service`

### Custom Port or Custom host

- `TBD`
- Weaviate exposes the service by default on port 8080 and host 127.0.0.1
