Run the test server in Docker.

Prerequisites:
- Docker 1.10.0 or higher
- docker-compose 1.6.0 or higher

from this directory (i.e. docker-server), run `docker-compose up`

After an update of the Weaviate code, run
1. docker-compose stop
1. docker-compose rm -f
1. docker-compose build
1. docker-compose up
