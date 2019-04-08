#!/bin/bash

# create animal thing class
curl localhost:8080/weaviate/v1/schema/things -H 'Content-Type: application/json' -d @./tools/dev/example-payloads/animal-class.json

# create zoo thing class
curl localhost:8080/weaviate/v1/schema/things -H 'Content-Type: application/json' -d @./tools/dev/example-payloads/zoo-class.json

# create an elephant
curl localhost:8080/weaviate/v1/things -H 'Content-Type: application/json' -d @./tools/dev/example-payloads/animal-instance-elephant.json

# # create a zoo
# curl localhost:8080/weaviate/v1/things -H 'Content-Type: application/json' -d @./tools/dev/example-payloads/zoo-instance-nowhere.json

