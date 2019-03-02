# Getting Started

> An introduction to Weaviate and how to contribute.

You can run a complete setup while developing like this;

```bash
$ docker-compose -f ./docker-compose/runtime-dev/docker-compose.yml up 
```

Clone weaviate;

```bash
$ git clone https://github.com/creativesoftwarefdn/weaviate
```

From the Weaviate root, download the contextionary;

```bash
$ wget -O ./contextionary/contextionary.idx https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.idx 
$ wget -O ./contextionary/contextionary.knn https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.knn 
$ wget -O ./contextionary/contextionary.vocab https://contextionary.creativesoftwarefdn.org/$(curl -sS https://contextionary.creativesoftwarefdn.org/contextionary.json | jq -r ".latestVersion")/en/contextionary.vocab 
```

From the Weaviate root;

```bash
$ export GO111MODULE=on
$ go run cmd/weaviate-server/main.go --host=0.0.0.0 --port=8080 --scheme=http --config=dev --config-file=./docker-compose/runtime-dev/config.json
```

- Weaviate will be availabe on port 8080
- Weaviate Playground will be available on port 80