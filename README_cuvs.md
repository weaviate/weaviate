# How to set up Weaviate-CUVS

## 1. Install CUVS

Since Weaviate relies on some PRs that aren't merged yet, we cannot use the official builds from conda. 

Clone `https://github.com/ajit283/cuvs.git`, ideally directly next to the weaviate folder, then you don't have to change this rewrite: `replace github.com/rapidsai/cuvs/go => ../cuvs/go`

Checkout to `integration-branch`, this is a branch which includes all unmerged PRs, including the go API.

Follow the instructions on `https://docs.rapids.ai/api/cuvs/nightly/build/#build-from-source` to build and install.

## 2. Setup CGO

CGO needs to find cuvs and cuda libraries. Set these environment variables, adjusting the cuvs locations:

```
CGO_CFLAGS=-I/usr/local/cuda/include -I/home/ajit/miniforge3/envs/cuvs/include
```

```
CGO_LDFLAGS=-L/usr/local/cuda/lib64 -L/home/ajit/miniforge3/envs/cuvs/lib -lcudart -lcuvs -lcuvs_c
```

```
LD_LIBRARY_PATH="/home/ajit/miniforge3/envs/cuvs/lib:$LD_LIBRARY_PATH"
```

## 3. Install Go API

In the CUVS repo folder, navigate to `/go` and run `go build ./...` to build the library.

## 4. Run benchmarks

In the Weaviate repo, check out to the `cuvs` branch. 
You can run the benchmark on the `dbpedia-openai-1000k-angular.hdf5` dataset like this in the `adapters/repos/db/vector/cuvs/` location:

```
go test -bench='^BenchmarkDataset$' -tags cuvs -v
```

You can run the entire Weaviate server like this:

```
ASYNC_INDEXING=true ASYNC_BATCH_SIZE=330000 ./tools/dev/run_dev_server.sh local-cuvs-development
```

In the `weaviate-benchmarking` repo, check out to the `cuvs` branch and run something like this:

```
go run . ann-benchmark -v ~/datasets/dbpedia-openai-1000k-angular.hdf5 -d l2-squared --indexType "cuvs" -b 100000 --queryDelaySeconds 30
```


## Docker

To build the docker container, run `docker_build.sh`, potentially adjusting the cuvs build context location.

You need to install the `nvidia-container-toolkit` package from APT before. Then, run:

```
docker run --gpus all -p 8080:8080 weaviate-cuvs
```
