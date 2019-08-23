# Some common issues while developing and how to solve them

## Issue: `etcd: requested lease not found` while running the test suite locally

### Symptoms
Running the test suite locally, every request which neeeds distributed locking,
fails with `could not aquire lock: could not get schema lock: etcdserver:
requested lease not found`.

### Reason
Most likely you ran weaviate on your host machine and forgot to exit the app.
Weaviate is thus still listening locally on port 8080. Then you started the
test suite which also tries to spin up weaviate on port 8080. Since
docker-compose only checks if the desired port is already occupied in the
docker network it doesn't failed. You're effectively running weaviate twice on
the same port now. Calls from within the docker network land on the correct
version, calls from the host machine (such as from the test suite) land on the
wrong instance

### Solution
Exit the locally running version and restart the test suite

## Issue: Docker for Mac hangs during local load tests

### Symptoms
When generating a bit more load than usual with the local docker-compose setup,
such as during load tests, at some point the entire docker daemon stops responding,
requests to containers simply hang and commands like `docker ps` either fail
or are incredibly slow.

### Reason 
Docker for mac seems broken.

### Solution
As described in [this comment](https://github.com/docker/for-mac/issues/3674#issuecomment-498039937) 
downgrading to the mentioned version fixes the problem.
