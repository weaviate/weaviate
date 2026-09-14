# Acceptance test images

Testcontainer-based tests (`test/docker`) take the Weaviate image from `TEST_WEAVIATE_IMAGE`. When it
is unset, they build the image from the repository's `Dockerfile` for every container they start. A
package with many test functions (e.g. `reindex_multinode`) therefore rebuilds the image many times,
wasting disk and time, and can exceed startup timeouts.

## Running tests locally

Build the image once, then point the tests at it:

```bash
docker compose -f docker-compose-test.yml build weaviate
TEST_WEAVIATE_IMAGE=weaviate/test-server go test -count 1 -race -timeout 20m ./test/acceptance/reindex_multinode/...
```

Add `--build-arg EXTRA_BUILD_ARGS="-race"` to the build to match CI, which runs the server with the
race detector.

Rebuild the image after every code change. Nothing checks that the image matches the working tree, so
a stale image runs the tests against old code.

Use the `weaviate/test-server` tag, not a custom one. Some recovery and replication tests call
`t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")` and ignore any other value, so they only
pick up your changes when that tag is fresh.

## Adding a CI job to `test/run.sh`

A new `run_acceptance_*` function must call `build_weaviate_test_image` before running its tests.
The helper builds `weaviate/test-server` with the race detector and exports `TEST_WEAVIATE_IMAGE`,
and returns early when the variable is already set, so jobs that run several groups build once:

```bash
function run_acceptance_my_new_tests() {
  build_weaviate_test_image
  run_aof_group "my-tests" test/acceptance/my_tests
}
```
