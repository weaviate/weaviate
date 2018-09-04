# Acceptance tests

The `*_test.go` files in this directory make up the acceptance test suite of Weaviate.

The file names describe which part of the API of Weaviate they test.

## Acceptance helper.
The files in `test/acceptance/helper` are the support code to write acceptance tests against Weaviate
using the automatically generated client.

These files are extensively commented, so check them out if you want to find out how this works.

## Getting HTTP logs
By passing the `-debug-http` flag to the test suite (via `go test ./test/acceptance/ -args -debug-http`),
you'll be able to see the HTTP requests and responses made by the test suite to Weaviate.
