# Testing

## Unit
Feel free to run any unit test that doesn't requires special build tags. When
adding unit tests, aim to exhaust all possibilities. For example if dataTypes
are involved, make sure all dataTypes are covered, etc.

## Integration 
Integration tests have special build tags (`integrationTest`). They are perfect
for making sure multiple components work together. You may run them
yourselfs by setting the appropriate build tags for a single package at the
time. Do not run them across the entire repo (takes too long). Ask the user to
do that.

## E2e
**Package Structure**
E2e test have considerable runtime. Be smart about them. When you add new
tests, prefer creating a separate package. Only extend existing packages when
the tests clearly fit with an existing package. 

**Test Dependencies**
The repo uses two styles of e2e tests: And old one where the test relies on a
Weaviate instance running. And a modern one where all dependencies are started
using test containers. Always prefer the new one, especially for tests that
require a multi-node cluster. But also for single-node tests.

**Running e2e tests**
Don't ever run the full e2e suite. It takes too long, and it will eventually
run CI or ask the user. Do run specific e2e test packages that you added or
changed.

## Linter

Always validate that the linters passes at the end of a task:
* golangci-lint
* ./tools/linter_go_routines.sh

# Allocation Efficiency

When your code path resembles a potential hot path, please pay a lot of
attention to avoid unnecessary allocations. One specific gotcha is the use of
binary.Read which allocates a lot. A better alternative is the
github.com/weaviate/weaviate/usecases/byteops package.
