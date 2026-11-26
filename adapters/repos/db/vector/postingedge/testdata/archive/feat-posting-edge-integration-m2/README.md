# The bench harness of `feat/posting-edge-integration-m2`, archived

The core of the index (`feat/posting-edge-integration-m2`, PR #14041) carried
its bench harness, the files under the `postingedgebench` build tag, until
2026-11-26, when it left that branch to keep the core PR small. Every file of
it is on this branch at its path, or in this branch's history, except these
two, which use code only the integration branches have (the user config of
the index, the bucket names of its centroid codes). They are kept here at
their paths under this directory, where Go does not build them.

- `adapters/repos/db/vector/postingedge/bench_centroid_codes_test.go`:
  `TestDiskCentroidCodes`, the measure of the centroids as RQ8, CRQ4 and CRQ1
  codes.
- `adapters/repos/db/vector/postingedge/bench_integration_test.go`: the
  measures of the index a shard opens (integration milestones 1 and 2).

The harness lives on in `feat/posting-edge-integration`, where these two
build.
