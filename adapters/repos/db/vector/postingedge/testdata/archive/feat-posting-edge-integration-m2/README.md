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

## The tests shrunk to fit CI (2026-11-26)

The tests of the package were shrunk so that the package fits CI's limits
(`-race` with atomic coverage, ten minutes a package): see
`feat/posting-edge-integration-m2` from `c78c016d3b`. The tests that only
mean something at scale, or whose subject is the simulators of the
experiments, left the package. Their originals, renamed with an `AtScale`
or `EveryPoint` suffix where a small version stayed, are here, under the
`postingedgebench` build tag. They build only in the package of
`feat/posting-edge-integration-m2` from `b5222a9252` on, whose test helpers
they use, and belong in the bench harness of `feat/posting-edge-integration`
once it merges that commit.

- `grow_journal_harness_test.go`, `grow_layout_harness_test.go`,
  `grow_refresh_harness_test.go`, `grow_clean_harness_test.go`: the crash
  sweep at every point, and the layout, refresh and clean equivalences at
  scale.
- `grow_equiv_harness_test.go`, `grow_batch_harness_test.go`,
  `grow_halo_harness_test.go`: the grow equivalences, batches and halos at
  scale.
- `persistent_centroids_harness_test.go`, `persistent_journal_harness_test.go`:
  the centroid memory and the crash sweep of the index a shard opens.
- `search_harness_test.go`, `stream_insert_harness_test.go`,
  `stream_counts_harness_test.go`: recall and work measures, and the
  streaming experiments of v2.x.
- `delete_v62_harness_test.go`, `disk_equiv_harness_test.go`: the delete
  cells of v6.1 and v6.2, and the disk equivalences of every cell.
