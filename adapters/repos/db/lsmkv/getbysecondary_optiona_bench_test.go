//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"math/bits"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketGetBySecondaryOptionASortDelta records the sort-order (Option A)
// benchmark delta against the serial read-op baseline. It resolves the same 500 doc
// IDs in two orders (caller order = baseline, and on-disk secondary-key order: sort
// by bits.ReverseBytes64) and reports per-phase read-op counts and wall time for both.
//
// Finding it pins: the read-op COUNT is order-INDEPENDENT. segmentindex.DiskTree.Get
// restarts from the root on every call, so k sorted Gets touch exactly the same
// nodes as k unsorted Gets. Sorting therefore changes neither index-descent nor value
// read counts; its win is CPU/page-cache temporal locality (upper tree nodes stay hot
// across a worker's index-adjacent range), a wall-time effect this counting harness is
// explicitly designed NOT to measure and which in-heap reconstructed trees cannot
// reproduce (no cold mmap page cache). The real wall-time win is measured at cold-disk
// scale, tracked as a load-test follow-up.
func TestBucketGetBySecondaryOptionASortDelta(t *testing.T) {
	shape := scaledShape()
	_, dir, universe := buildReadOpsBucket(t, shape)
	segs := reconstructSegments(t, dir)

	rng := rand.New(rand.NewSource(shape.seed))
	keys := pickRandom(rng, universe, shape.numResolve)

	baseline, baselineWall := resolveAllCounting(t, segs, keys) // caller order

	sorted := append([]uint64(nil), keys...)
	sort.SliceStable(sorted, func(a, b int) bool {
		return bits.ReverseBytes64(sorted[a]) < bits.ReverseBytes64(sorted[b])
	})
	optionA, optionAWall := resolveAllCounting(t, segs, sorted) // on-disk key order

	t.Logf("Option-A benchmark delta (%s shape, %d keys, %d segments):", shape.name, len(keys), shape.segments)
	t.Logf("  baseline (caller order):  index_node_reads=%d value_read_ops=%d recheck_node_reads=%d wall=%s",
		baseline.indexNodeReads, baseline.valueReadOps, baseline.recheckNodeReads, baselineWall)
	t.Logf("  option-A (sorted order):  index_node_reads=%d value_read_ops=%d recheck_node_reads=%d wall=%s",
		optionA.indexNodeReads, optionA.valueReadOps, optionA.recheckNodeReads, optionAWall)
	t.Logf("  read-op delta: index=%d value=%d recheck=%d (expected 0: DiskTree.Get restarts from root; win is cache-locality wall-time)",
		optionA.indexNodeReads-baseline.indexNodeReads,
		optionA.valueReadOps-baseline.valueReadOps,
		optionA.recheckNodeReads-baseline.recheckNodeReads)

	// core assertion: sorting does not change the read-op count at any phase.
	assert.Equal(t, baseline.indexNodeReads, optionA.indexNodeReads,
		"index-descent read-op count must be unchanged by sort order")
	assert.Equal(t, baseline.valueReadOps, optionA.valueReadOps,
		"value read-op count must be unchanged by sort order")
	assert.Equal(t, baseline.recheckNodeReads, optionA.recheckNodeReads,
		"recheck read-op count must be unchanged by sort order")

	// Sanity: the baseline is non-trivial (the harness actually descended and read).
	require.Positive(t, baseline.indexNodeReads)
	require.Positive(t, baseline.valueReadOps)
}

// resolveAllCounting resolves every key against segs (newest->oldest) via the
// read-op-counting resolver, accumulating per-phase read-ops and total wall time.
func resolveAllCounting(t *testing.T, segs []reconstructedSegment, ids []uint64) (phaseCounters, time.Duration) {
	t.Helper()
	var total phaseCounters
	start := time.Now()
	for _, id := range ids {
		_, c, err := resolveSerialCounting(t, segs, encodeDocID(id))
		require.NoError(t, err)
		total.add(c)
	}
	return total, time.Since(start)
}
