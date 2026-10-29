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
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestDocIDReuse_C1_SegmentSecondaryDuplicate is a RECONSTRUCTION (the verbatim
// audit file audit_c1_repro_test.go was lost) of finding C1's compaction route,
// at bucket/segment isolation.
//
// C1: docID reuse makes two LIVE primary rows carry the same secondary key X
// (a reused docID held as a secondary key by both the stale and the new owner
// before the stale row is removed). The segment's secondary index holds both
// entries for X with no dedupe, so GetBySecondary(X) resolves to one of them
// placement-arbitrarily, violating LSM Replace's newest-wins contract. The
// audit was explicit that this two-live-rows form, not tombstone+live, is the
// deterministic trigger ("secondaryWrong never fires — needs two live rows
// sharing X"); a tombstone+recreate variant resolves correctly via
// newest-segment-wins and was confirmed GREEN during reconstruction.
//
// Reconstruction notes vs the original: this drives the public Bucket API
// (Put/FlushAndSwitch/compactOnce/GetBySecondary) rather than the original's
// per-segment secondary-BST diagnostic. It loops 100 independent shared-X pairs
// through two segments + a full compaction to surface the placement-arbitrary
// winner. A single older-row win fails the test.
//
// Verified RED at tests/docid-reuse-regression head (dec791e549): 48/100 lost,
// which matches the loss mode of 48 the audit recorded for Jero's complementary
// C1 suite (38-63/100) — strong evidence the reconstruction hits the original
// mechanism. If this ever runs GREEN, treat it as SUSPECT before concluding C1
// is fixed.
func TestDocIDReuse_C1_SegmentSecondaryDuplicate(t *testing.T) {
	t.Skip("pins finding C1 (segment secondary duplicate under docID reuse); no fix yet — see docid-reuse-jero-audit. RECONSTRUCTION of lost audit_c1_repro_test.go. Verified RED at dec791e549: 48/100 shared secondary keys resolved to the older live row (matches the audit's recorded mode of 48).")

	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithKeepTombstones(true),
		WithPread(true),
		WithMinMMapSize(0),
		WithUseBloomFilter(false),
		WithDisableCompaction(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	const triples = 100
	key := func(id uint64) []byte { return fmt.Appendf(nil, "object-%d", id) }
	uuid := func(i int) []byte { return fmt.Appendf(nil, "uuid-%08d", i) }

	// Two LIVE primary rows share the same secondary key X — the state docID
	// reuse makes reachable (a reused docID X carried as a secondary key by
	// both the stale and the new owner before the stale row is removed). The
	// audit was explicit that this two-live-rows form, not tombstone+live, is
	// C1's deterministic trigger ("secondaryWrong never fires — needs two live
	// rows sharing X"). LSM Replace semantics are newest-wins, so the second
	// writer ("new") must win GetBySecondary; if the older row ever wins, the
	// segment secondary index violated newest-wins (C1).
	for i := 0; i < triples; i++ {
		oldPrimary := key(uint64(2*i + 1))
		require.NoError(t, b.Put(oldPrimary, []byte("old"), WithSecondaryKey(0, uuid(i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	for i := 0; i < triples; i++ {
		newPrimary := key(uint64(2*i + 2))
		require.NoError(t, b.Put(newPrimary, []byte("new"), WithSecondaryKey(0, uuid(i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	for {
		compacted, err := b.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	var losses int
	for i := 0; i < triples; i++ {
		got, err := b.GetBySecondary(ctx, 0, uuid(i))
		if err != nil || string(got) != "new" {
			losses++
		}
	}
	require.Zerof(t, losses,
		"%d/%d shared secondary keys resolved to the older live row, not newest-wins — segment secondary BST duplicate (C1)",
		losses, triples)
}
