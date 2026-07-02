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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// flushPropLenSegmentQA flushes one inverted segment whose stored property
// lengths are exactly `want` (docID -> length; a length of 0 is stored verbatim,
// which forces the pairs layout) and returns the on-disk segment.
func flushPropLenSegmentQA(t *testing.T, want map[uint64]uint32) (Segment, func()) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	key := []byte("term")
	for docID, l := range want {
		require.NoError(t, bucket.MapSet(key, NewMapPairFromDocIdAndTf(docID, 1, float32(l), false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	view := bucket.GetConsistentView()
	require.Len(t, view.Disk, 1)
	return view.Disk[0], func() { view.ReleaseView(); bucket.Shutdown(ctx) }
}

// TestProplen270_BoundaryMaxUint32 pins the load-side uint32/uint64 gate at the
// exact boundary. SC's table only covers well-under (4990) and well-over (1<<33);
// the off-by-one at math.MaxUint32 is where a `<` vs `<=` slip would hide.
func TestProplen270_BoundaryMaxUint32(t *testing.T) {
	cases := []struct {
		name       string
		maxID      uint64
		wantNarrow bool
	}{
		{"max_eq_maxuint32_is_narrow", math.MaxUint32, true},           // 2^32-1 fits uint32 (inclusive)
		{"max_eq_maxuint32_plus_1_is_wide", math.MaxUint32 + 1, false}, // 2^32 must stay uint64
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// a few low ids + the boundary id -> huge span forces pairs (not dense)
			want := map[uint64]uint32{0: 11, 1: 22, 2: 33, tc.maxID: 44}
			seg, cleanup := flushPropLenSegmentQA(t, want)
			defer cleanup()

			plView, err := seg.propLengthsView()
			require.NoError(t, err)
			assert.Nil(t, plView.dense, "huge span must not select dense")
			if tc.wantNarrow {
				assert.NotNil(t, plView.ids32, "max docID == MaxUint32 must select uint32 ids")
				assert.Nil(t, plView.ids, "narrow: no uint64 pairs")
			} else {
				assert.NotNil(t, plView.ids, "max docID == MaxUint32+1 must keep uint64 ids")
				assert.Nil(t, plView.ids32, "wide: no uint32 pairs")
			}
			// exact lengths incl. the boundary id (a uint32 truncation of MaxUint32+1
			// would alias to 0 and return a wrong length here)
			for id, l := range want {
				assert.Equal(t, l, plView.get(id), "view docID %d", id)
			}
			gotMap, err := seg.getPropertyLengths()
			require.NoError(t, err)
			assert.Equal(t, want, gotMap, "map reconstruction exact at the boundary")
		})
	}
}

// TestProplen270_ZeroLengthHighDocIDStaysWide is the regression test for the trap
// the issue calls out: the dense-gate min/max loop early-stops at the first
// zero-length doc, so its max is NOT the segment's true max. The uint32 gate must
// use sortPropLenPairs' full-scan max instead. Here a stored 0-length forces the
// pairs layout AND the true max is > MaxUint32 — the segment MUST keep uint64 ids
// and must not truncate the high docID. If the gate ever regresses to the dense
// gate's early-stopped max, this segment would be misclassified as uint32 and the
// 1<<33 length would be silently lost.
func TestProplen270_ZeroLengthHighDocIDStaysWide(t *testing.T) {
	const highID = uint64(1) << 33 // > MaxUint32
	want := map[uint64]uint32{
		3:      0, // zero-length: forces pairs AND is where the dense gate early-stops
		7:      5,
		50:     9,
		highID: 12, // true max, far beyond uint32
	}
	seg, cleanup := flushPropLenSegmentQA(t, want)
	defer cleanup()

	plView, err := seg.propLengthsView()
	require.NoError(t, err)
	assert.Nil(t, plView.dense, "a stored 0-length must force pairs")
	assert.NotNil(t, plView.ids, "true max > MaxUint32 must keep uint64 ids despite the early zero-length")
	assert.Nil(t, plView.ids32, "must NOT narrow to uint32 — the high docID would be truncated")
	assert.Equal(t, uint32(12), plView.get(highID), "high docID length must be exact, not truncated")
	assert.Equal(t, uint32(0), plView.get(3), "stored zero-length resolves to 0")
	assert.Equal(t, uint32(5), plView.get(7))

	gotMap, err := seg.getPropertyLengths()
	require.NoError(t, err)
	// The pairs path of getPropertyLengths reconstructs every stored entry verbatim,
	// including the explicit 0-length (only the dense path drops zeros — and dense is
	// never chosen when a 0 exists, so the two paths stay consistent for real data).
	// The point of this test: the >uint32 docID is present and exact (not truncated).
	assert.Equal(t, want, gotMap, "map reconstruction includes every stored docID, exact, incl. the >uint32 one")
}
