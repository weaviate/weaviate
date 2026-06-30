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

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCursorMap_InvertedReusable_DeferredAdvance drives a Next→consume→Next
// loop over a multi-segment inverted bucket whose disk-resident segments use
// the reusable cursor (segmentCursorInvertedReusable). It asserts:
//
//  1. Each iteration's returned key/values match the expected merge across
//     segments — i.e. mergeMapPairs ran against unclobbered inner-cursor
//     buffers (the deferred-advance contract added in PR #11450).
//  2. Between consecutive Next calls — the "consume" window — the previously
//     returned key remains bit-identical (CursorMap copies the key before
//     return so it survives subsequent deferred advances).
//  3. After all Next calls, the snapshots captured during the consume window
//     still match the expected data for that iteration.
func TestCursorMap_InvertedReusable_DeferredAdvance(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9)

	// Two segments. Each row's logical value list is the union of segment1
	// and segment2 entries for that row, so the merge path is exercised.
	type rowSpec struct {
		key      []byte
		seg1Tfs  []float32 // doc IDs 0..len-1 written in segment 1
		seg2Tfs  []float32 // doc IDs base..base+len-1 written in segment 2
		seg2Base uint64
	}
	rows := []rowSpec{
		// Small row: single value per segment -> small-value (ENCODE_AS_FULL_BYTES) path.
		{key: []byte("aaa-small"), seg1Tfs: []float32{1}, seg2Tfs: []float32{2}, seg2Base: 1000},
		// Mid-sized row: spans one block in each segment.
		{key: []byte("bbb-onelock"), seg1Tfs: floatRange(50, 1), seg2Tfs: floatRange(50, 1), seg2Base: 1000},
		// Large row: forces multi-block encoding so the reusable
		// block-decode buffers are exercised across iterations.
		{key: []byte("ccc-multi"), seg1Tfs: floatRange(300, 1), seg2Tfs: floatRange(200, 1), seg2Base: 10000},
		// Row only in segment 1 (no merge for this key).
		{key: []byte("ddd-seg1only"), seg1Tfs: floatRange(10, 1), seg2Tfs: nil, seg2Base: 0},
		// Row only in segment 2.
		{key: []byte("eee-seg2only"), seg1Tfs: nil, seg2Tfs: floatRange(10, 1), seg2Base: 2000},
	}

	// Build, insert, and flush segment 1.
	for _, r := range rows {
		for docId, tf := range r.seg1Tfs {
			require.NoError(t, bucket.MapSet(r.key,
				NewMapPairFromDocIdAndTf(uint64(docId), tf, tf, false)))
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// Build, insert, and flush segment 2.
	for _, r := range rows {
		for i, tf := range r.seg2Tfs {
			require.NoError(t, bucket.MapSet(r.key,
				NewMapPairFromDocIdAndTf(r.seg2Base+uint64(i), tf, tf, false)))
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// Build the expected (key -> sorted-by-docId []docId) result for the scan.
	type expectedRow struct {
		key    []byte
		docIds []uint64
	}
	expected := make([]expectedRow, 0, len(rows))
	for _, r := range rows {
		if r.seg1Tfs == nil && r.seg2Tfs == nil {
			continue
		}
		var docIds []uint64
		for d := range r.seg1Tfs {
			docIds = append(docIds, uint64(d))
		}
		for i := range r.seg2Tfs {
			docIds = append(docIds, r.seg2Base+uint64(i))
		}
		sort.Slice(docIds, func(i, j int) bool { return docIds[i] < docIds[j] })
		expected = append(expected, expectedRow{
			key:    append([]byte(nil), r.key...),
			docIds: docIds,
		})
	}
	sort.Slice(expected, func(i, j int) bool {
		return bytes.Compare(expected[i].key, expected[j].key) < 0
	})

	c, err := bucket.MapCursor()
	require.NoError(t, err)
	defer c.Close()

	type snapshot struct {
		returnedKey    []byte // the slice as returned by First/Next (alias check)
		snapshotKey    []byte // a deep copy captured at consume time
		snapshotDocIds []uint64
	}
	var snapshots []snapshot

	var prevKeyAlias []byte // slice from the previous iteration (not a copy)
	var prevKeySnap []byte  // deep copy snapshot from the previous iteration

	idx := 0
	for k, vs := c.First(ctx); k != nil; k, vs = c.Next(ctx) {
		require.Lessf(t, idx, len(expected),
			"cursor returned more rows than expected at idx=%d (key=%q)", idx, k)

		exp := expected[idx]

		// Assertion (1): the data returned by this call matches expectations.
		assert.Equalf(t, exp.key, k,
			"key mismatch at idx=%d", idx)
		require.Equalf(t, len(exp.docIds), len(vs),
			"value count mismatch at idx=%d (key=%q)", idx, k)
		gotDocIds := make([]uint64, len(vs))
		for i, mp := range vs {
			gotDocIds[i] = binary.BigEndian.Uint64(mp.Key)
		}
		assert.Equalf(t, exp.docIds, gotDocIds,
			"docId list mismatch at idx=%d (key=%q)", idx, k)

		// Assertion (2): the PREVIOUS iteration's key (as a live slice
		// alias) still equals the snapshot we took for it. The deferred
		// advance triggered at the start of *this* Next must not have
		// mutated the previously returned key — CursorMap copies the key
		// before returning, so it survives.
		if idx > 0 {
			assert.Truef(t, bytes.Equal(prevKeyAlias, prevKeySnap),
				"previous iteration's key was mutated by the deferred advance at idx=%d", idx)
		}

		// Capture for assertion (3) and for the next iteration's
		// previous-key check.
		snap := snapshot{
			returnedKey:    k,
			snapshotKey:    append([]byte(nil), k...),
			snapshotDocIds: append([]uint64(nil), gotDocIds...),
		}
		snapshots = append(snapshots, snap)
		prevKeyAlias = k
		prevKeySnap = snap.snapshotKey

		idx++
	}
	require.Equalf(t, len(expected), idx, "cursor returned fewer rows than expected")

	// Assertion (3): after the full scan, every snapshot taken at
	// consume-time must still match the expected data for that iteration.
	// Plus, each snapshot's returnedKey alias must still equal its own
	// snapshotKey (the key slice is independent of reusable buffers).
	for i, snap := range snapshots {
		assert.Equalf(t, expected[i].key, snap.snapshotKey,
			"snapshot key at idx=%d does not match expected", i)
		assert.Equalf(t, expected[i].docIds, snap.snapshotDocIds,
			"snapshot docId list at idx=%d does not match expected", i)
		assert.Truef(t, bytes.Equal(snap.returnedKey, snap.snapshotKey),
			"the returned key slice at idx=%d was mutated post-return", i)
	}
}

func floatRange(n int, base float32) []float32 {
	out := make([]float32, n)
	for i := range out {
		out[i] = base + float32(i)
	}
	return out
}

// TestCursorMap_InvertedReusable_ConsumeBetweenCalls is a focused variant:
// it captures BOTH key and value byte slices returned by Next, performs a
// "consume" step that reads every byte, then advances the cursor and
// re-reads the captured slices. This verifies that during the consume
// window (between Next calls), the returned data is stable.
//
// The values' underlying bytes ARE expected to be invalidated by the next
// Next call (they alias the inner cursor's reusable mapPairBuf/kvArena),
// so we only assert stability WITHIN a consume window, not across it.
func TestCursorMap_InvertedReusable_ConsumeBetweenCalls(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9)

	rowKeys := [][]byte{
		[]byte("row-a"),
		[]byte("row-b"),
		[]byte("row-c"),
	}
	// Each row gets enough values to land in the block-encoded path.
	const valuesPerRow = 200
	for rIdx, key := range rowKeys {
		for d := 0; d < valuesPerRow; d++ {
			docId := uint64(rIdx)*1000 + uint64(d)
			tf := float32(d + 1)
			require.NoError(t, bucket.MapSet(key,
				NewMapPairFromDocIdAndTf(docId, tf, tf, false)))
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	c, err := bucket.MapCursor()
	require.NoError(t, err)
	defer c.Close()

	k1, v1 := c.First(ctx)
	require.NotNil(t, k1)
	require.Len(t, v1, valuesPerRow)

	// Consume step: read every byte of every returned value, capturing
	// docIds. The slices must remain readable for the duration of the
	// consume.
	consumed1 := make([]uint64, len(v1))
	for i, mp := range v1 {
		require.Lenf(t, mp.Key, 8, "key[%d]", i)
		require.Lenf(t, mp.Value, 8, "value[%d]", i)
		consumed1[i] = binary.BigEndian.Uint64(mp.Key)
	}
	keySnapshot1 := append([]byte(nil), k1...)

	// Now advance. The deferred advance from First runs at the start of
	// Next, invalidating the inner cursor's reusable buffers (which back
	// v1's slices). However, k1 was copied by CursorMap and must remain
	// intact.
	k2, v2 := c.Next(ctx)
	require.NotNil(t, k2)
	require.Len(t, v2, valuesPerRow)

	assert.Truef(t, bytes.Equal(k1, keySnapshot1),
		"k1 must be intact across the Next call (CursorMap copies returned keys)")

	// k2 must be a fresh, independent slice — not the same backing array
	// as k1.
	if len(k1) > 0 && len(k2) > 0 {
		assert.NotSamef(t, &k1[0], &k2[0],
			"k1 and k2 must not alias the same underlying array")
	}

	// And v2 must contain the second row's docIds (i.e. the merge ran
	// against unclobbered buffers).
	consumed2 := make([]uint64, len(v2))
	for i, mp := range v2 {
		consumed2[i] = binary.BigEndian.Uint64(mp.Key)
	}
	require.Equal(t, valuesPerRow, len(consumed2))
	// First docId of the second row equals 1000 (rIdx=1, d=0) by construction.
	assert.Equal(t, uint64(1000), consumed2[0],
		"v2 must contain the second row's docIds — wrong data here would indicate the merge saw clobbered buffers")

	// And the consumed1 we captured during the consume window of First
	// still matches the first row (rIdx=0, docIds 0..valuesPerRow-1).
	for i, got := range consumed1 {
		require.Equal(t, uint64(i), got, fmt.Sprintf("consumed1[%d]", i))
	}
}
