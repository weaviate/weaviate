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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// findSingleDBFile returns the one .db segment file in dir, failing the
// test if there isn't exactly one.
func findSingleDBFile(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var dbFiles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			dbFiles = append(dbFiles, filepath.Join(dir, e.Name()))
		}
	}
	require.Len(t, dbFiles, 1, "test needs exactly one segment file on disk")
	return dbFiles[0]
}

// setIndexStart overwrites the 8-byte little-endian IndexStart field at
// byte offset 8 of a segment header (level:2, version:2, secondaryIndices:2,
// strategy:2, indexStart:8) with the given value, size-preserving.
func setIndexStart(t *testing.T, path string, value uint64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	_, err = f.WriteAt(buf, 8)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestSegment_CorruptIndexStart_PastSegmentLength pins weaviate/weaviate#12280:
// an IndexStart past the file's actual length must fail loudly at open, not
// panic slicing contents[IndexStart:].
func TestSegment_CorruptIndexStart_PastSegmentLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucket(t, ctx, dir, StrategyReplace)

	require.NoError(t, b.Put([]byte("key-000"), []byte("val-000")))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	target := findSingleDBFile(t, dir)
	info, err := os.Stat(target)
	require.NoError(t, err)
	fileSize := uint64(info.Size())

	setIndexStart(t, target, fileSize+100_000)

	sizeAfter, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, info.Size(), sizeAfter.Size(), "corruption must be size-preserving")

	logger, _ := test.NewNullLogger()
	opts := []BucketOption{WithStrategy(StrategyReplace)}
	require.NotPanics(t, func() {
		_, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	})
	require.Error(t, err, "reopening with IndexStart past the segment end must fail loudly, not panic")
	require.Contains(t, err.Error(), "corrupt segment header")
}

// TestSegment_CorruptIndexStart_MidRegion_NoSilentEmptyRead pins
// weaviate/weaviate#12280: an in-bounds but corrupt IndexStart must fail
// loudly at open, never silently return an empty read for data that is
// actually intact on disk.
func TestSegment_CorruptIndexStart_MidRegion_NoSilentEmptyRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucket(t, ctx, dir, StrategyReplace)

	require.NoError(t, b.Put([]byte("key-000"), []byte("val-000")))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	target := findSingleDBFile(t, dir)
	info, err := os.Stat(target)
	require.NoError(t, err)

	const corruptOffset = 48
	require.Less(t, uint64(corruptOffset), uint64(info.Size()),
		"precondition: the corrupt offset must be in-bounds")
	setIndexStart(t, target, corruptOffset)

	sizeAfter, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, info.Size(), sizeAfter.Size(), "corruption must be size-preserving")

	logger, _ := test.NewNullLogger()
	opts := []BucketOption{WithStrategy(StrategyReplace)}
	var reopened *Bucket
	require.NotPanics(t, func() {
		reopened, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	})

	if err == nil {
		// If the bucket somehow still opens, it must never silently lose the
		// real value - the one outcome this test exists to forbid.
		defer reopened.Shutdown(ctx)
		got, getErr := reopened.Get([]byte("key-000"))
		require.NoError(t, getErr)
		require.Equal(t, []byte("val-000"), got,
			"a corrupt in-bounds IndexStart must never silently serve an empty/wrong read for real data")
		return
	}

	// The exact inner reason depends on the garbage bytes at the corrupt
	// offset, but it must come from the primary-index validation choke point.
	require.Contains(t, err.Error(), "validate primary index",
		"the loud failure must come from the new index-validation choke point, not an unrelated error")
}

// TestSegment_CorruptSecondaryIndicesCount_PastSegmentLength pins
// weaviate/weaviate#12280: a corrupt-large SecondaryIndices count pushing the
// secondary offset table past the segment's actual length must fail loudly
// at open, not panic slicing the offset table.
func TestSegment_CorruptSecondaryIndicesCount_PastSegmentLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)

	require.NoError(t, b.Put([]byte("key-000"), []byte("val-000"),
		WithSecondaryKey(0, []byte("secondary-000"))))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	target := findSingleDBFile(t, dir)
	info, err := os.Stat(target)
	require.NoError(t, err)

	// SecondaryIndices lives at header byte offset 4 (uint16, after level and
	// version), independent of IndexStart at offset 8.
	f, err := os.OpenFile(target, os.O_RDWR, 0o644)
	require.NoError(t, err)
	secBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(secBuf, 65535)
	_, err = f.WriteAt(secBuf, 4)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	sizeAfter, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, info.Size(), sizeAfter.Size(), "corruption must be size-preserving")

	opts2 := []BucketOption{WithStrategy(StrategyReplace), WithSecondaryIndices(1)}
	require.NotPanics(t, func() {
		_, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts2...)
	})
	require.Error(t, err, "reopening with a corrupt-large SecondaryIndices count must fail loudly, not panic")
	require.Contains(t, err.Error(), "corrupt segment header")
}

// TestSegment_CorruptIndexStart_BoundaryAroundSegmentLength pins
// weaviate/weaviate#12280 F1 (QA Claude round 2): ValidateIndexBounds uses
// '>' against the segment's actual length, so IndexStart == length passes
// it, leaving an empty primary index that ValidateRootInBounds's
// len(t.data)==0 short-circuit treats as a legitimate empty segment -
// silent data loss one boundary past the already-fixed shape 4. Sweeps all
// three neighbors of the boundary on a real flushed sec=0 segment.
func TestSegment_CorruptIndexStart_BoundaryAroundSegmentLength(t *testing.T) {
	tests := []struct {
		name          string
		offset        int64 // relative to the segment's actual length
		wantErrSubstr string
	}{
		// A 1-byte "index" is too short to hold even one node's fixed
		// overhead, so this is caught by DiskTree's own node-read bounds
		// check (via ValidateRootInBounds), not ValidateIndexBounds/
		// ValidateNonEmptyIndex - a different choke point, already correct
		// before this round's F1 fix.
		{name: "one byte before segment end: rejected (truncated index)", offset: -1, wantErrSubstr: "validate primary index"},
		{name: "exactly at segment end: rejected (empty index, data present)", offset: 0, wantErrSubstr: "corrupt segment header"},
		{name: "one byte past segment end: rejected (past segment end)", offset: 1, wantErrSubstr: "corrupt segment header"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			b := createTestBucket(t, ctx, dir, StrategyReplace)

			require.NoError(t, b.Put([]byte("key-000"), []byte("val-000")))
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			target := findSingleDBFile(t, dir)
			info, err := os.Stat(target)
			require.NoError(t, err)
			fileSize := info.Size()

			setIndexStart(t, target, uint64(fileSize+tt.offset))

			sizeAfter, err := os.Stat(target)
			require.NoError(t, err)
			require.Equal(t, fileSize, sizeAfter.Size(), "corruption must be size-preserving")

			logger, _ := test.NewNullLogger()
			opts := []BucketOption{WithStrategy(StrategyReplace)}
			require.NotPanics(t, func() {
				_, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			})

			require.Error(t, err, "corrupt IndexStart at this boundary must fail loudly, not open silently empty")
			require.Contains(t, err.Error(), tt.wantErrSubstr)
		})
	}
}

// TestSegment_CorruptIndexStart_Inverted_RealData_FailsLoud pins the QA
// Claude round-2 finding on weaviate/weaviate#12280: the StrategyInverted
// exclusion from ValidateNonEmptyIndex must not be a blanket exclusion for
// the whole strategy. A normal (non-tombstone) Inverted flush's primary
// index IS load-bearing for real term lookups, so the same F1 corruption
// (IndexStart == the segment's actual length, emptying the primary index)
// must still fail loudly here, not open silently and serve MapList as if
// the term had no results. QA's exact repro: real MapSet data, no deletes.
func TestSegment_CorruptIndexStart_Inverted_RealData_FailsLoud(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{WithStrategy(StrategyInverted)}

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)

	require.NoError(t, b.MapSet([]byte("realword"), NewMapPairFromDocIdAndTf(42, 1, 1, false)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	target := findSingleDBFile(t, dir)
	info, err := os.Stat(target)
	require.NoError(t, err)
	fileSize := info.Size()

	setIndexStart(t, target, uint64(fileSize))

	sizeAfter, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, fileSize, sizeAfter.Size(), "corruption must be size-preserving")

	var reopened *Bucket
	require.NotPanics(t, func() {
		reopened, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	})

	if err == nil {
		// If the bucket somehow still opens, it must never silently lose the
		// real entry - the one outcome this test exists to forbid.
		defer reopened.Shutdown(ctx)
		res, listErr := reopened.MapList(ctx, []byte("realword"))
		require.NoError(t, listErr)
		require.Len(t, res, 1, "a corrupt in-bounds IndexStart must never silently serve an empty result for a real (non-tombstone) inverted entry")
		return
	}
	require.Contains(t, err.Error(), "primary index is empty even though",
		"the loud failure must come from the propLengthCount discriminant, not an unrelated error")
}

// TestSegment_Inverted_AllTombstoneFlush_EmptyIndex_StillOpensCleanly is the
// no-false-rejection counterpart to
// TestSegment_CorruptIndexStart_Inverted_RealData_FailsLoud: a genuinely
// all-tombstone Inverted flush (a real key, later deleted, flushed to its
// own segment) legitimately has IndexStart > HeaderSize with a zero-entry
// primary index - that must still open cleanly, uncorrupted, per the
// propLengthCount discriminant's other branch.
func TestSegment_Inverted_AllTombstoneFlush_EmptyIndex_StillOpensCleanly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyInverted),
		WithMinWalThreshold(0), // force a flush per operation, isolating the tombstone-only flush into its own segment
	}

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)

	docID := uint64(1)
	require.NoError(t, b.MapSet([]byte("word1"), NewMapPairFromDocIdAndTf(docID, 1, 1, false)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	b, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docID)
	require.NoError(t, b.MapDeleteKey([]byte("word1"), key))
	require.NoError(t, b.Shutdown(ctx)) // threshold 0: flushes a second, all-tombstone segment

	var reopened *Bucket
	require.NotPanics(t, func() {
		reopened, err = NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	})
	require.NoError(t, err, "an uncorrupted, genuinely all-tombstone inverted segment must still open cleanly")
	defer reopened.Shutdown(ctx)

	res, err := reopened.MapList(ctx, []byte("word1"))
	require.NoError(t, err)
	require.Len(t, res, 0, "the tombstoned entry must resolve to no results, not an open error")
}
