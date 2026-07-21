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

// TestSegment_CorruptIndexStart_PastSegmentLength pins
// weaviate/weaviate#12280 shape 3: QA Claude's real-segment repro
// (622914-byte objects segment, IndexStart=722914) - an IndexStart past the
// file's actual length must fail loudly at open, not panic slicing
// contents[IndexStart:].
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
// weaviate/weaviate#12280 shape 4 - the serious one: QA Claude's real-segment
// repro (a replace bucket, no secondary index, IndexStart corrupted to an
// in-bounds mid-region offset). Pre-fix this opened with NO error and Get
// silently returned empty for real data. Post-fix, reopening must fail
// loudly at open (the root node's Start/End land outside the data region) -
// never a silent empty read for data that is actually intact on disk.
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
		"precondition: the corrupt offset must be in-bounds for this to be shape 4, not shape 3")
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

	// The exact inner reason varies with what garbage bytes land at the
	// corrupt offset (this segment's happened to decode to an oversized
	// node key length, caught by DiskTree's own pre-existing bounds check;
	// a different byte layout could instead trip the root Start/End check
	// this fix adds) - both are legitimate, loud rejections from the same
	// new choke point (validating the primary index at open), which is
	// what actually matters here: never a panic, never a silent empty read.
	require.Contains(t, err.Error(), "validate primary index",
		"the loud failure must come from the new index-validation choke point, not an unrelated error")
}

// TestSegment_CorruptSecondaryIndicesCount_PastSegmentLength pins
// weaviate/weaviate#12280 shape 5: a corrupt-large SecondaryIndices count
// pushes the secondary offset table past the segment's actual length; this
// must fail loudly at open, not panic slicing the offset table.
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
