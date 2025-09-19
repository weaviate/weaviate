//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func TestMetadataNoWrites(t *testing.T) {
	ctx := context.Background()

	logger, _ := test.NewNullLogger()

	tests := []struct {
		name          string
		writeMetadata bool
		bloomFilter   bool
		cna           bool
		expectedFiles []string
	}{
		{name: "no meta at all1", writeMetadata: false, bloomFilter: false, cna: false, expectedFiles: []string{".db"}},
		{name: "no meta at all1", writeMetadata: true, bloomFilter: false, cna: false, expectedFiles: []string{".db"}},
		{name: "no meta but bloom", writeMetadata: false, bloomFilter: true, cna: false, expectedFiles: []string{".db", ".bloom"}},
		{name: "no meta but bloom+cna", writeMetadata: false, bloomFilter: true, cna: true, expectedFiles: []string{".db", ".bloom", ".cna"}},
		{name: "with meta and bloom+cna", writeMetadata: true, bloomFilter: true, cna: true, expectedFiles: []string{".db", ".metadata"}},
		{name: "with meta and cna", writeMetadata: true, bloomFilter: true, cna: true, expectedFiles: []string{".db", ".metadata"}},
		{name: "with meta and bloom", writeMetadata: true, bloomFilter: true, cna: true, expectedFiles: []string{".db", ".metadata"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()

			secondaryIndexCount := 2
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithWriteMetadata(tt.writeMetadata), WithUseBloomFilter(tt.bloomFilter), WithCalcCountNetAdditions(tt.cna), WithSecondaryIndices(uint16(secondaryIndexCount)), WithStrategy(StrategyReplace))
			require.NoError(t, err)
			require.NoError(t, b.Shutdown(ctx))

			require.NoError(t, b.Put([]byte("key"), []byte("value")))
			require.NoError(t, b.FlushMemtable())
			fileTypes := countFileTypes(t, dirName)
			require.Len(t, fileTypes, len(tt.expectedFiles))
			for _, expectedFile := range tt.expectedFiles {
				if expectedFile == ".bloom" {
					require.Equal(t, fileTypes[expectedFile], 1+secondaryIndexCount)
				} else {
					require.Equal(t, fileTypes[expectedFile], 1)
				}
			}

			// read again
			_, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithWriteMetadata(tt.writeMetadata), WithUseBloomFilter(tt.bloomFilter), WithCalcCountNetAdditions(tt.cna), WithSecondaryIndices(uint16(secondaryIndexCount)), WithStrategy(StrategyReplace))
			require.NoError(t, err)
		})
	}
}

func TestNoWriteIfBloomPresent(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("key"), []byte("value")))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.Shutdown(ctx))
	fileTypes := countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".bloom"], 1)

	// load with writeMetadata enabled, no metadata files should be written
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true), WithWriteMetadata(true), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b2.Shutdown(ctx))

	fileTypes = countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".bloom"], 1)
}

func TestCnaNoBloomPresent(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(false), WithWriteMetadata(true), WithCalcCountNetAdditions(true), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("key"), []byte("value")))
	require.NoError(t, b.FlushMemtable())
	fileTypes := countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".metadata"], 1)

	require.Equal(t, b.disk.segments[0].getSegment().getCountNetAdditions(), 1)
	require.NoError(t, b.Shutdown(ctx))
}

func TestSecondaryBloomNoCna(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true), WithWriteMetadata(true), WithCalcCountNetAdditions(false), WithSecondaryIndices(2), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("key"), []byte("value"), WithSecondaryKey(0, []byte("key0")), WithSecondaryKey(1, []byte("key1"))))
	require.NoError(t, b.FlushMemtable())
	fileTypes := countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".metadata"], 1)

	require.True(t, b.disk.segments[0].getSegment().secondaryBloomFilters[0].Test([]byte("key0")))
	require.False(t, b.disk.segments[0].getSegment().secondaryBloomFilters[0].Test([]byte("key1")))
	require.True(t, b.disk.segments[0].getSegment().secondaryBloomFilters[1].Test([]byte("key1")))
	require.False(t, b.disk.segments[0].getSegment().secondaryBloomFilters[1].Test([]byte("key0")))
	require.NoError(t, b.Shutdown(ctx))
}

func TestMarkMetadataAsDeleted(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true), WithWriteMetadata(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("key"), []byte("value"), WithSecondaryKey(0, []byte("key0")), WithSecondaryKey(1, []byte("key1"))))
	require.NoError(t, b.FlushMemtable())
	fileTypes := countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".metadata"], 1)

	sgment := b.disk.segments[0]
	require.NoError(t, sgment.markForDeletion())
	fileTypes = countFileTypes(t, dirName)
	require.Len(t, fileTypes, 1)
	require.Equal(t, fileTypes[DeleteMarkerSuffix], 2)
}

func TestDropImmediately(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true), WithWriteMetadata(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("key"), []byte("value"), WithSecondaryKey(0, []byte("key0")), WithSecondaryKey(1, []byte("key1"))))
	require.NoError(t, b.FlushMemtable())
	fileTypes := countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".metadata"], 1)

	lazySgment := b.disk.segments[0]
	sgment := lazySgment.getSegment()
	require.NoError(t, sgment.dropImmediately())
	fileTypes = countFileTypes(t, dirName)
	require.Len(t, fileTypes, 0)
}

func TestCorruptFile(t *testing.T) {
	dirName := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithWriteMetadata(true), WithUseBloomFilter(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(uint16(2)), WithStrategy(StrategyReplace))
	require.NoError(t, err)

	require.NoError(t, b.Put([]byte("key"), []byte("value")))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.Shutdown(ctx))

	files, err := os.ReadDir(dirName)
	require.NoError(t, err)
	fname, ok := findFileWithExt(files, ".metadata")
	require.True(t, ok)
	require.NoError(t, corruptBloomFileByTruncatingIt(path.Join(dirName, fname)))

	// broken file is ignored and correct one is recreated
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithWriteMetadata(true), WithUseBloomFilter(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(uint16(2)), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	value, err := b2.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestReadObjectCountFromMetadataFile(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "test.metadata")

	// checksum (4) + version (1) + primary bloom len (4) + cna len (4) + cna data (8)
	totalSize := 4 + 1 + 4 + 4 + 8

	data := make([]byte, totalSize)
	rw := byteops.NewReadWriter(data)

	rw.MoveBufferPositionForward(4) // leave space for checksum
	rw.WriteByte(0)                 // version
	rw.WriteUint32(0)               // primary bloom filter length (0 bytes)
	rw.WriteUint32(8)               // CNA length (8 bytes)
	rw.WriteUint64(42)              // CNA data

	// Write with checksum
	err := writeWithChecksum(rw, metadataPath, nil)
	require.NoError(t, err)

	// Test reading the metadata file
	count, err := ReadObjectCountFromMetadataFile(metadataPath)
	require.NoError(t, err)
	require.Equal(t, int64(42), count)
}

func countFileTypes(t *testing.T, path string) map[string]int {
	t.Helper()
	fileTypes := map[string]int{}

	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		fileTypes[filepath.Ext(entry.Name())] += 1
	}
	return fileTypes
}
