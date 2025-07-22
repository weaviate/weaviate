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
				WithWriteMetadata(tt.writeMetadata), WithUseBloomFilter(tt.bloomFilter), WithCalcCountNetAdditions(tt.cna), WithSecondaryIndices(uint16(secondaryIndexCount)))
			require.NoError(t, err)

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
		})
	}
}

func TestNoWriteIfBloomPresent(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithUseBloomFilter(true))
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
		WithUseBloomFilter(true), WithWriteMetadata(true))
	require.NoError(t, err)
	require.NoError(t, b2.Shutdown(ctx))

	fileTypes = countFileTypes(t, dirName)
	require.Len(t, fileTypes, 2)
	require.Equal(t, fileTypes[".db"], 1)
	require.Equal(t, fileTypes[".bloom"], 1)
}

func TestCorruptFile(t *testing.T) {
	dirName := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithWriteMetadata(true), WithUseBloomFilter(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(uint16(2)))
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
		WithWriteMetadata(true), WithUseBloomFilter(true), WithCalcCountNetAdditions(true), WithSecondaryIndices(uint16(2)))
	require.NoError(t, err)
	value, err := b2.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
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
