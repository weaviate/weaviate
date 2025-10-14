//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type bucketTest struct {
	name string
	f    func(context.Context, *testing.T, []BucketOption)
	opts []BucketOption
}

type bucketTests []bucketTest

func (tests bucketTests) run(ctx context.Context, t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("mmap", func(t *testing.T) {
				test.f(ctx, t, test.opts)
			})
			t.Run("pread", func(t *testing.T) {
				test.f(ctx, t, append([]BucketOption{WithPread(true)}, test.opts...))
			})
		})
	}
}

func TestBucket(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "bucket_WasDeleted_KeepTombstones",
			f:    bucket_WasDeleted_KeepTombstones,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithKeepTombstones(true),
			},
		},
		{
			name: "bucket_WasDeleted_CleanupTombstones",
			f:    bucket_WasDeleted_CleanupTombstones,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "bucketReadsIntoMemory",
			f:    bucketReadsIntoMemory,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
			},
		},
	}
	tests.run(ctx, t)
}

func bucket_WasDeleted_KeepTombstones(ctx context.Context, t *testing.T, opts []BucketOption) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b.Shutdown(context.Background()))
	})

	var (
		key = []byte("key")
		val = []byte("value")
	)

	t.Run("insert object", func(t *testing.T) {
		err = b.Put(key, val)
		require.Nil(t, err)
	})

	t.Run("assert object was not deleted yet", func(t *testing.T) {
		deleted, _, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.False(t, deleted)
	})

	deletionTime := time.Now()

	time.Sleep(3 * time.Millisecond)

	t.Run("delete object", func(t *testing.T) {
		err = b.DeleteWith(key, deletionTime)
		require.Nil(t, err)
	})

	time.Sleep(1 * time.Millisecond)

	t.Run("assert object was deleted", func(t *testing.T) {
		deleted, ts, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.True(t, deleted)
		require.WithinDuration(t, deletionTime, ts, 1*time.Millisecond)
	})

	t.Run("assert a nonexistent object is not detected as deleted", func(t *testing.T) {
		deleted, _, err := b.WasDeleted([]byte("DNE"))
		require.Nil(t, err)
		assert.False(t, deleted)
	})
}

func bucket_WasDeleted_CleanupTombstones(ctx context.Context, t *testing.T, opts []BucketOption) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, b.Shutdown(context.Background()))
	})

	var (
		key = []byte("key")
		val = []byte("value")
	)

	t.Run("insert object", func(t *testing.T) {
		err = b.Put(key, val)
		require.Nil(t, err)
	})

	t.Run("fails on WasDeleted without keepTombstones set (before delete)", func(t *testing.T) {
		deleted, _, err := b.WasDeleted(key)
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})

	t.Run("delete object", func(t *testing.T) {
		err = b.Delete(key)
		require.Nil(t, err)
	})

	t.Run("fails on WasDeleted without keepTombstones set (after delete)", func(t *testing.T) {
		deleted, _, err := b.WasDeleted(key)
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})

	t.Run("fails on WasDeleted without keepTombstones set (non-existent key)", func(t *testing.T) {
		deleted, _, err := b.WasDeleted([]byte("DNE"))
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})
}

func bucketReadsIntoMemory(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(b.GetDir())
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".bloom")
	assert.True(t, ok)

	_, ok = findFileWithExt(files, "secondary.0.bloom")
	assert.True(t, ok)
	b.Shutdown(ctx)

	b2, err := NewBucketCreator().NewBucket(ctx, b.GetDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	valuePrimary, err := b2.Get([]byte("hello"))
	require.Nil(t, err)
	valueSecondary := make([]byte, 5)
	valueSecondary, _, err = b2.GetBySecondaryIntoMemory(0, []byte("bonjour"), valueSecondary)
	require.Nil(t, err)

	assert.Equal(t, []byte("world"), valuePrimary)
	assert.Equal(t, []byte("world"), valueSecondary)
}

func TestBucket_MemtableCountWithFlushing(t *testing.T) {
	b := Bucket{
		// by using an empty segment group for the disk portion, we can test the
		// memtable portion in isolation
		disk: &SegmentGroup{},
	}

	tests := []struct {
		name                string
		current             *countStats
		previous            *countStats
		expectedNetActive   int
		expectedNetPrevious int
		expectedNetTotal    int
	}{
		{
			name: "only active, only additions",
			current: &countStats{
				upsertKeys: [][]byte{[]byte("key-1")},
			},
			expectedNetActive: 1,
		},
		{
			name: "only active, both additions and deletions",
			current: &countStats{
				upsertKeys: [][]byte{[]byte("key-1")},
				// no key with key-2 ever existed, so this does not alter the net count
				tombstonedKeys: [][]byte{[]byte("key-2")},
			},
			expectedNetActive: 1,
		},
		{
			name: "an deletion that was previously added",
			current: &countStats{
				tombstonedKeys: [][]byte{[]byte("key-a")},
			},
			previous: &countStats{
				upsertKeys: [][]byte{[]byte("key-a")},
			},
			expectedNetActive:   -1,
			expectedNetPrevious: 1,
			expectedNetTotal:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualActive, err := b.memtableNetCount(context.Background(), tt.current, tt.previous)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNetActive, actualActive)

			if tt.previous != nil {
				actualPrevious, err := b.memtableNetCount(context.Background(), tt.previous, nil)
				require.NoError(t, err)

				assert.Equal(t, tt.expectedNetPrevious, actualPrevious)

				assert.Equal(t, tt.expectedNetTotal, actualPrevious+actualActive)
			}
		})
	}
}

func TestBucketGetBySecondary(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	err = b.Put([]byte("hello"), []byte("world"), WithSecondaryKey(0, []byte("bonjour")))
	require.Nil(t, err)

	value, err := b.Get([]byte("hello"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(0, []byte("bonjour"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(1, []byte("bonjour"))
	require.Error(t, err)

	require.Nil(t, b.FlushMemtable())

	value, err = b.Get([]byte("hello"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(0, []byte("bonjour"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(1, []byte("bonjour"))
	require.Error(t, err)
}

func TestBucketInfoInFileName(t *testing.T) {
	ctx := context.Background()

	logger, _ := test.NewNullLogger()

	for _, segmentInfo := range []bool{true, false} {
		t.Run(fmt.Sprintf("%t", segmentInfo), func(t *testing.T) {
			dirName := t.TempDir()
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(segmentInfo),
			)
			require.NoError(t, err)
			require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1"))))
			require.NoError(t, b.FlushMemtable())
			dbFiles, _ := countDbAndWalFiles(t, dirName)
			require.Equal(t, dbFiles, 1)
		})
	}
}

func TestBucketCompactionFileName(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		firstSegment  bool
		secondSegment bool
		compaction    bool
	}{
		{firstSegment: false, secondSegment: false, compaction: true},
		{firstSegment: false, secondSegment: true, compaction: true},
		{firstSegment: true, secondSegment: false, compaction: true},
		{firstSegment: true, secondSegment: true, compaction: true},
		{firstSegment: true, secondSegment: true, compaction: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("firstSegment: %t", tt.firstSegment), func(t *testing.T) {
			dirName := t.TempDir()
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(tt.firstSegment),
			)
			require.NoError(t, err)
			require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1"))))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.Shutdown(ctx))
			dbFiles, _ := countDbAndWalFiles(t, dirName)
			require.Equal(t, dbFiles, 1)
			oldNames := verifyFileInfo(t, dirName, nil, tt.firstSegment, 0)

			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(tt.secondSegment),
			)
			require.NoError(t, err)
			require.NoError(t, b.Put([]byte("hello2"), []byte("world2"), WithSecondaryKey(0, []byte("bonjour2"))))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.Shutdown(ctx))

			dbFiles, _ = countDbAndWalFiles(t, dirName)
			require.Equal(t, dbFiles, 2)
			oldNames = verifyFileInfo(t, dirName, oldNames, tt.secondSegment, 0)

			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(tt.compaction),
			)
			require.NoError(t, err)
			compact, err := b.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compact)
			dbFiles, _ = countDbAndWalFiles(t, dirName)
			require.Equal(t, dbFiles, 1)
			verifyFileInfo(t, dirName, oldNames, tt.compaction, 1)
		})
	}
}

func countDbAndWalFiles(t *testing.T, path string) (int, int) {
	t.Helper()
	fileTypes := map[string]int{}
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		fileTypes[filepath.Ext(entry.Name())] += 1
	}
	return fileTypes[".db"], fileTypes[".wal"]
}

func verifyFileInfo(t *testing.T, path string, oldEntries []string, segmentInfo bool, level int) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	fileNames := []string{}
	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
		if oldEntries != nil && slices.Contains(oldEntries, entry.Name()) {
			continue
		}
		if filepath.Ext(entry.Name()) == ".db" {
			if segmentInfo {
				require.Contains(t, entry.Name(), fmt.Sprintf(".l%d.", level))
				require.Contains(t, entry.Name(), ".s0.")
			} else {
				require.NotRegexp(t, regexp.MustCompile(`\.l\d+\.`), entry.Name())
				require.NotContains(t, entry.Name(), ".s0.")
			}
		}
	}
	return fileNames
}

func TestNetCountComputationAtInit(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctx := context.Background()
	dirName := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true),
	)
	require.NoError(t, err)

	// create separate segments for each entry
	require.NoError(t, b.Put([]byte("hello1"), []byte("world1")))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.Put([]byte("hello2"), []byte("world2")))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.Put([]byte("hello3"), []byte("world3")))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.Put([]byte("hello4"), []byte("world4")))
	require.NoError(t, b.FlushMemtable())

	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	fileTypes := getFileTypeCount(t, dirName)
	require.Equal(t, 4, fileTypes[".db"])
	require.Equal(t, 0, fileTypes[".wal"])
	require.Equal(t, 4, fileTypes[".cna"])

	// Create a single segment with all deletions - shutdown so the cna files are not written right away, but at startup
	require.NoError(t, b.Delete([]byte("hello1")))
	require.NoError(t, b.Delete([]byte("hello2")))
	require.NoError(t, b.Delete([]byte("hello3")))
	require.NoError(t, b.Delete([]byte("hello4")))
	require.NoError(t, b.Shutdown(ctx))

	fileTypes = getFileTypeCount(t, dirName)
	require.Equal(t, 5, fileTypes[".db"])
	require.Equal(t, 0, fileTypes[".wal"])
	require.Equal(t, 4, fileTypes[".cna"]) // cna file for new segment not yet computed

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true),
	)
	require.NoError(t, err)

	fileTypes = getFileTypeCount(t, dirName)
	require.Equal(t, 5, fileTypes[".db"])
	require.Equal(t, 0, fileTypes[".wal"])
	require.Equal(t, 5, fileTypes[".cna"]) // now computed after startup

	count, err = b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func getFileTypeCount(t *testing.T, path string) map[string]int {
	t.Helper()
	fileTypes := map[string]int{}
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		fileTypes[filepath.Ext(entry.Name())] += 1
	}
	return fileTypes
}
