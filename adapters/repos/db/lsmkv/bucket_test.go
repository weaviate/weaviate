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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
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
	valueSecondary, _, err = b2.GetBySecondaryWithBuffer(ctx, 0, []byte("bonjour"), valueSecondary)
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
			actualActive, err := b.memtableNetCount(context.Background(), tt.current, tt.previous, []Segment{})
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNetActive, actualActive)

			if tt.previous != nil {
				actualPrevious, err := b.memtableNetCount(context.Background(), tt.previous, nil, []Segment{})
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

	_, err = b.GetBySecondary(ctx, 0, []byte("bonjour"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(ctx, 1, []byte("bonjour"))
	require.Error(t, err)

	require.Nil(t, b.FlushMemtable())

	value, err = b.Get([]byte("hello"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(ctx, 0, []byte("bonjour"))
	require.Nil(t, err)
	require.Equal(t, []byte("world"), value)

	_, err = b.GetBySecondary(ctx, 1, []byte("bonjour"))
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

// TestBucketReplaceStrategyConsistentView verifies that a Bucket using the
// "replace" strategy provides snapshot isolation via consistent views. The
// test follows this timeline:
//
//  1. Initial state: disk has key1, active memtable has key2. Reads return
//     both correctly.
//
//  2. First view: sees active=key2, flushing=nil, disk=key1.
//
//  3. Memtable switch: bucket switches to a new active with key3, moving key2
//     to flushing.
//     - Old view remains unchanged.
//     - New view sees active=key3, flushing=key2, disk=key1.
//
// 4. Flush: flushing (key2) is written to disk and removed.
//   - Old and second views remain stable.
//   - Writers progress without blocking readers.
//
// 5. Final view: sees active=key3, flushing=nil, disk containing key1 and key2.
//
// In summary, readers always see a consistent snapshot, while concurrent
// writes and flushes can proceed without disturbing existing views.
func TestBucketReplaceStrategyConsistentView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	diskSegments := &SegmentGroup{
		strategy: StrategyReplace,
		segments: []Segment{
			newFakeReplaceSegment(map[string][]byte{
				"key1": []byte("value1"),
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableReplace(map[string][]byte{
		"key2": []byte("value2"),
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyReplace,
	}

	// validate initial data before making any changes
	value, err := b.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), value)
	value, err = b.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value)
	n, err := b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	// open a consistent view
	view := b.getConsistentView()
	defer view.Release()

	// controls before making changes
	validateOriginalView := func(view BucketConsistentView) {
		// active
		v, err := view.Active.get([]byte("key2"))
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), v)

		// flushing
		require.Nil(t, view.Flushing)

		// disk
		v, err = b.disk.getWithSegmentList([]byte("key1"), view.Disk)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)

		// count across all
		n, err := b.countFromCV(ctx, view)
		require.NoError(t, err)
		require.Equal(t, 2, n)
	}
	validateOriginalView(view)

	// prove that we can switch memtables despite having an open consistent view
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableReplace(map[string][]byte{
			"key3": []byte("value3"),
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// prove that the open view still sees the same data
	validateOriginalView(view)

	// prove that a new view sees the new state
	view2 := b.getConsistentView()
	defer view2.Release()
	validateSecondView := func(view BucketConsistentView) {
		require.NotNil(t, view.Active)
		v, err := view.Active.get([]byte("key3"))
		require.NoError(t, err)
		require.Equal(t, []byte("value3"), v)

		require.NotNil(t, view.Flushing)
		v, err = view.Flushing.get([]byte("key2"))
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), v)

		v, err = b.disk.getWithSegmentList([]byte("key1"), view.Disk)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)

		// count across all
		n, err := b.countFromCV(ctx, view)
		require.NoError(t, err)
		require.Equal(t, 3, n)
	}
	validateSecondView(view2)

	// prove that we can flush the flushing mt into a disk segment without
	// affecting our two open views
	seg := flushReplaceTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateOriginalView(view)
	validateSecondView(view2)

	// finally, validate that a new view sees the final state
	view3 := b.getConsistentView()
	defer view3.Release()

	require.NotNil(t, view3.Active)
	v, err := view3.Active.get([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), v)

	require.Nil(t, view3.Flushing)

	// both key1 and key2 are now on disk
	v, err = b.disk.getWithSegmentList([]byte("key1"), view3.Disk)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v)
	v, err = b.disk.getWithSegmentList([]byte("key2"), view3.Disk)
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v)

	// count across all
	n, err = b.countFromCV(ctx, view3)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

// TestBucketReplaceStrategyWriteVsFlush verifies that writes remain consistent
// when overlapping with a flush-and-switch cycle. The timeline:
//
// 1. Initial state: active memtable has key1.
// 2. First write: key2 is added to the active memtable.
// 3. Concurrent flush-and-switch:
//   - Active memtable is moved to flushing, new empty active is installed.
//   - Writer still holds a reference to the old active while flushing proceeds.
//
// 4. Second write: key3 is written through the still-held reference.
// 5. Flush completes: flushing memtable (with key1, key2, key3) is persisted to disk.
// 6. Validation: a consistent view confirms all keys (key1, key2, key3) are present.
//
// In summary, the test proves that writers holding references can continue writing
// safely during a flush, and all writes are eventually preserved on disk.
func TestBucketReplaceStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()

	b := Bucket{
		active: newTestMemtableReplace(map[string][]byte{
			"key1": []byte("value1"),
		}),
		disk: &SegmentGroup{
			strategy:         StrategyReplace,
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategyReplace,
	}

	active, freeRefs := b.getActiveMemtableForWrite()

	// perform first write in initial state
	active.put([]byte("key2"), []byte("value2"), nil)

	// simulate a FlushAndSwitch() in a separate goroutine
	switchComplete := make(chan struct{})
	flushComplete := make(chan struct{})
	go func() {
		// switch memtable into flushing and add new. This also proves that we can
		// switch memtables despite having an active writer.
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableReplace(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchComplete)

		b.waitForZeroWriters(b.flushing)
		seg := flushReplaceTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushComplete)
	}()

	<-switchComplete

	// perform another write post-switch
	active.put([]byte("key3"), []byte("value3"), nil)

	freeRefs() // this unblocks the actual flush
	<-flushComplete

	// validate that all writes are present on disk
	view := b.getConsistentView()
	defer view.Release()

	expected := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, expectedV := range expected {
		v, err := b.disk.getWithSegmentList([]byte(k), view.Disk)
		require.NoError(t, err)
		require.Equal(t, expectedV, v)
	}

	n, err := b.countFromCV(context.Background(), view)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

// TestBucketRoaringSetStrategyConsistentView behaves like
// [TestBucketReplaceStrategyConsistentView], but for the "RoaringSet"
// strategy. See other test for detailed comments.
func TestBucketRoaringSetStrategyConsistentView(t *testing.T) {
	t.Parallel()

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
				"key1": bitmapFromSlice([]uint64{1}),
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableRoaringSet(map[string][]uint64{
		"key1": {2},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyRoaringSet,
	}

	// validate initial data before making any changes
	value, releaseBuffers, err := b.RoaringSetGet([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, bitmapFromSlice([]uint64{1, 2}).ToArray(), value.ToArray())
	releaseBuffers()

	// open a consistent view
	view := b.getConsistentView()
	defer view.Release()

	// controls before making changes
	validateOriginalView := func(view BucketConsistentView) {
		expected := map[string]*sroar.Bitmap{
			"key1": bitmapFromSlice([]uint64{1, 2}),
		}

		for k, expectedV := range expected {
			v, release, err := b.roaringSetGetFromConsistentView(view, []byte(k))
			require.NoError(t, err)
			require.Equal(t, expectedV.ToArray(), v.ToArray())
			release()
		}
	}
	validateOriginalView(view)

	// prove that we can switch memtables despite having an open consistent view
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableRoaringSet(map[string][]uint64{
			"key1": {3},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// prove that the open view still sees the same data
	validateOriginalView(view)

	// prove that a new view sees the new state
	view2 := b.getConsistentView()
	defer view2.Release()
	validateSecondView := func(view BucketConsistentView) {
		expected := map[string]*sroar.Bitmap{
			"key1": bitmapFromSlice([]uint64{1, 2, 3}),
		}

		for k, expectedV := range expected {
			v, release, err := b.roaringSetGetFromConsistentView(view, []byte(k))
			require.NoError(t, err)
			require.Equal(t, expectedV.ToArray(), v.ToArray())
			release()
		}
	}
	validateSecondView(view2)

	// prove that we can flush the flushing mt into a disk segment without
	// affecting our two open views
	seg := flushRoaringSetTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateOriginalView(view)
	validateSecondView(view2)

	// finally, validate that a new view sees the final state
	view3 := b.getConsistentView()
	defer view3.Release()

	// the original memtable was flushed to disk
	v, release, err := b.disk.roaringSetGet([]byte("key1"), view3.Disk)
	assert.Equal(t, []uint64{1, 2}, v.Flatten(true).ToArray())
	require.NoError(t, err)
	release()
}

// TestBucketRoaringSetStrategyWriteVsFlush verifies that writers can keep
// updating a RoaringSet while a flush-and-switch is in progress, and that all
// writes are preserved once the flush completes.
//
// Timeline:
//
//  1. Initial active has key1 -> {1}.
//  2. First write adds {2} to key1 via the active memtable reference.
//  3. Concurrent flush-and-switch moves the active to flushing and installs a
//     new empty active.
//  4. Second write adds {3} to key1 through the still-held active reference.
//  5. Flush persists key1 -> {1,2,3} to disk.
//  6. Validation: a consistent view observes {1,2,3} for key1 on disk.
func TestBucketRoaringSetStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()

	b := Bucket{
		active: newTestMemtableRoaringSet(map[string][]uint64{
			"key1": {1},
		}),
		disk: &SegmentGroup{
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategyRoaringSet,
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	require.NoError(t, active.roaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{2})))

	// Simulate a FlushAndSwitch() running concurrently
	switchComplete := make(chan struct{})
	flushComplete := make(chan struct{})
	go func() {
		// Switch active -> flushing, new empty active installed
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableRoaringSet(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchComplete)

		b.waitForZeroWriters(b.flushing)

		seg := flushRoaringSetTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushComplete)
	}()

	// Ensure the switch has happened (we still hold a writer ref to the old active)
	<-switchComplete

	// Second write after switch: still writing through the old active reference
	require.NoError(t, active.roaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{3})))

	// Release writer refs to allow the flush to proceed
	freeRefs()
	<-flushComplete

	// Validate: key1 has {1,2,3} on disk
	view := b.getConsistentView()
	defer view.Release()

	bm, release, err := b.disk.roaringSetGet([]byte("key1"), view.Disk)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, bm.Flatten(true).ToArray())
	release()
}

// TestBucketRoaringSetRangeStrategyConsistentViewUsingReader behaves similarly to
// [TestBucketReplaceStrategyConsistentView], but for the "RoaringSetRange"
// strategy and with usage of ReaderRoaringSetRange. See other test for detailed comments.
func TestBucketRoaringSetRangeStrategyConsistentViewUsingReader(t *testing.T) {
	t.Parallel()

	key1 := uint64(1)
	createValidateReader := func(reader ReaderRoaringSetRange, expected map[uint64][]uint64) func(*testing.T) {
		return func(t *testing.T) {
			t.Helper()
			for k, expectedV := range expected {
				v, release, err := reader.Read(context.Background(), k, filters.OperatorEqual)

				require.NoError(t, err)
				require.Equal(t, expectedV, v.ToArray())
				release()
			}
		}
	}

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
				key1: roaringset.NewBitmap(1),
			}, sroar.NewBitmap()),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableRoaringSetRange(map[uint64][]uint64{
		key1: {2},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyRoaringSetRange,
	}

	// validate initial data before making any changes
	reader1 := b.ReaderRoaringSetRange()
	defer reader1.Close()
	validateOriginalReader := createValidateReader(reader1, map[uint64][]uint64{key1: {1, 2}})
	validateOriginalReader(t)

	// prove that we can switch memtables despite having an open reader
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableRoaringSetRange(map[uint64][]uint64{
			key1: {3},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// prove that the open reader still sees the same data
	validateOriginalReader(t)

	// prove that a new reader sees the new state
	reader2 := b.ReaderRoaringSetRange()
	defer reader2.Close()
	validateSecondReader := createValidateReader(reader2, map[uint64][]uint64{key1: {1, 2, 3}})
	validateSecondReader(t)

	// prove that we can flush the flushing mt into a disk segment without
	// affecting our two open readers
	seg := flushRoaringSetRangeTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateOriginalReader(t)
	validateSecondReader(t)

	// validate that a new reader sees the current state
	reader3 := b.ReaderRoaringSetRange()
	defer reader3.Close()
	validateThirdReader := createValidateReader(reader3, map[uint64][]uint64{key1: {1, 2, 3}})
	validateThirdReader(t)

	require.NoError(t, b.active.roaringSetRangeAdd(key1, 4))

	// validate that last reader sees the final state
	reader4 := b.ReaderRoaringSetRange()
	defer reader4.Close()
	validateFourthReader := createValidateReader(reader4, map[uint64][]uint64{key1: {1, 2, 3, 4}})
	validateFourthReader(t)

	// prove that the open readers still see the same data
	validateOriginalReader(t)
	validateSecondReader(t)
	validateThirdReader(t)
}

// TestBucketRoaringSetRangeStrategyConsistentViewUsingReaderInMemo behaves similarly to
// [TestBucketReplaceStrategyConsistentView], but for the "RoaringSetRange"
// strategy and with usage of ReaderRoaringSetRange. See other test for detailed comments.
func TestBucketRoaringSetRangeStrategyConsistentViewUsingReaderInMemo(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	key1 := uint64(1)
	createValidateReader := func(reader ReaderRoaringSetRange, expected map[uint64][]uint64) func(*testing.T) {
		return func(t *testing.T) {
			t.Helper()
			for k, expectedV := range expected {
				v, release, err := reader.Read(context.Background(), k, filters.OperatorEqual)

				require.NoError(t, err)
				require.Equal(t, expectedV, v.ToArray())
				release()
			}
		}
	}

	initialMemtable := newTestMemtableRoaringSetRange(map[uint64][]uint64{
		key1: {2},
	})

	mt := roaringsetrange.NewMemtable(logger)
	mt.Insert(key1, []uint64{1})
	segInMemo := roaringsetrange.NewSegmentInMemory(logger)
	segInMemo.MergeMemtableEventually(mt)

	b := Bucket{
		active: initialMemtable,
		disk: &SegmentGroup{
			segments:                       []Segment{},
			roaringSetRangeSegmentInMemory: segInMemo,
		},
		strategy:             StrategyRoaringSetRange,
		keepSegmentsInMemory: true,
		bitmapBufPool:        roaringset.NewBitmapBufPoolNoop(),
	}

	// validate initial data before making any changes
	reader1 := b.ReaderRoaringSetRange()
	defer reader1.Close()
	validateOriginalReader := createValidateReader(reader1, map[uint64][]uint64{key1: {1, 2}})
	validateOriginalReader(t)

	// prove that we can switch memtables despite having an open reader
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableRoaringSetRange(map[uint64][]uint64{
			key1: {3},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// prove that the open reader still sees the same data
	validateOriginalReader(t)

	// prove that a new reader sees the new state
	reader2 := b.ReaderRoaringSetRange()
	defer reader2.Close()
	validateSecondReader := createValidateReader(reader2, map[uint64][]uint64{key1: {1, 2, 3}})
	validateSecondReader(t)

	// prove that we can flush the flushing mt into a disk segment without
	// affecting our two open readers
	b.atomicallyAddDiskSegmentAndRemoveFlushing(&fakeSegment{})
	validateOriginalReader(t)
	validateSecondReader(t)

	// validate that a new reader sees the current state
	reader3 := b.ReaderRoaringSetRange()
	defer reader3.Close()
	validateThirdReader := createValidateReader(reader3, map[uint64][]uint64{key1: {1, 2, 3}})
	validateThirdReader(t)

	require.NoError(t, b.active.roaringSetRangeAdd(key1, 4))

	// validate that last reader sees the final state
	reader4 := b.ReaderRoaringSetRange()
	defer reader4.Close()
	validateFourthReader := createValidateReader(reader4, map[uint64][]uint64{key1: {1, 2, 3, 4}})
	validateFourthReader(t)

	// prove that the open readers still see the same data
	validateOriginalReader(t)
	validateSecondReader(t)
	validateThirdReader(t)
}

// TestBucketRoaringSetRangeStrategyWriteVsFlush verifies that writers can keep
// updating a RoaringSetRange while a flush-and-switch is in progress, and that all
// writes are preserved once the flush completes.
//
// Timeline:
//
//  1. Initial active has key `1` -> {1}.
//  2. First write adds {2} to key `1` via the active memtable reference.
//  3. Concurrent flush-and-switch moves the active to flushing and installs a
//     new empty active.
//  4. Second write adds {3} to key `1` through the still-held active reference.
//  5. Flush persists key `1` -> {1,2,3} to disk.
//  6. Validation: a reader observes {1,2,3} for key `1` on disk.
func TestBucketRoaringSetRangeStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()

	key1 := uint64(1)
	b := Bucket{
		active: newTestMemtableRoaringSetRange(map[uint64][]uint64{
			key1: {1},
		}),
		disk: &SegmentGroup{
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategyRoaringSetRange,
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	require.NoError(t, active.roaringSetRangeAdd(key1, 2))

	// Simulate a FlushAndSwitch() running concurrently
	switchComplete := make(chan struct{})
	flushComplete := make(chan struct{})
	go func() {
		// Switch active -> flushing, new empty active installed
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableRoaringSetRange(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchComplete)

		b.waitForZeroWriters(b.flushing)

		seg := flushRoaringSetRangeTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushComplete)
	}()

	// Ensure the switch has happened (we still hold a writer ref to the old active)
	<-switchComplete

	// Second write after switch: still writing through the old active reference
	require.NoError(t, active.roaringSetRangeAdd(key1, 3))

	// Release writer refs to allow the flush to proceed
	freeRefs()
	<-flushComplete

	// Validate: key1 has {1,2,3} on disk (active memtable is empty)
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	v, release, err := reader.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, v.ToArray())
	release()
}

// TestBucketRoaringSetRangeStrategyWriteVsFlushInMemo verifies that writers can keep
// updating a RoaringSetRange while a flush-and-switch is in progress, and that all
// writes are preserved once the flush completes.
//
// Timeline:
//
//  1. Initial active has key `1` -> {1}.
//  2. First write adds {2} to key `1` via the active memtable reference.
//  3. Concurrent flush-and-switch moves the active to flushing and installs a
//     new empty active.
//  4. Second write adds {3} to key `1` through the still-held active reference.
//  5. Flush persists key `1` -> {1,2,3} to disk.
//  6. Validation: a reader observes {1,2,3} for key `1` on disk.
func TestBucketRoaringSetRangeStrategyWriteVsFlushInMemo(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	key1 := uint64(1)
	b := Bucket{
		active: newTestMemtableRoaringSetRange(map[uint64][]uint64{
			key1: {1},
		}),
		disk: &SegmentGroup{
			segments:                       []Segment{},
			segmentsWithRefs:               map[string]Segment{},
			roaringSetRangeSegmentInMemory: roaringsetrange.NewSegmentInMemory(logger),
		},
		strategy:             StrategyRoaringSetRange,
		keepSegmentsInMemory: true,
		bitmapBufPool:        roaringset.NewBitmapBufPoolNoop(),
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	require.NoError(t, active.roaringSetRangeAdd(key1, 2))

	// Simulate a FlushAndSwitch() running concurrently
	switchComplete := make(chan struct{})
	flushComplete := make(chan struct{})
	go func() {
		// Switch active -> flushing, new empty active installed
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableRoaringSetRange(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchComplete)

		b.waitForZeroWriters(b.flushing)

		seg := flushRoaringSetRangeTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushComplete)
	}()

	// Ensure the switch has happened (we still hold a writer ref to the old active)
	<-switchComplete

	// Second write after switch: still writing through the old active reference
	require.NoError(t, active.roaringSetRangeAdd(key1, 3))

	// Release writer refs to allow the flush to proceed
	freeRefs()
	<-flushComplete

	// Validate: key1 has {1,2,3} on disk (active memtable is empty)
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	v, release, err := reader.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, v.ToArray())
	release()
}

func TestBucketSetStrategyConsistentView(t *testing.T) {
	t.Parallel()

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeSetSegment(map[string][][]byte{
				"key1": {[]byte("d1")},
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableSet(map[string][][]byte{
		"key2": {[]byte("a2")},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategySetCollection,
	}

	// Sanity via Bucket API
	got, err := b.SetList([]byte("key1"))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("d1")}, got)

	got, err = b.SetList([]byte("key2"))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("a2")}, got)

	// View #1 (pre-switch): active=key2, flushing=nil, disk=key1
	view1 := b.getConsistentView()
	defer view1.Release()

	validateView1 := func(v BucketConsistentView) {
		expected := map[string][][]byte{
			"key1": {[]byte("d1")},
			"key2": {[]byte("a2")},
		}

		for k, expectedV := range expected {
			value, err := b.setListFromConsistentView(v, []byte(k))
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView1(view1)

	// Switch: move active->flushing (key2), new active with key3 -> {"a3"}
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableSet(map[string][][]byte{
			"key3": {[]byte("a3")},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// view1 remains unchanged
	validateView1(view1)

	// View #2 (post-switch): active=key3, flushing=key2, disk=key1
	view2 := b.getConsistentView()
	defer view2.Release()

	validateView2 := func(v BucketConsistentView) {
		expected := map[string][][]byte{
			"key1": {[]byte("d1")},
			"key2": {[]byte("a2")},
			"key3": {[]byte("a3")},
		}

		for k, expectedV := range expected {
			value, err := b.setListFromConsistentView(v, []byte(k))
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView2(view2)

	// Flush flushing (key2) -> disk; both views stay stable
	seg := flushSetTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateView1(view1)
	validateView2(view2)

	// Final view: active=key3, flushing=nil, disk has key1 & key2
	view3 := b.getConsistentView()
	defer view3.Release()

	// active: key3 -> {"a3"}
	a3, err := b.setListFromConsistentView(view3, []byte("key3"))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("a3")}, a3)

	require.Nil(t, view3.Flushing)

	// disk: key1 -> {"d1"}, key2 -> {"a2"}
	raw1, err := b.disk.getCollection([]byte("key1"), view3.Disk)
	require.NoError(t, err)
	v := newSetDecoder().Do(raw1)
	require.ElementsMatch(t, [][]byte{[]byte("d1")}, v)

	raw2, err := b.disk.getCollection([]byte("key2"), view3.Disk)
	require.NoError(t, err)
	v = newSetDecoder().Do(raw2)
	require.ElementsMatch(t, [][]byte{[]byte("a2")}, v)
}

func TestBucketSetStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()

	b := Bucket{
		active: newTestMemtableSet(map[string][][]byte{"key1": {[]byte("v1")}}),
		disk: &SegmentGroup{
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategySetCollection,
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	err := active.append([]byte("key1"), newSetEncoder().Do([][]byte{[]byte("v2")}))
	require.NoError(t, err)

	switchDone := make(chan struct{})
	flushDone := make(chan struct{})

	go func() {
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableSet(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchDone)

		b.waitForZeroWriters(b.flushing)

		seg := flushSetTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushDone)
	}()

	<-switchDone

	// Second write (post-switch) through the old active reference
	err = active.append([]byte("key1"), newSetEncoder().Do([][]byte{[]byte("v3")}))
	require.NoError(t, err)

	// Release and let flush proceed
	freeRefs()
	<-flushDone

	// Validate disk now has {"v1","v2","v3"} for key1
	view := b.getConsistentView()
	defer view.Release()

	raw, err := b.disk.getCollection([]byte("key1"), view.Disk)
	require.NoError(t, err)

	got := newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}, got)
}

func TestBucketMapStrategyConsistentView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeMapSegment(map[string][]MapPair{
				"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableMap(map[string][]MapPair{
		"key2": {{Key: []byte("ak1"), Value: []byte("av1")}},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyMapCollection,
	}

	// Sanity via Bucket API
	got, err := b.MapList(ctx, []byte("key1"))
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{{Key: []byte("dk1"), Value: []byte("dv1")}}, got)

	got, err = b.MapList(ctx, []byte("key2"))
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{{Key: []byte("ak1"), Value: []byte("av1")}}, got)

	// View #1 (pre-switch): active=key2, flushing=nil, disk=key1
	view1 := b.getConsistentView()
	defer view1.Release()

	validateView1 := func(v BucketConsistentView) {
		expected := map[string][]MapPair{
			"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
			"key2": {{Key: []byte("ak1"), Value: []byte("av1")}},
		}

		for k, expectedV := range expected {
			value, err := b.mapListFromConsistentView(ctx, v, []byte(k))
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView1(view1)

	// Switch: move active->flushing (key2), new active with key3 -> {"a3"}
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableMap(map[string][]MapPair{
			"key3": {{Key: []byte("ak2"), Value: []byte("av2")}},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// view1 remains unchanged
	validateView1(view1)

	// View #2 (post-switch): active=key3, flushing=key2, disk=key1
	view2 := b.getConsistentView()
	defer view2.Release()

	validateView2 := func(v BucketConsistentView) {
		expected := map[string][]MapPair{
			"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
			"key2": {{Key: []byte("ak1"), Value: []byte("av1")}},
			"key3": {{Key: []byte("ak2"), Value: []byte("av2")}},
		}

		for k, expectedV := range expected {
			value, err := b.mapListFromConsistentView(ctx, v, []byte(k))
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView2(view2)

	// Flush flushing (key2) -> disk; both views stay stable
	seg := flushMapTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateView1(view1)
	validateView2(view2)

	// Final view: active=key3, flushing=nil, disk has key1 & key2
	view3 := b.getConsistentView()
	defer view3.Release()

	// active: key3 -> {"a3"}
	a3, err := b.mapListFromConsistentView(ctx, view3, []byte("key3"))
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		{Key: []byte("ak2"), Value: []byte("av2")},
	}, a3)

	require.Nil(t, view3.Flushing)

	// disk: key1 -> {"d1"}, key2 -> {"a2"}
	raw1, err := b.disk.getCollection([]byte("key1"), view3.Disk)
	require.NoError(t, err)
	v, err := newMapDecoder().Do(raw1, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		{Key: []byte("dk1"), Value: []byte("dv1")},
	}, v)

	raw2, err := b.disk.getCollection([]byte("key2"), view3.Disk)
	require.NoError(t, err)
	v, err = newMapDecoder().Do(raw2, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		{Key: []byte("ak1"), Value: []byte("av1")},
	}, v)
}

func TestBucketMapStrategyDocPointersConsistentView(t *testing.T) {
	// DocPointers is a special accessor of a map bucket that uses tf-idf style
	// data
	t.Parallel()
	ctx := context.Background()

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeMapSegment(map[string][]MapPair{
				"key1": {mapFromDocPointers(0, 1.0, 3)},
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableMap(map[string][]MapPair{
		"key2": {mapFromDocPointers(1, 0.8, 4)},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyMapCollection,
	}

	// Sanity via Bucket API
	got, err := b.DocPointerWithScoreList(ctx, []byte("key1"), 1)
	require.NoError(t, err)
	require.ElementsMatch(t, []terms.DocPointerWithScore{docPointers(0, 1.0, 3)}, got)

	got, err = b.DocPointerWithScoreList(ctx, []byte("key2"), 1)
	require.NoError(t, err)
	require.ElementsMatch(t, []terms.DocPointerWithScore{docPointers(1, 0.8, 4)}, got)

	// View #1 (pre-switch): active=key2, flushing=nil, disk=key1
	view1 := b.getConsistentView()
	defer view1.Release()

	validateView1 := func(v BucketConsistentView) {
		expected := map[string][]terms.DocPointerWithScore{
			"key1": {docPointers(0, 1.0, 3)},
			"key2": {docPointers(1, 0.8, 4)},
		}

		for k, expectedV := range expected {
			value, err := b.docPointerWithScoreListFromConsistentView(ctx, v, []byte(k), 1)
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView1(view1)

	// Switch: move active->flushing (key2), new active with key3 -> {"a3"}
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableMap(map[string][]MapPair{
			"key3": {mapFromDocPointers(2, 0.6, 2)},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// view1 remains unchanged
	validateView1(view1)

	// View #2 (post-switch): active=key3, flushing=key2, disk=key1
	view2 := b.getConsistentView()
	defer view2.Release()

	validateView2 := func(v BucketConsistentView) {
		expected := map[string][]terms.DocPointerWithScore{
			"key1": {docPointers(0, 1.0, 3)},
			"key2": {docPointers(1, 0.8, 4)},
			"key3": {docPointers(2, 0.6, 2)},
		}

		for k, expectedV := range expected {
			value, err := b.docPointerWithScoreListFromConsistentView(ctx, v, []byte(k), 1)
			require.NoError(t, err)
			require.ElementsMatch(t, expectedV, value)
		}
	}
	validateView2(view2)

	// Flush flushing (key2) -> disk; both views stay stable
	seg := flushMapTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateView1(view1)
	validateView2(view2)

	// Final view: active=key3, flushing=nil, disk has key1 & key2
	view3 := b.getConsistentView()
	defer view3.Release()

	// active: key3 -> {"a3"}
	a3, err := b.docPointerWithScoreListFromConsistentView(ctx, view3, []byte("key3"), 1)
	require.NoError(t, err)
	require.ElementsMatch(t, []terms.DocPointerWithScore{
		docPointers(2, 0.6, 2),
	}, a3)

	require.Nil(t, view3.Flushing)

	// disk: key1 -> {"d1"}, key2 -> {"a2"}
	raw1, err := b.disk.getCollection([]byte("key1"), view3.Disk)
	require.NoError(t, err)
	v, err := newMapDecoder().Do(raw1, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		mapFromDocPointers(0, 1.0, 3),
	}, v)

	raw2, err := b.disk.getCollection([]byte("key2"), view3.Disk)
	require.NoError(t, err)
	v, err = newMapDecoder().Do(raw2, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		mapFromDocPointers(1, 0.8, 4),
	}, v)
}

func TestBucketMapStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()

	b := Bucket{
		active: newTestMemtableMap(map[string][]MapPair{"key1": {
			{Key: []byte("k1"), Value: []byte("v1")},
		}}),
		disk: &SegmentGroup{
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategyMapCollection,
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	err := active.appendMapSorted([]byte("key1"), MapPair{
		Key: []byte("k2"), Value: []byte("v2"),
	})
	require.NoError(t, err)

	switchDone := make(chan struct{})
	flushDone := make(chan struct{})

	go func() {
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableMap(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchDone)

		b.waitForZeroWriters(b.flushing)

		seg := flushMapTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushDone)
	}()

	<-switchDone

	// Second write (post-switch) through the old active reference
	err = active.appendMapSorted([]byte("key1"), MapPair{
		Key: []byte("k3"), Value: []byte("v3"),
	})
	require.NoError(t, err)

	// Release and let flush proceed
	freeRefs()
	<-flushDone

	// Validate disk now has {"v1","v2","v3"} for key1
	view := b.getConsistentView()
	defer view.Release()

	raw, err := b.disk.getCollection([]byte("key1"), view.Disk)
	require.NoError(t, err)

	got, err := newMapDecoder().Do(raw, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}, got)
}

func TestBucketInvertedStrategyConsistentView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	diskSegments := &SegmentGroup{
		segments: []Segment{
			newFakeInvertedSegment(map[string][]MapPair{
				"key1": {NewMapPairFromDocIdAndTf(0, 2, 1, false), NewMapPairFromDocIdAndTf(10, 10, 1, false)},
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	initialMemtable := newTestMemtableInverted(map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(1, 3, 1, false)},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyInverted,
		logger:   logrus.New(),
	}

	err := validateMapPairListVsBlockMaxSearch(ctx, &b, []kv{
		{
			key: []byte("key1"),
			values: []MapPair{
				NewMapPairFromDocIdAndTf(0, 2, 1, false),
				NewMapPairFromDocIdAndTf(1, 3, 1, false),
			},
		},
	})
	require.NoError(t, err)

	// View #1 (pre-switch): active=doc_id(0), flushing=nil, disk=doc_id(1)
	view1 := b.getConsistentView()
	defer view1.Release()

	validateView1 := func(v BucketConsistentView) {
		expected := []kv{
			{
				key: []byte("key1"),
				values: []MapPair{
					NewMapPairFromDocIdAndTf(0, 2, 1, false),
					NewMapPairFromDocIdAndTf(1, 3, 1, false),
				},
			},
		}

		require.NoError(t, validateMapPairListVsBlockMaxSearchFromView(ctx, &b, v, expected))
	}
	validateView1(view1)

	// Switch: move active->flushing (key2), new active with key3 -> {"a3"}
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableInverted(map[string][]MapPair{
			"key1": {NewMapPairFromDocIdAndTf(2, 4, 1, false)},
		}), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// view1 remains unchanged
	validateView1(view1)

	// View #2 (post-switch): active=doc_id(2), flushing=doc_id(1), disk=doc_id(0)
	view2 := b.getConsistentView()
	defer view2.Release()

	validateView2 := func(v BucketConsistentView) {
		expected := []kv{
			{
				key: []byte("key1"),
				values: []MapPair{
					NewMapPairFromDocIdAndTf(0, 2, 1, false),
					NewMapPairFromDocIdAndTf(1, 3, 1, false),
					NewMapPairFromDocIdAndTf(2, 4, 1, false),
				},
			},
		}

		require.NoError(t, validateMapPairListVsBlockMaxSearchFromView(ctx, &b, v, expected))
	}
	validateView2(view2)

	// Flush flushing (key2) -> disk; both views stay stable
	seg := flushInvertedTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateView1(view1)
	validateView2(view2)

	// Final view: active=doc_id(2), flushing=nil, disk has doc_id(0) & doc_id(1)
	err = validateMapPairListVsBlockMaxSearch(ctx, &b, []kv{
		{
			key: []byte("key1"),
			values: []MapPair{
				NewMapPairFromDocIdAndTf(0, 2, 1, false),
				NewMapPairFromDocIdAndTf(1, 3, 1, false),
				NewMapPairFromDocIdAndTf(2, 4, 1, false),
			},
		},
	})
	require.NoError(t, err)
}

func TestBucketInvertedStrategyWriteVsFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := Bucket{
		active: newTestMemtableInverted(map[string][]MapPair{
			"key1": {NewMapPairFromDocIdAndTf(0, 3, 1, false)},
		}),
		disk: &SegmentGroup{
			segments:         []Segment{},
			segmentsWithRefs: map[string]Segment{},
		},
		strategy: StrategyInverted,
	}

	active, freeRefs := b.getActiveMemtableForWrite()
	err := active.appendMapSorted([]byte("key1"),
		NewMapPairFromDocIdAndTf(1, 2, 1, false),
	)
	require.NoError(t, err)

	switchDone := make(chan struct{})
	flushDone := make(chan struct{})

	go func() {
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableInverted(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		close(switchDone)

		b.waitForZeroWriters(b.flushing)

		seg := flushInvertedTestMemtableIntoTestSegment(b.flushing)
		b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
		close(flushDone)
	}()

	<-switchDone

	// Second write (post-switch) through the old active reference
	err = active.appendMapSorted([]byte("key1"),
		NewMapPairFromDocIdAndTf(2, 2, 1, false),
	)
	require.NoError(t, err)

	// Release and let flush proceed
	freeRefs()
	<-flushDone

	// Validate disk now has all 3 writes
	view := b.getConsistentView()
	defer view.Release()

	require.Len(t, view.Disk, 1, "there should be exactly one disk segment")
	expected := []kv{
		{
			key: []byte("key1"),
			values: []MapPair{
				NewMapPairFromDocIdAndTf(0, 3, 1, false),
				NewMapPairFromDocIdAndTf(1, 2, 1, false),
				NewMapPairFromDocIdAndTf(2, 2, 1, false),
			},
		},
	}
	require.NoError(t, validateMapPairListVsBlockMaxSearchFromSingleSegment(ctx, view.Disk[0], expected))
}

type testMemtable struct {
	*Memtable
	totalWriteCountIncs int
	totalWriteCountDecs int
}

func (t *testMemtable) incWriterCount() {
	t.totalWriteCountIncs++
	t.Memtable.incWriterCount()
}

func (t *testMemtable) decWriterCount() {
	t.totalWriteCountDecs++
	t.Memtable.decWriterCount()
}

func newTestMemtableReplace(initialData map[string][]byte) *testMemtable {
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableReplace: %w", err))
	}

	m := &Memtable{
		strategy:  StrategyReplace,
		key:       &binarySearchTree{},
		commitlog: newDummyCommitLogger(),
		metrics:   metrics,
	}

	for k, v := range initialData {
		m.key.insert([]byte(k), v, nil)
		m.size += uint64(len(k) + len(v))
	}

	return &testMemtable{Memtable: m}
}

func newTestMemtableRoaringSet(initialData map[string][]uint64) *testMemtable {
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableRoaringSet: %w", err))
	}

	m := &Memtable{
		strategy:   StrategyRoaringSet,
		roaringSet: &roaringset.BinarySearchTree{},
		commitlog:  newDummyCommitLogger(),
		metrics:    metrics,
	}

	for k, v := range initialData {
		m.roaringSet.Insert([]byte(k), roaringset.Insert{Additions: v})
		m.size += uint64(len(k) + len(v))
	}

	return &testMemtable{Memtable: m}
}

func newTestMemtableRoaringSetRange(initialData map[uint64][]uint64) *testMemtable {
	logger, _ := test.NewNullLogger()
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableRoaringSetRange: %w", err))
	}

	m := &Memtable{
		strategy:        StrategyRoaringSetRange,
		roaringSetRange: roaringsetrange.NewMemtable(logger),
		commitlog:       newDummyCommitLogger(),
		metrics:         metrics,
	}

	for k, v := range initialData {
		m.roaringSetRange.Insert(k, v)
		m.size += uint64(len(v))
	}

	return &testMemtable{Memtable: m}
}

func newTestMemtableSet(initialData map[string][][]byte) *testMemtable {
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableSet: %w", err))
	}

	m := &Memtable{
		strategy:  StrategySetCollection,
		keyMulti:  &binarySearchTreeMulti{},
		commitlog: newDummyCommitLogger(),
		metrics:   metrics,
	}

	for k, v := range initialData {
		m.append([]byte(k), newSetEncoder().Do(v))
	}

	return &testMemtable{Memtable: m}
}

func newTestMemtableMap(initialData map[string][]MapPair) *testMemtable {
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableMap: %w", err))
	}

	m := &Memtable{
		strategy:  StrategyMapCollection,
		keyMap:    &binarySearchTreeMap{},
		commitlog: newDummyCommitLogger(),
		metrics:   metrics,
	}

	for k, v := range initialData {
		for _, mp := range v {
			m.appendMapSorted([]byte(k), mp)
		}
	}

	return &testMemtable{Memtable: m}
}

func newTestMemtableInverted(initialData map[string][]MapPair) *testMemtable {
	metrics, err := newMemtableMetrics(nil, "", "")
	if err != nil {
		panic(fmt.Errorf("newTestMemtableInverted: %w", err))
	}

	m := &Memtable{
		strategy:   StrategyInverted,
		keyMap:     &binarySearchTreeMap{},
		commitlog:  newDummyCommitLogger(),
		metrics:    metrics,
		tombstones: sroar.NewBitmap(),
	}

	for k, v := range initialData {
		for _, mp := range v {
			m.appendMapSorted([]byte(k), mp)
		}
	}

	return &testMemtable{Memtable: m}
}

func flushReplaceTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	allEntries := m.(*testMemtable).key.flattenInOrder()
	data := map[string][]byte{}
	for _, e := range allEntries {
		data[string(e.key)] = e.value
	}
	return newFakeReplaceSegment(data)
}

func flushRoaringSetTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	// NOTE: This fake pretends only additions exist, it ignores deletes
	allEntries := m.(*testMemtable).roaringSet.FlattenInOrder()
	data := map[string]*sroar.Bitmap{}
	for _, e := range allEntries {
		data[string(e.Key)] = e.Value.Additions.Clone()
	}
	return newFakeRoaringSetSegment(data)
}

func flushRoaringSetRangeTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	if m.getStrategy() != StrategyRoaringSetRange {
		panic("not a roaring set range memtable")
	}

	rsrMt := m.extractRoaringSetRange()
	return newFakeRoaringSetRangeSegment(rsrMt.Additions(), rsrMt.Deletions())
}

func flushSetTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	allEntries := m.(*testMemtable).keyMulti.flattenInOrder()
	data := map[string][][]byte{}
	for _, e := range allEntries {
		values := newSetDecoder().Do(e.values)
		data[string(e.key)] = values
	}
	return newFakeSetSegment(data)
}

func flushMapTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	allEntries := m.(*testMemtable).keyMap.flattenInOrder()
	data := map[string][]MapPair{}
	for _, e := range allEntries {
		valuesCopy := make([]MapPair, len(e.values))
		copy(valuesCopy, e.values)
		data[string(e.key)] = valuesCopy
	}
	return newFakeMapSegment(data)
}

func flushInvertedTestMemtableIntoTestSegment(m memtable) *fakeSegment {
	allEntries := m.(*testMemtable).keyMap.flattenInOrder()
	data := map[string][]MapPair{}
	for _, e := range allEntries {
		valuesCopy := make([]MapPair, len(e.values))
		copy(valuesCopy, e.values)
		data[string(e.key)] = valuesCopy
	}
	return newFakeInvertedSegment(data)
}

func newDummyCommitLogger() memtableCommitLogger {
	return &dummyCommitLogger{}
}

type dummyCommitLogger struct{}

func (d *dummyCommitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	return nil
}

func (d *dummyCommitLogger) put(node segmentReplaceNode) error {
	return nil
}

func (d *dummyCommitLogger) append(node segmentCollectionNode) error {
	return nil
}

func (d *dummyCommitLogger) add(node *roaringset.SegmentNodeList) error {
	return nil
}

func (d *dummyCommitLogger) walPath() string {
	return "dummy"
}

func (d *dummyCommitLogger) size() int64 {
	return 0
}

func (d *dummyCommitLogger) flushBuffers() error {
	return nil
}

func (d *dummyCommitLogger) close() error {
	return nil
}

func (d *dummyCommitLogger) delete() error {
	return nil
}

func (d *dummyCommitLogger) sync() error {
	return nil
}

func mapFromDocPointers(id uint64, frequency, proplength float32) MapPair {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], id)
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(frequency))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(proplength))

	return MapPair{Key: buf[0:8], Value: buf[8:16]}
}

func docPointers(id uint64, frequency, proplength float32) terms.DocPointerWithScore {
	return terms.DocPointerWithScore{Id: id, Frequency: frequency, PropLength: proplength}
}

// moved from an intregation test, so we can use it both in unit and
// integration tests
func NewMapPairFromDocIdAndTf(docId uint64, tf float32, propLength float32, isTombstone bool) MapPair {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docId)

	value := make([]byte, 8)
	binary.LittleEndian.PutUint32(value[0:4], math.Float32bits(tf))
	binary.LittleEndian.PutUint32(value[4:8], math.Float32bits(propLength))

	return MapPair{
		Key:       key,
		Value:     value,
		Tombstone: isTombstone,
	}
}

func (kv MapPair) UpdateTf(tf float32, propLength float32) {
	kv.Value = make([]byte, 8)
	binary.LittleEndian.PutUint32(kv.Value[0:4], math.Float32bits(tf))
	binary.LittleEndian.PutUint32(kv.Value[4:8], math.Float32bits(propLength))
}

type kv struct {
	key    []byte
	values []MapPair
}

func validateMapPairListVsBlockMaxSearch(ctx context.Context, bucket *Bucket, expectedMultiKey []kv) error {
	view := bucket.getConsistentView()
	defer view.Release()
	return validateMapPairListVsBlockMaxSearchFromView(ctx, bucket, view, expectedMultiKey)
}

func validateMapPairListVsBlockMaxSearchFromView(ctx context.Context, bucket *Bucket, view BucketConsistentView, expectedMultiKey []kv) error {
	for _, termPair := range expectedMultiKey {
		expected := termPair.values
		mapKey := termPair.key
		// get more results, as there may be more results than expected on the result heap
		// during intermediate steps of insertions
		N := len(expected) * 10
		bm25config := schema.BM25Config{
			K1: 1.2,
			B:  0.75,
		}
		avgPropLen := 1.0
		queries := []string{string(mapKey)}
		duplicateTextBoosts := make([]int, 1)
		duplicateTextBoosts[0] = 1
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(N), nil, queries, "", 1, duplicateTextBoosts, bm25config)
		if err != nil {
			return fmt.Errorf("failed to create disk term: %w", err)
		}

		expectedSet := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		for _, diskTerm := range diskTerms {
			topKHeap, err := DoBlockMaxWand(ctx, N, diskTerm, avgPropLen, true, 1, 1, bucket.logger)
			if err != nil {
				return fmt.Errorf("failed to execute DoBlockMaxWand for diskTerm %v: %w", diskTerm, err)
			}
			for topKHeap.Len() > 0 {
				item := topKHeap.Pop()
				expectedSet[item.ID] = item.Value
			}
		}

		for _, val := range expected {
			docId := binary.BigEndian.Uint64(val.Key)
			if val.Tombstone {
				continue
			}
			freq := math.Float32frombits(binary.LittleEndian.Uint32(val.Value[0:4]))
			if _, ok := expectedSet[docId]; !ok {
				return fmt.Errorf("expected docId %v not found in topKHeap: %v", docId, expectedSet)
			}
			if expectedSet[docId][0].Frequency != freq {
				return fmt.Errorf("expected frequency %v but got %v", freq, expectedSet[docId][0].Frequency)
			}

		}
	}

	return nil
}

func validateMapPairListVsBlockMaxSearchFromSingleSegment(ctx context.Context, segment Segment, expectedMultiKey []kv) error {
	return validateMapPairListVsBlockMaxSearchFromSegments(ctx, []Segment{segment}, expectedMultiKey)
}

func validateMapPairListVsBlockMaxSearchFromSegments(ctx context.Context, segments []Segment, expectedMultiKey []kv) error {
	for _, termPair := range expectedMultiKey {
		expected := termPair.values
		mapKey := termPair.key
		// get more results, as there may be more results than expected on the result heap
		// during intermediate steps of insertions
		N := len(expected) * 10
		bm25config := schema.BM25Config{
			K1: 1.2,
			B:  0.75,
		}
		avgPropLen := 1.0
		duplicateTextBoosts := make([]int, 1)
		duplicateTextBoosts[0] = 1
		diskTerms := make([][]*SegmentBlockMax, 0, len(segments))
		for _, segment := range segments {
			bmws := segment.newSegmentBlockMax(mapKey, 0, 1, 1, nil, nil, 3, bm25config)
			diskTerms = append(diskTerms, []*SegmentBlockMax{bmws})
		}

		expectedSet := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		for _, diskTerm := range diskTerms {
			topKHeap, err := DoBlockMaxWand(ctx, N, diskTerm, avgPropLen, true, 1, 1, nil)
			if err != nil {
				return fmt.Errorf("failed to execute DoBlockMaxWand for diskTerm %v: %w", diskTerm, err)
			}
			for topKHeap.Len() > 0 {
				item := topKHeap.Pop()
				expectedSet[item.ID] = item.Value
			}
		}

		for _, val := range expected {
			docId := binary.BigEndian.Uint64(val.Key)
			if val.Tombstone {
				continue
			}
			freq := math.Float32frombits(binary.LittleEndian.Uint32(val.Value[0:4]))
			if _, ok := expectedSet[docId]; !ok {
				return fmt.Errorf("expected docId %v not found in topKHeap: %v", docId, expectedSet)
			}
			if expectedSet[docId][0].Frequency != freq {
				return fmt.Errorf("expected frequency %v but got %v", freq, expectedSet[docId][0].Frequency)
			}

		}

	}
	return nil
}
