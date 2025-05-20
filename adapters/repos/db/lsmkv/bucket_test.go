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
			actualActive := b.memtableNetCount(tt.current, tt.previous)
			assert.Equal(t, tt.expectedNetActive, actualActive)

			if tt.previous != nil {
				actualPrevious := b.memtableNetCount(tt.previous, nil)
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

func TestBucketWalReload(t *testing.T) {
	for _, strategy := range []string{StrategyReplace, StrategySetCollection, StrategyMapCollection, StrategyRoaringSet, StrategyRoaringSetRange} {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			dirName := t.TempDir()

			logger, _ := test.NewNullLogger()

			secondaryIndicesCount := uint16(0)
			if strategy == StrategyReplace {
				secondaryIndicesCount = 1
			}

			// initial bucket, always create segment, even if it is just a single entry
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount), WithMinWalThreshold(4096))
			require.NoError(t, err)

			if strategy == StrategyReplace {
				require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1"))))
			} else if strategy == StrategySetCollection {
				require.NoError(t, b.SetAdd([]byte("hello1"), [][]byte{[]byte("world1")}))
			} else if strategy == StrategyRoaringSet {
				require.NoError(t, b.RoaringSetAddOne([]byte("hello1"), uint64(1)))
			} else if strategy == StrategyMapCollection {
				require.NoError(t, b.MapSet([]byte("hello1"), MapPair{Key: []byte("hello1"), Value: []byte("world1")}))
			} else if strategy == StrategyRoaringSetRange {
				require.NoError(t, b.RoaringSetRangeAdd(uint64(1), uint64(1)))
			} else {
				require.Fail(t, "unknown strategy %s", strategy)
			}
			testBucketContent(t, strategy, b, 2)

			require.NoError(t, b.Shutdown(ctx))

			entries, err := os.ReadDir(dirName)
			require.NoError(t, err)
			require.Len(t, entries, 1, "single wal file should be created")

			testBucketContent(t, strategy, b, 2)

			// start fresh with a new memtable, new entries will stay in wal until size is reached
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount), WithMinWalThreshold(4096))
			require.NoError(t, err)

			if strategy == StrategyReplace {
				require.NoError(t, b.Put([]byte("hello2"), []byte("world2"), WithSecondaryKey(0, []byte("bonjour2"))))
			} else if strategy == StrategySetCollection {
				require.NoError(t, b.SetAdd([]byte("hello2"), [][]byte{[]byte("world2")}))
			} else if strategy == StrategyRoaringSet {
				require.NoError(t, b.RoaringSetAddOne([]byte("hello2"), uint64(2)))
			} else if strategy == StrategyMapCollection {
				require.NoError(t, b.MapSet([]byte("hello2"), MapPair{Key: []byte("hello2"), Value: []byte("world2")}))
			} else if strategy == StrategyRoaringSetRange {
				require.NoError(t, b.RoaringSetRangeAdd(uint64(2), uint64(2)))
			}
			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			fileTypes := map[string]int{}
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 0, fileTypes[".db"], "no segment file")
			require.Equal(t, 1, fileTypes[".wal"], "single wal file")

			// will load wal and reuse memtable
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(1), WithMinWalThreshold(4096))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 3)

			if strategy == StrategyReplace {
				require.NoError(t, b.Put([]byte("hello3"), []byte("world3"), WithSecondaryKey(0, []byte("bonjour3"))))
			} else if strategy == StrategySetCollection {
				require.NoError(t, b.SetAdd([]byte("hello3"), [][]byte{[]byte("world3")}))
			} else if strategy == StrategyRoaringSet {
				require.NoError(t, b.RoaringSetAddOne([]byte("hello3"), uint64(3)))
			} else if strategy == StrategyMapCollection {
				require.NoError(t, b.MapSet([]byte("hello3"), MapPair{Key: []byte("hello3"), Value: []byte("world3")}))
			} else if strategy == StrategyRoaringSetRange {
				require.NoError(t, b.RoaringSetRangeAdd(uint64(3), uint64(3)))
				require.NoError(t, err)
			}
			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			clear(fileTypes)
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 0, fileTypes[".db"], "no segment file")
			require.Equal(t, 1, fileTypes[".wal"], "single wal file")

			// now add a lot of entries to hit .wal file limit
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount), WithMinWalThreshold(4096))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 4)

			for i := 4; i < 120; i++ { // larger than min .wal threshold
				if strategy == StrategyReplace {
					require.NoError(t, b.Put([]byte(fmt.Sprintf("hello%d", i)), []byte(fmt.Sprintf("world%d", i)), WithSecondaryKey(0, []byte(fmt.Sprintf("bonjour%d", i)))))
				} else if strategy == StrategySetCollection {
					require.NoError(t, b.SetAdd([]byte(fmt.Sprintf("hello%d", i)), [][]byte{[]byte(fmt.Sprintf("world%d", i))}))
				} else if strategy == StrategyRoaringSet {
					require.NoError(t, b.RoaringSetAddOne([]byte(fmt.Sprintf("hello%d", i)), uint64(i)))
				} else if strategy == StrategyMapCollection {
					require.NoError(t, b.MapSet([]byte(fmt.Sprintf("hello%d", i)), MapPair{Key: []byte(fmt.Sprintf("hello%d", i)), Value: []byte(fmt.Sprintf("world%d", i))}))
				} else if strategy == StrategyRoaringSetRange {
					require.NoError(t, b.RoaringSetRangeAdd(uint64(4), uint64(4)))
				}
			}
			testBucketContent(t, strategy, b, 120)

			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			clear(fileTypes)
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 1, fileTypes[".db"], "no segment file")
			require.Equal(t, 0, fileTypes[".wal"], "single wal file")

			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount), WithMinWalThreshold(4096))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 120)
		})
	}
}

func testBucketContent(t *testing.T, strategy string, b *Bucket, maxObject int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i < maxObject; i++ {
		key := []byte(fmt.Sprintf("hello%d", i))
		val := []byte(fmt.Sprintf("world%d", i))
		if strategy == StrategyReplace {
			get, err := b.Get(key)
			require.NoError(t, err)
			require.Equal(t, val, get)

			secondary, err := b.GetBySecondary(0, []byte(fmt.Sprintf("bonjour%d", i)))
			require.NoError(t, err)
			require.Equal(t, val, secondary)
		} else if strategy == StrategySetCollection {
			get, err := b.SetList(key)
			require.NoError(t, err)
			require.Equal(t, val, get[0])
		} else if strategy == StrategyRoaringSet {
			get, err := b.RoaringSetGet(key)
			require.NoError(t, err)
			require.True(t, get.Contains(uint64(i)))
		} else if strategy == StrategyRoaringSetRange {
			//_, err :=  b.Rang
			//require.NoError(t,err)
		} else if strategy == StrategyMapCollection {
			get, err := b.MapList(ctx, key)
			require.NoError(t, err)
			require.Equal(t, val, get[0].Value)
		}
	}
}
