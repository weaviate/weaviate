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
	"os"
	"testing"

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

	b, err := NewBucket(ctx, tmpDir, "", logger, nil,
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
		deleted, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.False(t, deleted)
	})

	t.Run("delete object", func(t *testing.T) {
		err = b.Delete(key)
		require.Nil(t, err)
	})

	t.Run("assert object was deleted", func(t *testing.T) {
		deleted, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.True(t, deleted)
	})

	t.Run("assert a nonexistent object is not detected as deleted", func(t *testing.T) {
		deleted, err := b.WasDeleted([]byte("DNE"))
		require.Nil(t, err)
		assert.False(t, deleted)
	})
}

func bucket_WasDeleted_CleanupTombstones(ctx context.Context, t *testing.T, opts []BucketOption) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, tmpDir, "", logger, nil,
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
		deleted, err := b.WasDeleted(key)
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})

	t.Run("delete object", func(t *testing.T) {
		err = b.Delete(key)
		require.Nil(t, err)
	})

	t.Run("fails on WasDeleted without keepTombstones set (after delete)", func(t *testing.T) {
		deleted, err := b.WasDeleted(key)
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})

	t.Run("fails on WasDeleted without keepTombstones set (non-existent key)", func(t *testing.T) {
		deleted, err := b.WasDeleted([]byte("DNE"))
		require.ErrorContains(t, err, "keepTombstones")
		require.False(t, deleted)
	})
}

func bucketReadsIntoMemory(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(b.dir)
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".bloom")
	assert.True(t, ok)

	_, ok = findFileWithExt(files, "secondary.0.bloom")
	assert.True(t, ok)

	b2, err := NewBucket(ctx, b.dir, "", logger, nil,
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
