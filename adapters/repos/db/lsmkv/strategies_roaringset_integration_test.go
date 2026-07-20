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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestRoaringSetStrategy(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "roaringsetInsertAndSetAdd",
			f:    roaringsetInsertAndSetAdd,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			},
		},
		{
			name: "roaringsetInsertAndSetAdd in memory",
			f:    roaringsetInsertAndSetAdd,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepMergedSegmentsInMemory(true),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			},
		},
		{
			name: "roaringsetReopenInMemory",
			f:    roaringsetReopenInMemory,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepMergedSegmentsInMemory(true),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			},
		},
	}
	tests.run(ctx, t)
}

func roaringsetInsertAndSetAdd(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		defer b.Shutdown(ctx)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetAddList(key1, orig1)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key2, orig2)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key3, orig3)
			require.Nil(t, err)

			res, release, err := b.RoaringSetGet(context.Background(), key1)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig1 {
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key2)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig2 {
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key3)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig3 {
				assert.True(t, res.Contains(testVal))
			}
		})

		t.Run("extend some, delete some, keep some", func(t *testing.T) {
			additions2 := []uint64{5}
			removal3 := uint64(5)

			err = b.RoaringSetAddList(key2, additions2)
			require.Nil(t, err)
			err = b.RoaringSetRemoveOne(key3, removal3)
			require.Nil(t, err)

			res, release, err := b.RoaringSetGet(context.Background(), key1)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{1, 2} { // unchanged values
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key2)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{3, 4, 5} { // extended with 5
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key3)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{6} { // fewer remain
				assert.True(t, res.Contains(testVal))
			}
			for _, testVal := range []uint64{5} { // no longer contained
				assert.False(t, res.Contains(testVal))
			}
		})
	})

	t.Run("with a single flush in between updates", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		defer b.Shutdown(ctx)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetAddList(key1, orig1)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key2, orig2)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key3, orig3)
			require.Nil(t, err)

			res, release, err := b.RoaringSetGet(context.Background(), key1)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig1 {
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key2)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig2 {
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key3)
			require.NoError(t, err)
			defer release()
			for _, testVal := range orig3 {
				assert.True(t, res.Contains(testVal))
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("extend some, delete some, keep some", func(t *testing.T) {
			additions2 := []uint64{5}
			removal3 := uint64(5)

			err = b.RoaringSetAddList(key2, additions2)
			require.Nil(t, err)
			err = b.RoaringSetRemoveOne(key3, removal3)
			require.Nil(t, err)

			res, release, err := b.RoaringSetGet(context.Background(), key1)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{1, 2} { // unchanged values
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key2)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{3, 4, 5} { // extended with 5
				assert.True(t, res.Contains(testVal))
			}

			res, release, err = b.RoaringSetGet(context.Background(), key3)
			require.NoError(t, err)
			defer release()
			for _, testVal := range []uint64{6} { // fewer remain
				assert.True(t, res.Contains(testVal))
			}
			for _, testVal := range []uint64{5} { // no longer contained
				assert.False(t, res.Contains(testVal))
			}
		})
	})
}

// roaringsetReopenInMemory pins the startup build of the merged in-memory
// segment from real on-disk segments: two rounds of additions/deletions are
// flushed into two segments, the bucket is shut down and reopened, and reads
// must then reflect the exact merged state without ever having gone through a
// memtable in this bucket instance.
func roaringsetReopenInMemory(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	key1 := []byte("test-reopen-key-1")
	key2 := []byte("test-reopen-key-2")
	key3 := []byte("test-reopen-key-3")

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	// so big it effectively never triggers as part of this test
	b.SetMemtableThreshold(1e9)

	t.Run("first round of additions and deletions", func(t *testing.T) {
		err = b.RoaringSetAddList(key1, []uint64{1, 2, 3})
		require.Nil(t, err)
		err = b.RoaringSetAddList(key2, []uint64{10, 11})
		require.Nil(t, err)
		err = b.RoaringSetAddList(key3, []uint64{20, 21})
		require.Nil(t, err)
		err = b.RoaringSetRemoveOne(key3, 20)
		require.Nil(t, err)
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, b.FlushAndSwitch())
	})

	t.Run("second round adds and deletes previously flushed ids", func(t *testing.T) {
		err = b.RoaringSetAddList(key1, []uint64{4})
		require.Nil(t, err)
		err = b.RoaringSetRemoveOne(key1, 1) // deletes an id flushed in round one
		require.Nil(t, err)
		err = b.RoaringSetRemoveOne(key2, 10) // deletes every id of key2
		require.Nil(t, err)
		err = b.RoaringSetRemoveOne(key2, 11)
		require.Nil(t, err)
		err = b.RoaringSetAddList(key3, []uint64{22})
		require.Nil(t, err)
	})

	t.Run("flush to disk again", func(t *testing.T) {
		require.Nil(t, b.FlushAndSwitch())
	})

	require.Nil(t, b.Shutdown(ctx))

	t.Run("reopen and verify merged state built from disk segments", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		defer b.Shutdown(ctx)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		// positive control: segments were flushed before shutdown, so the
		// startup build must have engaged and produced a non-empty merged
		// structure — otherwise the assertions below would also pass on the
		// plain disk read path and prove nothing about the in-memory one
		require.NotNil(t, b.disk.roaringSetSegmentInMemory)
		require.Positive(t, b.disk.roaringSetSegmentInMemory.Size())

		res, release, err := b.RoaringSetGet(context.Background(), key1)
		require.NoError(t, err)
		assert.Equal(t, []uint64{2, 3, 4}, res.ToArray()) // 1 deleted in round two, 4 added
		release()

		res, release, err = b.RoaringSetGet(context.Background(), key2)
		require.NoError(t, err)
		assert.Empty(t, res.ToArray()) // deleted to empty, no key must remain
		release()

		res, release, err = b.RoaringSetGet(context.Background(), key3)
		require.NoError(t, err)
		assert.Equal(t, []uint64{21, 22}, res.ToArray()) // 20 deleted in round one, 22 added in round two
		release()

		t.Run("fresh writes layer the active memtable on the built structure", func(t *testing.T) {
			err = b.RoaringSetAddList(key1, []uint64{100})
			require.Nil(t, err)
			err = b.RoaringSetAddList(key2, []uint64{200}) // revives the deleted-to-empty key
			require.Nil(t, err)

			// same control after fresh writes: they layer on the active
			// memtable, the merged structure must stay engaged
			require.NotNil(t, b.disk.roaringSetSegmentInMemory)
			require.Positive(t, b.disk.roaringSetSegmentInMemory.Size())

			res, release, err := b.RoaringSetGet(context.Background(), key1)
			require.NoError(t, err)
			defer release()
			assert.Equal(t, []uint64{2, 3, 4, 100}, res.ToArray())

			res, release, err = b.RoaringSetGet(context.Background(), key2)
			require.NoError(t, err)
			defer release()
			assert.Equal(t, []uint64{200}, res.ToArray())
		})
	})
}
