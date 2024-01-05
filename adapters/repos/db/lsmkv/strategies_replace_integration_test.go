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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestReplaceStrategy(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "replaceInsertAndUpdate",
			f:    replaceInsertAndUpdate,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "replaceInsertAndUpdate_WithSecondaryKeys",
			f:    replaceInsertAndUpdate_WithSecondaryKeys,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
			},
		},
		{
			name: "replaceInsertAndDelete",
			f:    replaceInsertAndDelete,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "replaceCursors",
			f:    replaceCursors,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
	}
	tests.run(ctx, t)
}

func replaceInsertAndUpdate(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			assert.Equal(t, 3, b.Count())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			assert.Equal(t, 3, b.Count())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("with single flush in between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("count only objects on disk segment", func(t *testing.T) {
			assert.Equal(t, 3, b.Count())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			// make sure that the updates aren't counted as additions
			assert.Equal(t, 3, b.Count())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, replaced2, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, replaced3, res)
		})
	})

	t.Run("with a flush after the initial write and after the update", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			// Flush before verifying!
			require.Nil(t, b.FlushAndSwitch())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})

		t.Run("count objects over several segments", func(t *testing.T) {
			assert.Equal(t, 3, b.Count())
		})
	})

	t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			res, err := b2.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b2.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b2.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)

			// count objects over several segments after disk read
			assert.Equal(t, 3, b2.Count())
		})
	})
}

func replaceInsertAndUpdate_WithSecondaryKeys(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2")
			secondaryKey3 := []byte("secondary-key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1, WithSecondaryKey(0, secondaryKey1))
			require.Nil(t, err)
			err = b.Put(key2, orig2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, orig3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)

			res, err := b.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some values, keep one - secondary keys not changed", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2")
			secondaryKey3 := []byte("secondary-key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, replaced3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)

			res, err := b.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})

		t.Run("replace the secondary keys on an update", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			err = b.Put(key2, replaced2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, replaced3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)

			// verify you can find by updated secondary keys
			res, err := b.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("with single flush in between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2")
			secondaryKey3 := []byte("secondary-key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1, WithSecondaryKey(0, secondaryKey1))
			require.Nil(t, err)
			err = b.Put(key2, orig2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, orig3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace the secondary keys on an update", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			err = b.Put(key2, replaced2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, replaced3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)

			// verify you can find by updated secondary keys
			res, err := b.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("with a flush after initial write and update", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2")
			secondaryKey3 := []byte("secondary-key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1, WithSecondaryKey(0, secondaryKey1))
			require.Nil(t, err)
			err = b.Put(key2, orig2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, orig3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace the secondary keys on an update", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			err = b.Put(key2, replaced2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, replaced3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("verify again", func(t *testing.T) {
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			// verify you can find by updated secondary keys
			res, err := b.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("update in memtable then do an orderly shutdown and reinit", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2")
			secondaryKey3 := []byte("secondary-key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1, WithSecondaryKey(0, secondaryKey1))
			require.Nil(t, err)
			err = b.Put(key2, orig2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, orig3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)
		})

		t.Run("replace the secondary keys on an update", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			err = b.Put(key2, replaced2, WithSecondaryKey(0, secondaryKey2))
			require.Nil(t, err)
			err = b.Put(key3, replaced3, WithSecondaryKey(0, secondaryKey3))
			require.Nil(t, err)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.Shutdown(context.Background()))
		})

		t.Run("init a new one and verify", func(t *testing.T) {
			b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			secondaryKey1 := []byte("secondary-key-1")
			secondaryKey2 := []byte("secondary-key-2-updated")
			secondaryKey3 := []byte("secondary-key-3-updated")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("twice updated value for key2")
			replaced3 := []byte("twice updated value for key3")

			// verify you can find by updated secondary keys
			res, err := b2.GetBySecondary(0, secondaryKey1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b2.GetBySecondary(0, secondaryKey2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b2.GetBySecondary(0, secondaryKey3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})
}

func replaceInsertAndDelete(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})

		t.Run("count objects", func(t *testing.T) {
			assert.Equal(t, 1, b.Count())
		})
	})

	t.Run("with single flush in between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})

		t.Run("count objects", func(t *testing.T) {
			assert.Equal(t, 1, b.Count())
		})
	})

	t.Run("with flushes after initial write and delete", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			// Flush again!
			require.Nil(t, b.FlushAndSwitch())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})

		t.Run("count objects", func(t *testing.T) {
			assert.Equal(t, 1, b.Count())
		})
	})
}

func replaceCursors(ctx context.Context, t *testing.T, opts []BucketOption) {
	t.Run("memtable-only", func(t *testing.T) {
		r := getRandomSeed()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			pairs := 20
			keys := make([][]byte, pairs)
			values := make([][]byte, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				values[i] = []byte(fmt.Sprintf("value-%03d", i))
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("value-018"),
				[]byte("value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-000"),
				[]byte("value-001"),
				[]byte("value-002"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("replace a key", func(t *testing.T) {
			key := []byte("key-002")
			value := []byte("value-002-updated")

			err = b.Put(key, value)
			require.Nil(t, err)

			expectedKeys := [][]byte{
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-001"),
				[]byte("value-002-updated"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("delete a key", func(t *testing.T) {
			key := []byte("key-002")

			err = b.Delete(key)
			require.Nil(t, err)

			t.Run("seek to a specific key", func(t *testing.T) {
				expectedKeys := [][]byte{
					[]byte("key-001"),
					[]byte("key-003"),
				}
				expectedValues := [][]byte{
					[]byte("value-001"),
					[]byte("value-003"),
				}
				var retrievedKeys [][]byte
				var retrievedValues [][]byte
				c := b.Cursor()
				defer c.Close()
				retrieved := 0
				for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
					retrieved++
					retrievedKeys = copyAndAppend(retrievedKeys, k)
					retrievedValues = copyAndAppend(retrievedValues, v)
				}

				assert.Equal(t, expectedKeys, retrievedKeys)
				assert.Equal(t, expectedValues, retrievedValues)
			})

			t.Run("seek to first key", func(t *testing.T) {
				expectedKeys := [][]byte{
					[]byte("key-000"),
					[]byte("key-001"),
					[]byte("key-003"),
				}
				expectedValues := [][]byte{
					[]byte("value-000"),
					[]byte("value-001"),
					[]byte("value-003"),
				}

				var retrievedKeys [][]byte
				var retrievedValues [][]byte
				c := b.Cursor()
				defer c.Close()
				retrieved := 0
				for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
					retrieved++
					retrievedKeys = copyAndAppend(retrievedKeys, k)
					retrievedValues = copyAndAppend(retrievedValues, v)
				}

				assert.Equal(t, expectedKeys, retrievedKeys)
				assert.Equal(t, expectedValues, retrievedValues)
			})
		})

		t.Run("delete the first key", func(t *testing.T) {
			key := []byte("key-000")

			err = b.Delete(key)
			require.Nil(t, err)

			t.Run("seek to a specific key", func(t *testing.T) {
				expectedKeys := [][]byte{
					[]byte("key-001"),
					[]byte("key-003"),
				}
				expectedValues := [][]byte{
					[]byte("value-001"),
					[]byte("value-003"),
				}
				var retrievedKeys [][]byte
				var retrievedValues [][]byte
				c := b.Cursor()
				defer c.Close()
				retrieved := 0
				for k, v := c.Seek([]byte("key-000")); k != nil && retrieved < 2; k, v = c.Next() {
					retrieved++
					retrievedKeys = copyAndAppend(retrievedKeys, k)
					retrievedValues = copyAndAppend(retrievedValues, v)
				}

				assert.Equal(t, expectedKeys, retrievedKeys)
				assert.Equal(t, expectedValues, retrievedValues)
			})

			t.Run("seek to first key", func(t *testing.T) {
				expectedKeys := [][]byte{
					[]byte("key-001"),
					[]byte("key-003"),
				}
				expectedValues := [][]byte{
					[]byte("value-001"),
					[]byte("value-003"),
				}

				var retrievedKeys [][]byte
				var retrievedValues [][]byte
				c := b.Cursor()
				defer c.Close()
				retrieved := 0
				for k, v := c.First(); k != nil && retrieved < 2; k, v = c.Next() {
					retrieved++
					retrievedKeys = copyAndAppend(retrievedKeys, k)
					retrievedValues = copyAndAppend(retrievedValues, v)
				}

				assert.Equal(t, expectedKeys, retrievedKeys)
				assert.Equal(t, expectedValues, retrievedValues)
			})
		})
	})

	t.Run("with a single flush", func(t *testing.T) {
		r := getRandomSeed()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			pairs := 20
			keys := make([][]byte, pairs)
			values := make([][]byte, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				values[i] = []byte(fmt.Sprintf("value-%03d", i))
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("value-018"),
				[]byte("value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-000"),
				[]byte("value-001"),
				[]byte("value-002"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})

	t.Run("mixing several disk segments and memtable - with updates", func(t *testing.T) {
		r := getRandomSeed()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("first third (%3==0)", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 0 {
					keys = copyAndAppend(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = copyAndAppend(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("second third (%3==1)", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 1 {
					keys = copyAndAppend(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = copyAndAppend(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("update something that was already written in segment 1", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-000"), []byte("updated-value-000")))
			require.Nil(t, b.Delete([]byte("key-003")))
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("third third (%3==2) memtable only", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 2 {
					keys = copyAndAppend(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = copyAndAppend(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}

			// no flush for this one, so this segment stays in the memtable
		})

		t.Run("update something that was already written previously", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-000"), []byte("twice-updated-value-000")))
			require.Nil(t, b.Put([]byte("key-001"), []byte("once-updated-value-001")))
			require.Nil(t, b.Put([]byte("key-019"), []byte("once-updated-value-019")))
			require.Nil(t, b.Delete([]byte("key-018")))
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				// key-018 deleted
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				// key-003 was deleted
				[]byte("key-004"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("value-004"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("re-add the deleted keys", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-003"), []byte("readded-003")))
			require.Nil(t, b.Put([]byte("key-018"), []byte("readded-018")))
			// tombstones are now only in memtable
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("readded-018"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				[]byte("key-003"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("readded-003"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("perform a final flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("readded-018"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				[]byte("key-003"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("readded-003"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})

	// This test is inspired by unusual behavior encountered as part of the
	// evaluation of gh-1569 where a delete could sometimes lead to no data after
	// a restart which was caused by the disk segment cursor's .first() method
	// not returning the correct key. Thus we'd have a null-key with a tombstone
	// which would override whatever is the real "first" key, since null is
	// always smaller
	t.Run("with deletes as latest in some segments", func(t *testing.T) {
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("add new datapoint", func(t *testing.T) {
			err := b.Put([]byte("key-1"), []byte("value-1"))
			require.Nil(t, err)
		})

		t.Run("add datapoint and flush", func(t *testing.T) {
			err := b.Put([]byte("key-8"), []byte("value-8"))
			require.Nil(t, err)

			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete datapoint and flush", func(t *testing.T) {
			err := b.Delete([]byte("key-8"))
			// note that we are deleting the key with the 'higher' key, so a missing
			// key on the delete would definitely be mismatched. If we had instead
			// the deleted the first key, the incorrect tombstone would have been
			// correct by coincidence
			require.Nil(t, err)

			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("verify", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-1"),
			}
			expectedValues := [][]byte{
				[]byte("value-1"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = copyAndAppend(retrievedKeys, k)
				retrievedValues = copyAndAppend(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})
}

func copyAndAppend(list [][]byte, elem []byte) [][]byte {
	elemCopy := make([]byte, len(elem))
	copy(elemCopy, elem)
	return append(list, elemCopy)
}
