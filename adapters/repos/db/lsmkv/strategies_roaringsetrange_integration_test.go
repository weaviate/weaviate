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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestRoaringSetRangeStrategy(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "roaringsetrangeInsertAndSetAdd",
			f:    roaringsetrangeInsertAndSetAdd,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSetRange),
			},
		},
	}
	tests.run(ctx, t)
}

func roaringsetrangeInsertAndSetAdd(ctx context.Context, t *testing.T, opts []BucketOption) {
	t.Run("memtable-only", func(t *testing.T) {
		dirName := t.TempDir()
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		defer b.Shutdown(ctx)

		key1 := uint64(1)
		key2 := uint64(2)
		key3 := uint64(3)

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetRangeAdd(key1, orig1...)
			require.Nil(t, err)
			err = b.RoaringSetRangeAdd(key2, orig2...)
			require.Nil(t, err)
			err = b.RoaringSetRangeAdd(key3, orig3...)
			require.Nil(t, err)

			reader := b.ReaderRoaringSetRange()
			defer reader.Close()

			bm1, release1, err := reader.Read(testCtx(), key1, filters.OperatorEqual)
			require.NoError(t, err)
			defer release1()
			assert.ElementsMatch(t, orig1, bm1.ToArray())

			bm2, release2, err := reader.Read(testCtx(), key2, filters.OperatorEqual)
			require.NoError(t, err)
			defer release2()
			assert.ElementsMatch(t, orig2, bm2.ToArray())

			bm3, release3, err := reader.Read(testCtx(), key3, filters.OperatorEqual)
			require.NoError(t, err)
			defer release3()
			assert.ElementsMatch(t, orig3, bm3.ToArray())
		})

		t.Run("extend some, delete some, keep some", func(t *testing.T) {
			deletions1 := []uint64{1}
			additions2 := []uint64{5, 7}

			err = b.RoaringSetRangeRemove(key1, deletions1...)
			require.NoError(t, err)
			err = b.RoaringSetRangeAdd(key2, additions2...) // implicit removal from key3 (5)
			require.NoError(t, err)

			reader := b.ReaderRoaringSetRange()
			defer reader.Close()

			bm1, release1, err := reader.Read(testCtx(), key1, filters.OperatorEqual)
			require.NoError(t, err)
			defer release1()
			assert.ElementsMatch(t, []uint64{2}, bm1.ToArray()) // unchanged values

			bm2, release2, err := reader.Read(testCtx(), key2, filters.OperatorEqual)
			require.NoError(t, err)
			defer release2()
			assert.ElementsMatch(t, []uint64{3, 4, 5, 7}, bm2.ToArray()) // extended with 5

			bm3, release3, err := reader.Read(testCtx(), key3, filters.OperatorEqual)
			require.NoError(t, err)
			defer release3()
			assert.ElementsMatch(t, []uint64{6}, bm3.ToArray()) // fewer remain
		})
	})

	t.Run("with a single flush in between updates", func(t *testing.T) {
		dirName := t.TempDir()
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		defer b.Shutdown(ctx)

		key1 := uint64(1)
		key2 := uint64(2)
		key3 := uint64(3)

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetRangeAdd(key1, orig1...)
			require.Nil(t, err)
			err = b.RoaringSetRangeAdd(key2, orig2...)
			require.Nil(t, err)
			err = b.RoaringSetRangeAdd(key3, orig3...)
			require.Nil(t, err)

			reader := b.ReaderRoaringSetRange()
			defer reader.Close()

			bm1, release1, err := reader.Read(testCtx(), key1, filters.OperatorEqual)
			require.NoError(t, err)
			defer release1()
			assert.ElementsMatch(t, orig1, bm1.ToArray())

			bm2, release2, err := reader.Read(testCtx(), key2, filters.OperatorEqual)
			require.NoError(t, err)
			defer release2()
			assert.ElementsMatch(t, orig2, bm2.ToArray())

			bm3, release3, err := reader.Read(testCtx(), key3, filters.OperatorEqual)
			require.NoError(t, err)
			defer release3()
			assert.ElementsMatch(t, orig3, bm3.ToArray())
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("extend some, delete some, keep some", func(t *testing.T) {
			deletions1 := []uint64{1}
			additions2 := []uint64{5, 7}

			err = b.RoaringSetRangeRemove(key1, deletions1...)
			require.NoError(t, err)
			err = b.RoaringSetRangeAdd(key2, additions2...) // implicit removal from key3 (5)
			require.NoError(t, err)

			reader := b.ReaderRoaringSetRange()
			defer reader.Close()

			bm1, release1, err := reader.Read(testCtx(), key1, filters.OperatorEqual)
			require.NoError(t, err)
			defer release1()
			assert.ElementsMatch(t, []uint64{2}, bm1.ToArray()) // unchanged values

			bm2, release2, err := reader.Read(testCtx(), key2, filters.OperatorEqual)
			require.NoError(t, err)
			defer release2()
			assert.ElementsMatch(t, []uint64{3, 4, 5, 7}, bm2.ToArray()) // extended with 5

			bm3, release3, err := reader.Read(testCtx(), key3, filters.OperatorEqual)
			require.NoError(t, err)
			defer release3()
			assert.ElementsMatch(t, []uint64{6}, bm3.ToArray()) // fewer remain
		})
	})
}
