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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestInvertedNaNPropLength(t *testing.T) {
	ctx := context.Background()
	size := 1000

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted), WithForceCompaction(true))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	for i := 0; i < size; i++ {
		pair := NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)
		err := bucket.MapSet(key, pair)
		require.Nil(t, err)
	}
	err := bucket.FlushAndSwitch()
	require.Nil(t, err)
	avg, sum := bucket.disk.GetAveragePropertyLength()
	bavg, err := bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	for i := 0; i < size/2; i++ {
		pair := NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)
		err := bucket.MapDeleteKey(key, pair.Key)
		require.Nil(t, err)
	}

	err = bucket.FlushAndSwitch()
	require.Nil(t, err)
	avg, sum = bucket.disk.GetAveragePropertyLength()
	bavg, err = bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	for i := size / 2; i < size; i++ {
		pair := NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)
		err := bucket.MapDeleteKey(key, pair.Key)
		require.Nil(t, err)
	}

	err = bucket.FlushAndSwitch()
	require.Nil(t, err)
	avg, sum = bucket.disk.GetAveragePropertyLength()
	bavg, err = bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	t.Run("re-init bucket after flush", func(t *testing.T) {
		bucket.Shutdown(t.Context())
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	avg, sum = bucket.disk.GetAveragePropertyLength()
	bavg, err = bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	// verify that all prior writes are still correct
	kvs, err := bucket.MapList(ctx, key)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvs))

	ok := false
	for !ok {
		ok, err = bucket.disk.compactOnce()
		require.NoError(t, err)
	}

	avg, sum = bucket.disk.GetAveragePropertyLength()
	bavg, err = bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	t.Run("re-init bucket after compact", func(t *testing.T) {
		bucket.Shutdown(t.Context())
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	avg, sum = bucket.disk.GetAveragePropertyLength()
	bavg, err = bucket.GetAveragePropertyLength()
	require.Nil(t, err)
	require.Equal(t, 1.0, avg)
	require.Equal(t, 1.0, bavg)
	require.Equal(t, uint64(size), sum)

	// verify that all prior writes are still correct
	kvs, err = bucket.MapList(ctx, key)
	require.NoError(t, err)
	require.Equal(t, 0, len(kvs))
}
