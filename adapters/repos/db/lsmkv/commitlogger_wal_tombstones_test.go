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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestTombstoneWALReuseUpdate(t *testing.T) {
	strategy := StrategyInverted
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	// initial bucket, always create segment, even if it is just a single entry
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithMinWalThreshold(4096),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)

	docId := uint64(1)
	require.NoError(t, b.MapSet([]byte("word1"), NewMapPairFromDocIdAndTf(docId, 1, 1, false)))
	require.NoError(t, b.MapSet([]byte("word2"), NewMapPairFromDocIdAndTf(docId, 2, 2, false)))

	err = b.FlushAndSwitch()
	require.NoError(t, err)

	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 0)

	// test after reload
	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithMinWalThreshold(4096),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docId)
	// for the inverted strategy, this adds a tombstone for docId 1
	// and expects all entries for docId 1 to be ignored from now on
	require.NoError(t, b.MapDeleteKey([]byte("word1"), key))

	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 1)

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithMinWalThreshold(4096),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)

	res, err := b.MapList(ctx, []byte("word2"))
	require.NoError(t, err)
	require.Len(t, res, 0)
}

func TestTombstoneWALReuseDelete(t *testing.T) {
	strategy := StrategyInverted
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	// initial bucket, always create segment, even if it is just a single entry
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithMinWalThreshold(4096),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)

	docId := uint64(1)
	docIdKey := make([]byte, 8)
	binary.BigEndian.PutUint64(docIdKey, docId)
	require.NoError(t, b.MapSet([]byte("word1"), NewMapPairFromDocIdAndTf(docId, 1, 1, false)))
	require.NoError(t, b.MapSet([]byte("word2"), NewMapPairFromDocIdAndTf(docId, 2, 2, false)))
	require.NoError(t, b.MapDeleteKey([]byte("word1"), docIdKey))

	require.NoError(t, err)

	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 0)
	require.Equal(t, walFiles, 1)

	// test after reload
	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithMinWalThreshold(4096),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)

	res, err := b.MapList(ctx, []byte("word2"))
	require.NoError(t, err)
	require.Len(t, res, 0)
}
