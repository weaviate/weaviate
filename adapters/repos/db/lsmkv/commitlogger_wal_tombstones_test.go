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

package lsmkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestTombstoneWALReuse(t *testing.T) {
	walThresholds := []int{0, 4096}

	for _, thresh := range walThresholds {
		opts := []BucketOption{
			WithStrategy(StrategyInverted),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			WithMinWalThreshold(int64(thresh)),
		}
		t.Run(fmt.Sprintf("update with threshold %d", thresh), func(t *testing.T) {
			ctx := context.Background()
			dirName := t.TempDir()

			logger, _ := test.NewNullLogger()

			// initial bucket, always create segment, even if it is just a single entry
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
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
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...,
			)
			require.NoError(t, err)

			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docId)
			// for the inverted strategy, this adds a tombstone for docId 1
			// and expects all entries for docId 1 to be ignored from now on
			require.NoError(t, b.MapDeleteKey([]byte("word1"), key))
			require.NoError(t, b.Shutdown(ctx))

			dbFiles, walFiles = countDbAndWalFiles(t, dirName)

			if thresh == 0 {
				// with threshold 0, we always flush to a segment on shutdown
				require.Equal(t, dbFiles, 2)
				require.Equal(t, walFiles, 0)
			} else {
				// with higher thresholds, we may not have flushed yet
				require.Equal(t, dbFiles, 1)
				require.Equal(t, walFiles, 1)
			}

			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)

			res, err := b.MapList(ctx, []byte("word1"))
			require.NoError(t, err)
			require.Len(t, res, 0)

			res, err = b.MapList(ctx, []byte("word2"))
			require.NoError(t, err)
			require.Len(t, res, 0)
		})

		t.Run(fmt.Sprintf("delete with threshold %d", thresh), func(t *testing.T) {
			opts := []BucketOption{
				WithStrategy(StrategyInverted),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
				WithMinWalThreshold(int64(thresh)),
			}
			ctx := context.Background()
			dirName := t.TempDir()

			logger, _ := test.NewNullLogger()

			// initial bucket, always create segment, even if it is just a single entry
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)

			docId := uint64(1)
			docIdKey := make([]byte, 8)
			binary.BigEndian.PutUint64(docIdKey, docId)
			require.NoError(t, b.MapSet([]byte("word1"), NewMapPairFromDocIdAndTf(docId, 1, 1, false)))
			require.NoError(t, b.MapSet([]byte("word2"), NewMapPairFromDocIdAndTf(docId, 2, 2, false)))
			require.NoError(t, b.MapDeleteKey([]byte("word1"), docIdKey))
			require.NoError(t, b.MapDeleteKey([]byte("word2"), docIdKey))

			require.NoError(t, err)

			require.NoError(t, b.Shutdown(ctx))
			dbFiles, walFiles := countDbAndWalFiles(t, dirName)
			if thresh == 0 {
				// with threshold 0, we always flush to a segment on shutdown
				require.Equal(t, dbFiles, 1)
				require.Equal(t, walFiles, 0)
			} else {
				// with higher thresholds, we may not have flushed yet
				require.Equal(t, dbFiles, 0)
				require.Equal(t, walFiles, 1)
			}

			// test after reload
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)

			res, err := b.MapList(ctx, []byte("word1"))
			require.NoError(t, err)
			require.Len(t, res, 0)

			res, err = b.MapList(ctx, []byte("word2"))
			require.NoError(t, err)
			require.Len(t, res, 0)
		})
	}
}
