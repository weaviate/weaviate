//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompactionSetSkipKeys(t *testing.T) {
	maxSize := 5

	keysToSkip := map[string]struct{}{}
	ctx := t.Context()
	opts := []BucketOption{
		WithStrategy(StrategySetCollection),
		WithForceCompaction(true),
		WithShouldSkipKeyFunction(
			func(key []byte, ctx context.Context) (bool, error) {
				if _, ok := keysToSkip[string(key)]; ok {
					return true, nil
				}
				return false, nil
			},
		),
	}

	var bucket *Bucket

	value1 := []byte("value-01")
	value2 := []byte("value-02")
	values := [][]byte{value1, value2}

	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})
	for i := 0; i < maxSize; i++ {
		t.Run(fmt.Sprintf("%v segments", i), func(t *testing.T) {
			t.Run("import data", func(t *testing.T) {
				key := []byte(fmt.Sprintf("key-delete-on-compaction-%d", i))
				err := bucket.SetAdd(key, values)
				require.Nil(t, err)

				key = []byte(fmt.Sprintf("key-delete-on-flush-%d", i))
				err = bucket.SetAdd(key, values)
				require.Nil(t, err)
			})

			t.Run("test and flush data", func(t *testing.T) {
				if i > 0 {
					keysToSkip[fmt.Sprintf("key-delete-on-compaction-%d", i-1)] = struct{}{}
				}
				keysToSkip[fmt.Sprintf("key-delete-on-flush-%d", i)] = struct{}{}

				res, err := bucket.SetList([]byte(fmt.Sprintf("key-delete-on-flush-%d", i)))
				require.Nil(t, err)
				require.Len(t, res, 2)

				require.Nil(t, bucket.FlushAndSwitch())

				res, err = bucket.SetList([]byte(fmt.Sprintf("key-delete-on-flush-%d", i)))
				require.Nil(t, err)
				require.Len(t, res, 0)

				key := []byte(fmt.Sprintf("key-delete-on-compaction-%d", i))
				res, err = bucket.SetList(key)
				require.Nil(t, err)
				require.Len(t, res, 2)

				if i > 0 {
					key = []byte(fmt.Sprintf("key-delete-on-compaction-%d", i-1))
					res, err := bucket.SetList(key)
					require.Nil(t, err)
					require.Len(t, res, 2)
				}

				if i > 1 {
					for j := 0; j < i-1; j++ {
						key = []byte(fmt.Sprintf("key-delete-on-compaction-%d", j))
						res, err := bucket.SetList(key)
						require.Nil(t, err)
						require.Len(t, res, 0)
					}
				}
			})
			t.Run("compact until no longer eligible", func(t *testing.T) {
				var compacted bool
				var err error
				for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
				}
				require.Nil(t, err)
			})

			t.Run("test after compaction", func(t *testing.T) {
				if i > 1 {
					for j := 0; j < i; j++ {
						key := []byte(fmt.Sprintf("key-delete-on-compaction-%d", i-1))
						res, err := bucket.SetList(key)
						require.Nil(t, err)
						require.Len(t, res, 0)
					}
				}
			})
		})
	}
}
