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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// BenchmarkGetBySecondary compares secondary-key lookups on d0 segments (where
// existsWithConsistentView recheck runs) vs d1 segments (where it is skipped).
func BenchmarkGetBySecondary(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.Run("d0_with_recheck", func(b *testing.B) {
				benchGetBySecondary(b, n, false)
			})
			b.Run("d1_skip_recheck", func(b *testing.B) {
				benchGetBySecondary(b, n, true)
			})
		})
	}
}

func benchGetBySecondary(b *testing.B, numKeys int, d1 bool) {
	b.Helper()
	ctx := context.Background()
	dirName := b.TempDir()
	logger, _ := test.NewNullLogger()

	opts := []BucketOption{
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
	}
	if d1 {
		opts = append(opts, WithWriteSegmentInfoIntoFileName(true))
	}

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(ctx) })
	bucket.SetMemtableThreshold(1e9)

	// Build secondary keys upfront
	secKeys := make([][]byte, numKeys)
	for i := range numKeys {
		key := []byte(fmt.Sprintf("key-%06d", i))
		secKey := []byte(fmt.Sprintf("sec-%06d", i))
		value := []byte(fmt.Sprintf("value-%06d", i))
		secKeys[i] = secKey

		err := bucket.Put(key, value, WithSecondaryKey(0, secKey))
		require.NoError(b, err)
	}
	require.NoError(b, bucket.FlushAndSwitch())

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		for _, sk := range secKeys {
			v, err := bucket.GetBySecondary(ctx, 0, sk)
			if err != nil {
				b.Fatal(err)
			}
			if v == nil {
				b.Fatal("unexpected nil value")
			}
		}
	}
}
