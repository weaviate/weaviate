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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCompactor_AbortOnShouldAbort exercises the abort contract for every
// strategy that SegmentGroup.compactOnce dispatches to. See
// weaviate/0-weaviate-issues#250.
func TestCompactor_AbortOnShouldAbort(t *testing.T) {
	cases := []struct {
		name     string
		strategy string
		seed     func(t *testing.T, bucket *Bucket, seg, n int)
	}{
		{
			name:     "replace",
			strategy: StrategyReplace,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					val := []byte(fmt.Sprintf("value-%d", i))
					require.NoError(t, bucket.Put(key, val))
				}
			},
		},
		{
			name:     "set",
			strategy: StrategySetCollection,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					vals := [][]byte{
						[]byte(fmt.Sprintf("val-a-%d", i)),
						[]byte(fmt.Sprintf("val-b-%d", i)),
					}
					require.NoError(t, bucket.SetAdd(key, vals))
				}
			},
		},
		{
			name:     "map",
			strategy: StrategyMapCollection,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					pair := MapPair{
						Key:   []byte(fmt.Sprintf("mk-%d", i)),
						Value: []byte(fmt.Sprintf("mv-%d", i)),
					}
					require.NoError(t, bucket.MapSet(key, pair))
				}
			},
		},
		{
			name:     "inverted",
			strategy: StrategyInverted,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-row-%08d", seg, i))
					pair := NewMapPairFromDocIdAndTf(uint64(seg*n+i), float32(i+1), float32(i+2), false)
					require.NoError(t, bucket.MapSet(key, pair))
				}
			},
		},
		{
			name:     "roaringset",
			strategy: StrategyRoaringSet,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					require.NoError(t, bucket.RoaringSetAddOne(key, uint64(seg*n+i)))
				}
			},
		},
		{
			name:     "roaringsetrange",
			strategy: StrategyRoaringSetRange,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				// roaringsetrange uses a single bitmap keyed by a value;
				// stamping `n` distinct ids per seg keeps both segments
				// non-empty so the merge has work to do.
				for i := 0; i < n; i++ {
					require.NoError(t, bucket.RoaringSetRangeAdd(uint64(seg*n+i), uint64(i)))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dirName := t.TempDir()

			bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(tc.strategy),
			)
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)
			bucket.SetMemtableThreshold(1e9)

			for seg := 0; seg < 2; seg++ {
				tc.seed(t, bucket, seg, 5000)
				require.NoError(t, bucket.FlushAndSwitch())
			}
			require.GreaterOrEqual(t, len(bucket.disk.segments), 2,
				"need at least two segments on disk to exercise compactOnce")

			// direct path: pre-cancelled ctx into the inner compactor
			t.Run("direct", func(t *testing.T) {
				abortCtx, cancel := context.WithCancel(ctx)
				cancel()
				start := time.Now()
				compacted, err := bucket.disk.compactOnce(abortCtx)
				elapsed := time.Since(start)
				require.NoError(t, err)
				assert.False(t, compacted)
				assert.Less(t, elapsed, 3*time.Second, "observed %s", elapsed)
			})

			// bridge path: shouldAbort=true exercised through compactOrCleanup;
			// the bridge inside compactOrCleanup pre-cancels the ctx so the
			// compactor sees the abort on its first sample.
			t.Run("bridge", func(t *testing.T) {
				start := time.Now()
				didWork := bucket.disk.compactOrCleanup(func() bool { return true })
				elapsed := time.Since(start)
				assert.False(t, didWork)
				assert.Less(t, elapsed, 3*time.Second, "observed %s", elapsed)
			})
		})
	}
}
