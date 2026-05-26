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

// TestCompactor_AbortOnShouldAbort verifies the core contract introduced by
// weaviate/0-weaviate-issues#250: a compactor in flight observes the
// cyclemanager's shouldAbort signal within compactor.AbortCheckEveryN keys
// and bails — returning (false, nil) so the cycle treats it as a no-op
// iteration.
//
// The partial .tmp file is intentionally left behind. segment_group.init
// removes orphan .tmp segments at the next startup, and for the
// delete-path the parent dir is unlinked synchronously by the caller, so
// no synchronous remove is needed here.
//
// All strategies that go through SegmentGroup.compactOnce share the same
// bridge (shouldAbort → ctx + pre-cancel + watcher goroutine), so the
// test parametrises across the strategies that use the bucket Put/SetAdd
// /MapSet APIs to exercise each strategy's inner-loop ctx.Err() probe.
// Roaring and Inverted strategies share the same plumbing but require
// different setup; covered separately on a follow-up.
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

			// Two segments → compactOnce has a real pair to merge.
			for seg := 0; seg < 2; seg++ {
				tc.seed(t, bucket, seg, 5000)
				require.NoError(t, bucket.FlushAndSwitch())
			}
			require.GreaterOrEqual(t, len(bucket.disk.segments), 2,
				"need at least two segments on disk to exercise compactOnce")

			shouldAbort := func() bool { return true }

			start := time.Now()
			compacted, err := bucket.disk.compactOnce(shouldAbort)
			elapsed := time.Since(start)

			require.NoError(t, err)
			assert.False(t, compacted,
				"compactor must return compacted=false on abort (strategy=%s)", tc.strategy)
			assert.Less(t, elapsed, 3*time.Second,
				"abort should land within a few seconds (strategy=%s); observed %s",
				tc.strategy, elapsed)
		})
	}
}
