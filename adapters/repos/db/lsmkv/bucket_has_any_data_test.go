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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucketHasAnyData pins the cheap structural emptiness probe used by
// shard-init rangeable-readiness reconciliation: false on a freshly-created
// bucket, true once data lands in the memtable, and still true after that data
// is flushed to a disk segment. Covered per strategy since the probe is
// strategy-agnostic and is applied to both RoaringSet (filterable) and
// RoaringSetRange (rangeable) buckets.
func TestBucketHasAnyData(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	cases := []struct {
		name        string
		strategyOpt BucketOption
		write       func(t *testing.T, b *Bucket)
	}{
		{
			name:        "roaringset",
			strategyOpt: WithStrategy(StrategyRoaringSet),
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetAddOne([]byte("key"), 7))
			},
		},
		{
			name:        "roaringsetrange",
			strategyOpt: WithStrategy(StrategyRoaringSetRange),
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRangeAdd(42, 7))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				tc.strategyOpt,
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
			b.SetMemtableThreshold(1e9) // never auto-flush; control segments explicitly

			require.False(t, b.HasAnyData(), "a freshly-created bucket holds no data")

			tc.write(t, b)
			require.True(t, b.HasAnyData(), "buffered memtable data must count as data")

			require.NoError(t, b.FlushAndSwitch())
			require.True(t, b.HasAnyData(), "flushed on-disk segment data must count as data")
		})
	}
}
