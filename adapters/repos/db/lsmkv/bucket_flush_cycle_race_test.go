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
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// The flush cycle and a FlushAndSwitch called from elsewhere, as by a shard
// halted for a backup, run on the same bucket at the same time. Run with -race.
func TestBucket_FlushCycleConcurrentWithFlushAndSwitch(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithMemtableThreshold(1), WithMinWalThreshold(0))
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	for i := range 200 {
		require.NoError(t, b.Put([]byte{byte(i)}, []byte{byte(i)}))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.flushAndSwitchIfThresholdsMet(func() bool { return false })
		}()
		go func() {
			defer wg.Done()
			require.NoError(t, b.FlushAndSwitch())
		}()
		wg.Wait()
	}
}
