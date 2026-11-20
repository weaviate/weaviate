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

package db

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestLazyLoadShard_WrapperStatusNeverReadsShutdownOnAnIdleUnload(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	f := newUnloadFixture(t)
	f.put(t)
	hits := &statusReadHits{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				hits.record("GetStatus", nil, f.lazy.GetStatus().String())
			}
		}, f.index.logger)
	}
	var cycles int
	deadline := time.Now().Add(stressDuration(t, 10*time.Second))
	for time.Now().Before(deadline) && hits.hits.Load() == 0 {
		if f.lazy.TryUnload(context.Background(), UnloadRequest{}) {
			cycles++
		}
		_, release, err := f.index.getOrInitShard(context.Background(), f.name)
		release()
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	t.Logf("unload cycles=%d status reads=%d hits=%d", cycles, hits.reads.Load(), hits.hits.Load())
	require.Positive(t, cycles)
	require.Zero(t, hits.hits.Load(), "a wrapper status read saw the idle unload's teardown: %v", hits.samples)
}
