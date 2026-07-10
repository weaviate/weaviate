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

package hfresh

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// TestAdaptiveConcurrency exercises the pure clamp helper:
// clamp(maxProcs/inflight, 1, configuredMax).
func TestAdaptiveConcurrency(t *testing.T) {
	tests := []struct {
		name          string
		maxProcs      int
		inflight      int
		configuredMax int
		want          int
	}{
		{name: "single in-flight query gets the full configured max", maxProcs: 32, inflight: 1, configuredMax: 16, want: 16},
		{name: "inflight equal to maxProcs collapses to 1", maxProcs: 32, inflight: 32, configuredMax: 16, want: 1},
		{name: "inflight greater than maxProcs floors at 1", maxProcs: 32, inflight: 64, configuredMax: 16, want: 1},
		{name: "configuredMax smaller than the quotient caps at configuredMax", maxProcs: 32, inflight: 2, configuredMax: 8, want: 8},
		{name: "quotient below configuredMax wins", maxProcs: 32, inflight: 8, configuredMax: 16, want: 4},
		{name: "single core box floors at 1", maxProcs: 1, inflight: 1, configuredMax: 16, want: 1},
		{name: "defensive floor when inflight is zero", maxProcs: 32, inflight: 0, configuredMax: 16, want: 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adaptiveConcurrency(tt.maxProcs, tt.inflight, tt.configuredMax)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestQueryConcurrencyDisabled verifies that when the adaptive governor is off,
// queryConcurrency returns the configured maximum unchanged regardless of the
// in-flight count.
func TestQueryConcurrencyDisabled(t *testing.T) {
	h := &HFresh{adaptiveConcurrencyEnabled: false}
	for _, inflight := range []int64{1, 4, 32, 1000} {
		assert.Equal(t, 16, h.queryConcurrency(16, inflight),
			"disabled governor must return configuredMax for inflight=%d", inflight)
		assert.Equal(t, 4, h.queryConcurrency(4, inflight))
	}
}

// TestQueryConcurrencyEnabled sanity-checks that the enabled path delegates to
// adaptiveConcurrency: a single in-flight query yields the configured max
// (assuming the test box has at least configuredMax procs), and a large
// in-flight count collapses to 1.
func TestQueryConcurrencyEnabled(t *testing.T) {
	h := &HFresh{adaptiveConcurrencyEnabled: true}
	// A very large in-flight count must collapse to 1 on any core count.
	assert.Equal(t, 1, h.queryConcurrency(16, 1_000_000))
}

// TestSearchInflightCounter verifies the in-flight counter is incremented while
// a fan-out search runs and returns to zero once all searches complete. The
// searches are held inside the posting fetch thunk via a channel handshake, so
// the assertions are deterministic without sleeps.
func TestSearchInflightCounter(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectorsSize := 300
	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectorsSize, 0, 32)

	// entered is signalled once per in-flight query when it reaches the fetch
	// thunk; release is closed to let all queries proceed past the thunk.
	entered := make(chan struct{})
	release := make(chan struct{})

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	for i := range vectorsSize {
		require.NoError(t, index.Add(t.Context(), uint64(i), vectors[i]))
	}
	// Drain background split/merge/reassign work BEFORE installing the blocking
	// fetch thunk, so those tasks can't deadlock on the handshake.
	for index.taskQueue.Size() > 0 {
		time.Sleep(50 * time.Millisecond)
	}

	// Route rescore fetches through the shared-view path so we can block inside
	// the fetch thunk with a channel handshake. The view itself is a noop.
	index.getViewThunk = func() common.BucketView { return &noopBucketView{} }
	index.vectorForIDWithView = func(ctx context.Context, id uint64, container *common.VectorSlice, v common.BucketView) ([]float32, error) {
		entered <- struct{}{}
		<-release
		if int(id) < len(vectors) {
			return vectors[id], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", id)
	}

	// Force each query to a single rescore fetch worker so exactly one "entered"
	// signal is produced per query before it parks, making the handshake exact.
	index.adaptiveConcurrencyEnabled = false
	index.rescoreConcurrency = 1

	const n = 5
	query := vectors[42]

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _, err := index.SearchByVector(t.Context(), query, 10, nil)
			assert.NoError(t, err)
		}()
	}

	// Wait until every query is parked inside the fetch thunk.
	for range n {
		<-entered
	}
	assert.EqualValues(t, n, index.searchInflight.Load(),
		"all %d searches must be counted while blocked in the fetch thunk", n)

	// Release them and drain any further "entered" signals the workers emit as
	// they fetch the remaining candidates.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	close(release)
	for {
		select {
		case <-entered:
			// keep draining follow-up fetches
		case <-done:
			assert.EqualValues(t, 0, index.searchInflight.Load(),
				"counter must return to zero after all searches complete")
			return
		}
	}
}
