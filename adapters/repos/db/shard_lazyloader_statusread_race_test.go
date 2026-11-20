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
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// stressDuration is d unless ISC_STRESS_DURATION overrides it.
func stressDuration(t *testing.T, d time.Duration) time.Duration {
	t.Helper()
	if v := os.Getenv("ISC_STRESS_DURATION"); v != "" {
		parsed, err := time.ParseDuration(v)
		require.NoError(t, err)
		return parsed
	}
	return d
}

type statusReadHits struct {
	mu      sync.Mutex
	samples []string
	hits    atomic.Int64
	busy    atomic.Int64
	reads   atomic.Int64
}

func (h *statusReadHits) record(op string, err error, status string) {
	h.reads.Add(1)
	if err != nil && errors.Is(err, errShardBusy) {
		h.busy.Add(1)
		return
	}
	if err == nil && (status == storagestate.StatusReady.String() || status == storagestate.StatusLazyLoading.String()) {
		return
	}
	h.hits.Add(1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) < 5 {
		h.samples = append(h.samples, fmt.Sprintf("%s: status=%q err=%v", op, status, err))
	}
}

func TestLazyLoadShard_StatusReadNeverErrsOnAnIdleUnload(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	f := newUnloadFixture(t)
	f.put(t)
	ctx := context.Background()
	hits := &statusReadHits{}
	reads := []func() (string, string, error){
		func() (string, string, error) {
			s, err := f.index.IncomingGetShardStatus(ctx, f.name)
			return "IncomingGetShardStatus", s, err
		},
		func() (string, string, error) {
			_, err := f.index.IncomingGetShardQueueSize(ctx, f.name)
			return "IncomingGetShardQueueSize", storagestate.StatusReady.String(), err
		},
		func() (string, string, error) {
			s, err := f.index.localShardStatus(ctx, f.name)
			return "localShardStatus", s, err
		},
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			read := reads[i%len(reads)]
			for {
				select {
				case <-stop:
					return
				default:
				}
				if f.lazy.unloadPhase.Load() != unloadPhaseTearing {
					time.Sleep(20 * time.Microsecond)
					continue
				}
				op, status, err := read()
				hits.record(op, err, status)
			}
		}, f.index.logger)
	}
	var cycles int
	deadline := time.Now().Add(stressDuration(t, 10*time.Second))
	for time.Now().Before(deadline) && hits.hits.Load() == 0 {
		if f.lazy.TryUnload(ctx, UnloadRequest{}) {
			cycles++
		}
		_, release, err := f.index.getOrInitShard(ctx, f.name)
		release()
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	t.Logf("unload cycles=%d status reads=%d busy=%d hits=%d", cycles, hits.reads.Load(), hits.busy.Load(), hits.hits.Load())
	require.Positive(t, cycles)
	require.Zero(t, hits.hits.Load(), "a status read failed on an idle unload: %v", hits.samples)
}
