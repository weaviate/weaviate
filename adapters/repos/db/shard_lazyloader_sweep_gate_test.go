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

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// errTestLoadRefused stops a probing load once it has done its work under the
// loading mutex, so the shard stays cold for the next round.
var errTestLoadRefused = fmt.Errorf("test: refusing to load")

// loadProbeAllocChecker runs the probe at the point [LazyLoadShard.Load] reaches
// its first step under the loading mutex, which is the earliest a real load
// could act on the shard.
type loadProbeAllocChecker struct {
	memwatch.AllocChecker
	probe func()
}

func (c loadProbeAllocChecker) CheckMappingAndReserve(int64, int) error {
	c.probe()
	return errTestLoadRefused
}

// canSkipUnloadedSweep answers from two reads — the shard's loaded flag and the
// shard's disk — and they only mean what the gate takes them to mean if nothing
// takes the loading mutex between them. A gate that dropped the mutex after the
// flag check would still leave it free on return, which is all the sweep's other
// tests observe, so the property needs a pin of its own: state planted the
// instant the mutex becomes available must never be state the gate's disk read
// can report.
//
// Each round lands in one of two orders, and the prober tells them apart by
// holding the mutex until the gate call has returned:
//
//   - The gate returns while the prober holds the mutex. The two never hold it
//     at once, so the gate had already released it, and the plant is strictly
//     after every read the gate made. A gate that reports the plant here read
//     the shard after answering for it — the round decides the property.
//   - The gate does not return. Then it is still queued for the mutex the prober
//     holds (Go's mutex lets a newcomer barge past a waiter), the plant preceded
//     the gate's own acquisition, and reporting it is correct. The round is
//     discarded, not failed.
//
// No timing assumption sits between those two, so a slow machine costs rounds,
// never a false failure.
func TestLazyLoadShardCanSkipUnloadedSweepIsOneStep(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
		gateShard = "gate-tenant"
		rounds    = 50
		// fillerDirs widens the gate's first directory read well past the time a
		// plant needs to land, so a gate that let one in gets caught by it.
		fillerDirs = 2000
		// holdFor bounds the wait for the gate to return before the round is
		// given up as one the prober barged.
		holdFor   = 250 * time.Millisecond
		pollEvery = 50 * time.Microsecond
	)

	ctx := testCtx()
	class := newTestClassWithProps("SweepGateOneStep_"+uuid.NewString()[:8], []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	lsm := shardPathLSM(idx.path(), gateShard)
	require.NoError(t, os.MkdirAll(filepath.Join(lsm, ".migrations"), 0o755))
	for i := range fillerDirs {
		require.NoError(t, os.Mkdir(filepath.Join(lsm, fmt.Sprintf("bucket-%05d", i)), 0o755))
	}
	trackerDir := filepath.Join(lsm, ".migrations", tracker)

	var gateReturned atomic.Bool
	// Written by the prober under the loading mutex, read once it has been joined.
	var (
		plantErr     error
		decidesRound bool
	)
	// probe runs with the loading mutex held, however the prober took it.
	probe := func() {
		if err := os.Mkdir(trackerDir, 0o755); err != nil {
			plantErr = err
			return
		}
		if err := os.WriteFile(filepath.Join(trackerDir, "started.mig"), []byte("x"), 0o644); err != nil {
			plantErr = err
			return
		}
		for deadline := time.Now().Add(holdFor); !gateReturned.Load() && time.Now().Before(deadline); {
			time.Sleep(pollEvery)
		}
		decidesRound = gateReturned.Load()
	}
	newGateShard := func(allocChecker memwatch.AllocChecker) *LazyLoadShard {
		return NewLazyLoadShard(ctx, nil, gateShard, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
	}

	tests := []struct {
		name     string
		newShard func() *LazyLoadShard
		// takeMutex reaches the probe through whatever takes the loading mutex.
		takeMutex func(lazy *LazyLoadShard)
	}{
		{
			name:     "a writer holding the loading mutex",
			newShard: func() *LazyLoadShard { return newGateShard(idx.allocChecker) },
			takeMutex: func(lazy *LazyLoadShard) {
				lazy.mutex.Lock()
				defer lazy.mutex.Unlock()
				probe()
			},
		},
		{
			name: "a concurrent load",
			newShard: func() *LazyLoadShard {
				return newGateShard(loadProbeAllocChecker{memwatch.NewDummyMonitor(), probe})
			},
			takeMutex: func(lazy *LazyLoadShard) { _ = lazy.Load(ctx) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy := tc.newShard()
			decided := 0

			for range rounds {
				// Each round starts from a shard with nothing to sweep, whatever
				// the round before it planted.
				require.NoError(t, os.RemoveAll(trackerDir))
				plantErr, decidesRound = nil, false
				gateReturned.Store(false)

				spinning, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				go func() {
					defer close(done)
					close(spinning)
					for {
						if lazy.mutex.TryLock() {
							lazy.mutex.Unlock()
							select {
							case <-stop:
								return
							default:
								continue
							}
						}
						// The gate has at least reached its own acquisition, so
						// this is the earliest a plant is worth making.
						tc.takeMutex(lazy)
						return
					}
				}()

				<-spinning
				skip, _ := lazy.canSkipUnloadedSweep(propName, indexType, nil)
				gateReturned.Store(true)
				close(stop)
				<-done

				require.NoError(t, plantErr)
				if decidesRound {
					decided++
					require.DirExists(t, trackerDir)
					require.True(t, skip,
						"the gate reported state that only reached the shard after it had answered")
				}
			}

			require.Positive(t, decided,
				"every round was barged, so none of them decided anything")

			// The gate reports the very state the prober plants, so the rounds
			// above are a claim about when it landed, not about what it is.
			mkTrackerDir(t, lsm, tracker, "started.mig")
			skip, _ := lazy.canSkipUnloadedSweep(propName, indexType, nil)
			require.False(t, skip)
			require.NoError(t, os.RemoveAll(trackerDir))
		})
	}
}
