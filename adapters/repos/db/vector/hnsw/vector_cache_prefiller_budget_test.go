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

package hnsw

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// withPrefillWorkers pins the node-wide cap for one test. The budget is resolved once
// per process, so it has to be reset on both sides or the first test to touch it
// decides the value for every test after it.
func withPrefillWorkers(t *testing.T, workers string) {
	t.Helper()
	resetPrefillBudgetForTest()
	t.Setenv(prefillScanWorkersEnv, workers)
	t.Cleanup(resetPrefillBudgetForTest)
}

// TestPrefillScanWorkersAreNodeWide: the cap covers every index at once, not each one
// separately. Async loading starts a prefill per named vector per shard, so a per-index
// width multiplies by the number of indexes restoring — all of them queued on one
// volume, which saturates long before the CPU does.
func TestPrefillScanWorkersAreNodeWide(t *testing.T) {
	withPrefillWorkers(t, "3")

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
		wg       sync.WaitGroup
	)
	logger, _ := test.NewNullLogger()

	// each "index" asks for far more than the cap; together they must still not exceed it
	for i := 0; i < 8; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			got, release, err := acquirePrefillWorkers(context.Background(), 32, logger)
			require.NoError(t, err)
			defer release()

			cur := inFlight.Add(int64(got))
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond) // hold the permits so the overlap is real
			inFlight.Add(-int64(got))
		}, logger)
	}
	wg.Wait()

	require.LessOrEqual(t, peak.Load(), int64(3),
		"%d workers were in flight at once against a node-wide cap of 3", peak.Load())
	require.Positive(t, peak.Load(), "nothing ever acquired; the test proved nothing")
}

// TestPrefillScanWorkersClampToCap: a scan asking for more than the node allows gets
// the cap rather than blocking forever or being refused.
func TestPrefillScanWorkersClampToCap(t *testing.T) {
	withPrefillWorkers(t, "2")
	logger, _ := test.NewNullLogger()

	got, release, err := acquirePrefillWorkers(context.Background(), 64, logger)
	require.NoError(t, err)
	defer release()
	require.Equal(t, 2, got)
}

// TestPrefillScanDisabledFallsBackToSerial: 0 is the revert path. HNSW_PREFILL_TARGETED_READS
// picks a read strategy and cannot turn the scan off, so without this an operator hitting
// trouble with the scan has nothing to reach for.
func TestPrefillScanDisabledFallsBackToSerial(t *testing.T) {
	withPrefillWorkers(t, "0")

	const n = 50
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i), 1}, nil)
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)
	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)

	require.False(t, h.useParallelPrefill(),
		"with the scan disabled the index must route to the serial prefiller")
}

// TestPrefillScanEnabledByDefault guards the default: the bound exists to stop a node
// over-committing, not to turn the feature off when nobody configured it.
func TestPrefillScanEnabledByDefault(t *testing.T) {
	withPrefillWorkers(t, "")
	require.True(t, scanPrefillEnabled())
	_, cap := prefillScanBudget()
	require.Equal(t, defaultPrefillScanWorkers(), cap)
}
