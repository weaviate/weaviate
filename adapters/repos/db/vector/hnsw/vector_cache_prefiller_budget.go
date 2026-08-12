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
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// A scan prefill is bound by the volume it reads, not by the CPU that decodes. On a
// gp3 volume the merged cursor reaches its sequential ceiling with four workers, and
// every worker past that adds memory, an open cursor and a pinned segment without
// adding throughput. Sizing per index therefore over-provisions badly: async loading
// runs one prefill per named vector per shard, so a node restoring many tenants
// multiplies 2xGOMAXPROCS by the number of indexes and they all queue on one device.
//
// The permits are node-wide for that reason. Width is still chosen per scan, but the
// total in flight across every index is capped here.
//
// HNSW_PREFILL_SCAN_WORKERS overrides the cap. 0 disables scan prefill entirely and
// falls back to the serial by-id prefiller, which is the revert path: the targeted
// reads flag selects a read strategy, not whether the scan runs.
const prefillScanWorkersEnv = "HNSW_PREFILL_SCAN_WORKERS"

var (
	prefillBudgetOnce sync.Once
	prefillBudget     *semaphore.Weighted
	prefillBudgetCap  int64
)

// prefillScanBudget returns the node-wide permit pool, or nil when scan prefill is
// disabled. Resolved once: the value is a deployment-wide property and re-reading it
// per shard would let a mid-restore change split the cap.
func prefillScanBudget() (*semaphore.Weighted, int64) {
	prefillBudgetOnce.Do(func() {
		prefillBudgetCap = defaultPrefillScanWorkers()
		if v := os.Getenv(prefillScanWorkersEnv); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				prefillBudgetCap = n
			}
		}
		if prefillBudgetCap > 0 {
			prefillBudget = semaphore.NewWeighted(prefillBudgetCap)
		}
	})
	return prefillBudget, prefillBudgetCap
}

// defaultPrefillScanWorkers keeps the previous per-index width as the node-wide total:
// enough to saturate a network volume, and the same number a single-index node used
// to get, so nothing gets slower for the common case of one index.
func defaultPrefillScanWorkers() int64 {
	n := int64(2 * runtime.GOMAXPROCS(0))
	if n < 1 {
		n = 1
	}
	return n
}

// scanPrefillEnabled reports whether the scan may run at all.
func scanPrefillEnabled() bool {
	_, cap := prefillScanBudget()
	return cap > 0
}

// acquirePrefillWorkers takes up to want permits and returns how many it got, along
// with the release. It always returns at least one — a scan that cannot get a permit
// waits for one rather than falling back, since the serial path it would fall back to
// is the slow disk-seek prefiller this exists to avoid.
func acquirePrefillWorkers(ctx context.Context, want int, logger logrus.FieldLogger) (int, func(), error) {
	sem, cap := prefillScanBudget()
	if sem == nil {
		return 0, func() {}, nil
	}
	if int64(want) > cap {
		want = int(cap)
	}
	if want < 1 {
		want = 1
	}
	if err := sem.Acquire(ctx, int64(want)); err != nil {
		return 0, func() {}, err
	}
	var once sync.Once
	return want, func() { once.Do(func() { sem.Release(int64(want)) }) }, nil
}

// resetPrefillBudgetForTest re-resolves the cap. Only tests need this; production
// reads the environment once for the reason given on prefillScanBudget.
func resetPrefillBudgetForTest() {
	prefillBudgetOnce = sync.Once{}
	prefillBudget = nil
	prefillBudgetCap = 0
}
