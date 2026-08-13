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
	"errors"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

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

// errScanPrefillDisabled reports that the operator turned the scan off. Callers route
// on useParallelPrefill, which checks the same switch, so reaching this is a routing
// bug rather than a configuration outcome. It is an error and not a zero width because
// zero reads as a width at both call sites: QuantileKeys(-1) yields no seeds, which
// would quietly scan the whole bucket on one cursor instead of not scanning.
var errScanPrefillDisabled = errors.New("vector cache scan prefill is disabled")

// acquirePrefillWorkers takes permits from the node-wide pool and returns how many it
// got, along with the release. It degrades rather than queues: a scan that cannot have
// its full width takes whatever is free, down to a single worker. Waiting for the full
// width instead would serialize scans node-wide, since the default width is the whole
// pool, and leave the last shard of a restore queued behind every earlier one.
func acquirePrefillWorkers(ctx context.Context, want int, logger logrus.FieldLogger) (int, func(), error) {
	sem, cap := prefillScanBudget()
	if sem == nil {
		return 0, func() {}, errScanPrefillDisabled
	}
	if int64(want) > cap {
		want = int(cap)
	}
	if want < 1 {
		want = 1
	}
	for n := want; n > 0; n /= 2 {
		if sem.TryAcquire(int64(n)) {
			return n, releasePrefillWorkers(sem, n), nil
		}
	}

	// Every permit is committed. Queueing for one beats falling back to the serial
	// by-id prefiller, which is the disk-seek path this exists to avoid.
	before := time.Now()
	if err := sem.Acquire(ctx, 1); err != nil {
		return 0, func() {}, err
	}
	logger.WithFields(logrus.Fields{
		"action": "hnsw_vector_cache_prefill",
		"waited": time.Since(before),
	}).Info("vector cache prefill scan queued for a worker; the node-wide pool was fully committed")
	return 1, releasePrefillWorkers(sem, 1), nil
}

func releasePrefillWorkers(sem *semaphore.Weighted, n int) func() {
	var once sync.Once
	return func() { once.Do(func() { sem.Release(int64(n)) }) }
}
