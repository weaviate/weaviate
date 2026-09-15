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

// A scan prefill is bound by the volume it reads, not by the CPU that decodes, and a
// node runs one prefill per named vector per shard. Permits are therefore node-wide:
// width is still chosen per scan, but the total in flight across every index is capped
// here, because past the volume's ceiling an extra worker adds an open cursor and a
// pinned segment without adding throughput.
//
// HNSW_PREFILL_SCAN_WORKERS overrides the cap; 0 disables the scan and falls back to
// the serial by-id prefiller, which is the revert path.
const prefillScanWorkersEnv = "HNSW_PREFILL_SCAN_WORKERS"

var (
	prefillBudgetOnce sync.Once
	prefillBudget     *semaphore.Weighted
	prefillBudgetCap  int64
)

// prefillScanBudget returns the node-wide permit pool, or nil when scan prefill is
// disabled. Resolved once: the value is a deployment-wide property and re-reading it
// per shard would let a mid-restore change split the cap.
func prefillScanBudget(logger logrus.FieldLogger) (*semaphore.Weighted, int64) {
	prefillBudgetOnce.Do(func() {
		prefillBudgetCap = defaultPrefillScanWorkers()

		// A value that does not parse must not read as "use the default": this is the
		// revert path, so a mistyped off switch failing open is the case an operator
		// reaches for it in. Rejecting the pod instead would turn a typo into an outage
		// on a setting that was previously ignored, so it warns and carries on.
		if v := os.Getenv(prefillScanWorkersEnv); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			switch {
			case err != nil || n < 0:
				logger.WithFields(logrus.Fields{
					"action": "hnsw_vector_cache_prefill",
					"value":  v,
				}).Warnf("ignoring %s: expected a non-negative integer", prefillScanWorkersEnv)
			default:
				prefillBudgetCap = n
			}
		}

		if prefillBudgetCap > 0 {
			prefillBudget = semaphore.NewWeighted(prefillBudgetCap)
		}
		// once per process, so a restore that behaves unexpectedly can be traced to the
		// cap that was actually resolved rather than to the one that was configured
		logger.WithFields(logrus.Fields{
			"action":  "hnsw_vector_cache_prefill",
			"workers": prefillBudgetCap,
		}).Info("resolved the node-wide vector cache prefill scan worker cap")
	})
	return prefillBudget, prefillBudgetCap
}

// prefillScanParallelism is the width one scan asks for: 2x GOMAXPROCS, so that while
// one reader blocks on disk another keeps a core busy decoding, the IO-bound default
// used across the vector package.
func prefillScanParallelism() int {
	const cursorsPerProc = 2
	parallel := cursorsPerProc * runtime.GOMAXPROCS(0)
	if parallel < 1 {
		parallel = 1
	}
	return parallel
}

// defaultPrefillScanWorkers makes the node-wide total the width one scan would have
// taken on its own, so a single-index node behaves exactly as it did before the cap
// existed and only the many-index case is bounded. Derived from prefillScanParallelism
// rather than restating it, so the two cannot drift apart.
func defaultPrefillScanWorkers() int64 {
	return int64(prefillScanParallelism())
}

// scanPrefillEnabled reports whether the scan may run at all.
func scanPrefillEnabled(logger logrus.FieldLogger) bool {
	_, cap := prefillScanBudget(logger)
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
	sem, cap := prefillScanBudget(logger)
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
