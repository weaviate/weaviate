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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// maxSlowOpsPerWorker bounds how many puts, and separately how many gets, may
// exceed the latency threshold in one run. It is a per-worker budget because a
// put holds the memtable's exclusive lock while it writes the commit log, so
// every stall delays one operation in each of the other workers
// (weaviate/0-weaviate-issues#525), and a run flushes tens of times.
//
// At 4 workers that puts the limit at 100. Healthy runs reach 30 slow ops per
// category on a CI runner and 26 on a loaded developer machine, and a 200ms
// stall on every 50,000th put produces 193 to 236 slow puts. So the usable
// window is [30, 193]: 100 sits 3.3x above the noise and 1.9x below the
// regression the gate has to catch.
//
// A count, not a share of all operations. A run performs millions of them and a
// stall only delays the few in flight, so any share stays vanishingly small no
// matter how often it happens.
const maxSlowOpsPerWorker = 25

// maxSingleOpStall fails a run on one catastrophically slow operation. A hang
// delays too few operations to reach maxSlowOps, so the count gate alone would
// pass it. The limit sits two orders of magnitude above the latency threshold
// because the worst healthy operation observed so far is 914ms, and scheduling
// noise on a shared runner must not be able to reach it.
const maxSingleOpStall = 10 * time.Second

func TestLSMKV_ReplaceBucket(t *testing.T) {
	putThreshold := 100 * time.Millisecond
	getThreshold := 100 * time.Millisecond
	trackWorstQueries := 10
	workers := 4

	dir := t.TempDir()
	ctx := context.Background()
	c := lsmkv.NewBucketCreator()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	if n := runtime.GOMAXPROCS(0); n < workers {
		workers = n
		logger.Infof("reducing workers to %d", workers)
	}
	maxSlowOps := maxSlowOpsPerWorker * workers

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", logger, 1)
	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", logger, 1)
	flushCycle := cyclemanager.NewManager("flush", cyclemanager.MemtableFlushCycleTicker(false), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()
	compactionCycle := cyclemanager.NewManager("compaction", cyclemanager.CompactionCycleTicker(false), compactionCallbacks.CycleCallback, logger)
	compactionCycle.Start()

	bucket, err := c.NewBucket(ctx, filepath.Join(dir, "my-bucket"), "", logger, nil,
		compactionCallbacks, flushCallbacks,
		lsmkv.WithPread(true),
		lsmkv.WithDynamicMemtableSizing(1, 2, 1, 4),
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
	)
	if err != nil {
		panic(err)
	}

	defer bucket.Shutdown(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results := make([]result, workers)
	wg := sync.WaitGroup{}
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go worker(ctx, t, &wg, workerID, bucket, logger, putThreshold, getThreshold, trackWorstQueries, results)
	}

	wg.Wait()
	logger.WithField("concurrency", workers).Infof("%d workers completed", workers)

	totalIngested := 0
	totalSpotChecks := 0
	totalSlowPuts := 0
	totalSlowGets := 0
	var worstPutMs, worstGetMs float32

	for _, r := range results {
		totalIngested += r.ingested
		totalSpotChecks += r.getSpotChecks
		totalSlowPuts += r.slowPuts
		totalSlowGets += r.slowGets

		for r.worstPutQueries.Len() > 0 {
			if tookMs := r.worstPutQueries.Pop().Dist * 1000; tookMs > worstPutMs {
				worstPutMs = tookMs
			}
		}

		for r.worstGetQueries.Len() > 0 {
			if tookMs := r.worstGetQueries.Pop().Dist * 1000; tookMs > worstGetMs {
				worstGetMs = tookMs
			}
		}
	}

	// The two lines below are what a future maintainer recalibrates maxSlowOps
	// from, so they are logged on every run and not only when the gate trips.
	// test/run.sh passes -v for this package because `go test` throws the test
	// binary's output away otherwise.
	logger.Infof("puts: %d of %d outside threshold (%s), limit %d, worst %.2fms",
		totalSlowPuts, totalIngested, putThreshold, maxSlowOps, worstPutMs)
	logger.Infof("gets: %d of %d outside threshold (%s), limit %d, worst %.2fms",
		totalSlowGets, totalSpotChecks, getThreshold, maxSlowOps, worstGetMs)

	if totalSlowPuts > maxSlowOps {
		t.Errorf("%d puts were outside threshold (%s), limit is %d; %d puts total, worst %.2fms",
			totalSlowPuts, putThreshold, maxSlowOps, totalIngested, worstPutMs)
	}

	if totalSlowGets > maxSlowOps {
		t.Errorf("%d gets were outside threshold (%s), limit is %d; %d gets total, worst %.2fms",
			totalSlowGets, getThreshold, maxSlowOps, totalSpotChecks, worstGetMs)
	}

	if maxStallMs := float32(maxSingleOpStall.Milliseconds()); worstPutMs > maxStallMs || worstGetMs > maxStallMs {
		t.Errorf("worst put took %.2fms and worst get took %.2fms, a single operation may not exceed %s",
			worstPutMs, worstGetMs, maxSingleOpStall)
	}

	// This a sanity check to make sure the test actually ran. The expected total
	// is a lot more, but if the test were to just block for 60s and do nothing,
	// this sanity check should catch it.
	if totalIngested < 500_000 {
		t.Errorf("expected at least 500k entries but got %d", totalIngested)
	} else {
		logger.Infof("ingested %d entries", totalIngested)
	}
	if totalSpotChecks < 250_000 {
		t.Errorf("expected at least 250k spot checks but got %d", totalSpotChecks)
	} else {
		logger.Infof("performed %d spot checks", totalSpotChecks)
	}
}

type result struct {
	workerID        int
	worstPutQueries *priorityqueue.Queue[float32]
	worstGetQueries *priorityqueue.Queue[float32]
	ingested        int
	getSpotChecks   int
	slowPuts        int
	slowGets        int
}

func worker(ctx context.Context, t *testing.T, wg *sync.WaitGroup, workerID int, bucket *lsmkv.Bucket, logger logrus.FieldLogger,
	putThreshold time.Duration, getThreshold time.Duration, trackWorstQueries int, results []result,
) {
	defer wg.Done()

	logger = logger.WithField("worker_id", workerID)
	worstPutQueries := priorityqueue.NewMin[float32](trackWorstQueries)
	worstGetQueries := priorityqueue.NewMin[float32](trackWorstQueries)

	i := 0
	totalAsserted := 0
	slowPuts := 0
	slowGets := 0

	// deferred so that a worker returning early still reports what it measured.
	// The caller sums every entry of results and calls Len() on the two queues,
	// which panics on the zero value. Registered after wg.Done() so it runs
	// before it.
	defer func() {
		results[workerID] = result{
			workerID:        workerID,
			worstPutQueries: worstPutQueries,
			worstGetQueries: worstGetQueries,
			ingested:        i,
			getSpotChecks:   totalAsserted,
			slowPuts:        slowPuts,
			slowGets:        slowGets,
		}

		logger.WithField("imported", i).WithField("get_spot_checks", totalAsserted).Infof("completed worker")
	}()

	for {
		if ctx.Err() != nil {
			break
		}
		before := time.Now()
		if err := bucket.Put([]byte(fmt.Sprintf("worker-%d-key-%d", workerID, i)),
			[]byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Errorf("failed to put key-%d: %s", i, err)
			return
		}
		took := time.Since(before)
		trackWorstQuery(worstPutQueries, i, took, trackWorstQueries)
		if took > putThreshold {
			slowPuts++
			logger.Warnf("put took too long: %s", took)
		}

		// perform spot checks every 10000 iterations
		// the spot checks pick a random increment between 0 and 100 between ids
		if i > 100 && i%10000 == 0 {
			j := 0
			for j < i {
				before := time.Now()
				val, err := bucket.Get([]byte(fmt.Sprintf("worker-%d-key-%d", workerID, j)))
				if err != nil {
					t.Errorf("failed to get key-%d: %s", j, err)
					return
				}
				took := time.Since(before)
				if took > getThreshold {
					slowGets++
					logger.Warnf("get took too long: %s", took)
				}

				if string(val) != fmt.Sprintf("value-%d", j) {
					t.Errorf("expected value-%d but got %s", j, val)
				}

				trackWorstQuery(worstGetQueries, j, took, trackWorstQueries)

				totalAsserted++
				j += rand.Intn(100)
			}
		}

		if i%100_000 == 0 {
			logger.WithField("current_id", i).Infof("worker %d inserted %d entries", workerID, i)
		}

		i++
	}
}

func trackWorstQuery(heap *priorityqueue.Queue[float32], i int, took time.Duration, trackWorstQueries int) {
	if heap.Len() < trackWorstQueries {
		heap.Insert(uint64(i), float32(took.Seconds()))
	} else if heap.Top().Dist < float32(took.Seconds()) {
		heap.Pop()
		heap.Insert(uint64(i), float32(took.Seconds()))
	}
}
