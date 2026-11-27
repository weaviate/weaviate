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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestLSMKV_ReplaceBucket(t *testing.T) {
	// Latency is asserted as a tail budget rather than on the single worst
	// query: shared CI runners occasionally stall a few individual queries past
	// the threshold through no fault of the bucket (weaviate/weaviate#12898).
	// The test fails if more than maxOverThresholdRatio of puts or gets exceed
	// their threshold (i.e. p99.99 must stay below it), or if any single query
	// exceeds hardCeiling, which still catches genuine stalls.
	putThreshold := 100 * time.Millisecond
	getThreshold := 100 * time.Millisecond
	hardCeiling := 1 * time.Second
	maxOverThresholdRatio := 0.0001
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

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", logger, 1)
	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", logger, 1)
	flushCycle := cyclemanager.NewManager("flush", cyclemanager.MemtableFlushCycleTicker(false), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()
	compactionCycle := cyclemanager.NewManager("compaction", cyclemanager.CompactionCycleTicker(false), compactionCallbacks.CycleCallback, logger)
	compactionCycle.Start()

	bucket, err := c.NewBucket(ctx, filepath.Join(dir, "my-bucket"), "", logger, nil,
		compactionCallbacks, flushCallbacks,
		lsmkv.WithPread(true),
		lsmkv.WithDynamicMemtableSizing(1*1024*1024, 2*1024*1024, 1, 4),
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

	var worstPutsMs []float32
	var worstGetsMs []float32

	totalIngested := 0
	totalSpotChecks := 0
	totalPutsOverThreshold := 0
	totalGetsOverThreshold := 0

	for _, r := range results {
		totalIngested += r.ingested
		totalSpotChecks += r.getSpotChecks
		totalPutsOverThreshold += r.putsOverThreshold
		totalGetsOverThreshold += r.getsOverThreshold

		for r.worstPutQueries.Len() > 0 {
			worstPutsMs = append(worstPutsMs, r.worstPutQueries.Pop().Dist*1000)
		}

		for r.worstGetQueries.Len() > 0 {
			worstGetsMs = append(worstGetsMs, r.worstGetQueries.Pop().Dist*1000)
		}
	}

	assertLatencyBudget(t, logger, "put", totalPutsOverThreshold, totalIngested, worstPutsMs,
		putThreshold, hardCeiling, maxOverThresholdRatio)
	assertLatencyBudget(t, logger, "get", totalGetsOverThreshold, totalSpotChecks, worstGetsMs,
		getThreshold, hardCeiling, maxOverThresholdRatio)

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

// assertLatencyBudget fails the test if the share of queries slower than
// threshold exceeds maxRatio, or if any tracked query is slower than ceiling.
// worstMs holds the slowest tracked queries in milliseconds, for reporting.
func assertLatencyBudget(t *testing.T, logger logrus.FieldLogger, op string,
	overThreshold, total int, worstMs []float32,
	threshold, ceiling time.Duration, maxRatio float64,
) {
	t.Helper()

	sort.Slice(worstMs, func(i, j int) bool { return worstMs[i] > worstMs[j] })

	var aboveCeiling []float32
	for _, ms := range worstMs {
		if ms > float32(ceiling.Milliseconds()) {
			aboveCeiling = append(aboveCeiling, ms)
		}
	}
	if len(aboveCeiling) > 0 {
		t.Errorf("%d %s queries exceeded the hard ceiling (%s): %v", len(aboveCeiling), op, ceiling, aboveCeiling)
	}

	if total == 0 {
		return // the throughput sanity checks report this case
	}
	ratio := float64(overThreshold) / float64(total)
	if ratio > maxRatio {
		t.Errorf("%d of %d %s queries (%.4f%%) exceeded threshold (%s), budget is %.4f%%; worst (ms): %v",
			overThreshold, total, op, ratio*100, threshold, maxRatio*100, worstMs)
	} else {
		logger.Infof("%d of %d %s queries (%.4f%%) exceeded threshold (%s), within budget of %.4f%%; worst (ms): %v",
			overThreshold, total, op, ratio*100, threshold, maxRatio*100, worstMs)
	}
}

type result struct {
	workerID          int
	worstPutQueries   *priorityqueue.Queue[float32]
	worstGetQueries   *priorityqueue.Queue[float32]
	ingested          int
	getSpotChecks     int
	putsOverThreshold int
	getsOverThreshold int
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
	putsOverThreshold := 0
	getsOverThreshold := 0
	for {
		if ctx.Err() != nil {
			break
		}
		before := time.Now()
		bucket.Put([]byte(fmt.Sprintf("worker-%d-key-%d", workerID, i)), []byte(fmt.Sprintf("value-%d", i)))
		took := time.Since(before)
		trackWorstQuery(worstPutQueries, i, took, trackWorstQueries)
		if took > putThreshold {
			putsOverThreshold++
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
					getsOverThreshold++
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

	results[workerID] = result{
		workerID:          workerID,
		worstPutQueries:   worstPutQueries,
		worstGetQueries:   worstGetQueries,
		ingested:          i,
		getSpotChecks:     totalAsserted,
		putsOverThreshold: putsOverThreshold,
		getsOverThreshold: getsOverThreshold,
	}

	logger.WithField("imported", i).WithField("get_spot_checks", totalAsserted).Infof("completed worker")
}

func trackWorstQuery(heap *priorityqueue.Queue[float32], i int, took time.Duration, trackWorstQueries int) {
	if heap.Len() < trackWorstQueries {
		heap.Insert(uint64(i), float32(took.Seconds()))
	} else if heap.Top().Dist < float32(took.Seconds()) {
		heap.Pop()
		heap.Insert(uint64(i), float32(took.Seconds()))
	}
}
