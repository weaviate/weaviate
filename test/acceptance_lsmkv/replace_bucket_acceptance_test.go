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
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

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

	// THROWAWAY stall tracing (LSMKV_STALL_TRACE=1)
	traceDone := make(chan struct{})
	traceWg := sync.WaitGroup{}
	if lsmkv.StallTraceEnabled() {
		lsmkv.StallTraceRaw(time.Now(), "test_start", "workers=%d gomaxprocs=%d dir=%s", workers, runtime.GOMAXPROCS(0), dir)
		traceWg.Add(3)
		go func() { defer traceWg.Done(); schedStallDetector(traceDone) }()
		go func() { defer traceWg.Done(); gcPauseWatcher(traceDone) }()
		go func() { defer traceWg.Done(); psiWatcher(traceDone) }()
	}
	defer func() {
		close(traceDone)
		traceWg.Wait()
		lsmkv.StallTraceRaw(time.Now(), "test_end", "failed=%v", t.Failed())
		lsmkv.StallTraceSummary()
	}()

	results := make([]result, workers)
	wg := sync.WaitGroup{}
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go worker(ctx, t, &wg, workerID, bucket, logger, putThreshold, getThreshold, trackWorstQueries, results)
	}

	wg.Wait()
	logger.WithField("concurrency", workers).Infof("%d workers completed", workers)

	var putOutsideThreshold []float32
	var getOutsideThreshold []float32

	totalIngested := 0
	totalSpotChecks := 0

	for _, r := range results {
		totalIngested += r.ingested
		totalSpotChecks += r.getSpotChecks

		for r.worstPutQueries.Len() > 0 {
			tookMs := r.worstPutQueries.Pop().Dist * 1000
			if tookMs > float32(putThreshold.Milliseconds()) {
				putOutsideThreshold = append(putOutsideThreshold, tookMs)
			}
		}

		for r.worstGetQueries.Len() > 0 {
			tookMs := r.worstGetQueries.Pop().Dist * 1000
			if tookMs > float32(getThreshold.Milliseconds()) {
				getOutsideThreshold = append(getOutsideThreshold, tookMs)
			}
		}
	}

	if len(putOutsideThreshold) > 0 {
		t.Errorf("%d put queries outside threshold (%s): %v", len(putOutsideThreshold), putThreshold, putOutsideThreshold)
	} else {
		logger.Infof("all put queries were within threshold (%s)", putThreshold)
	}

	if len(getOutsideThreshold) > 0 {
		t.Errorf("%d get queries outside threshold (%s) : %v", len(getOutsideThreshold), getThreshold, getOutsideThreshold)
	} else {
		logger.Infof("all get queries were within threshold (%s)", getThreshold)
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
	for {
		if ctx.Err() != nil {
			break
		}
		before := time.Now()
		bucket.Put([]byte(fmt.Sprintf("worker-%d-key-%d", workerID, i)), []byte(fmt.Sprintf("value-%d", i)))
		took := time.Since(before)
		trackWorstQuery(worstPutQueries, i, took, trackWorstQueries)
		if took > putThreshold {
			logger.Warnf("put took too long: %s", time.Since(before))
		}
		traceQuery(before, "test.put", workerID, took, putThreshold)

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
					logger.Warnf("get took too long: %s", time.Since(before))
				}
				traceQuery(before, "test.get", workerID, took, getThreshold)

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
		workerID:        workerID,
		worstPutQueries: worstPutQueries,
		worstGetQueries: worstGetQueries,
		ingested:        i,
		getSpotChecks:   totalAsserted,
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

// ---- THROWAWAY stall tracing helpers ----

var tracedQueries atomic.Int64

// traceQuery logs queries above 20ms; OVER marks the ones that fail the test.
func traceQuery(start time.Time, kind string, workerID int, took, threshold time.Duration) {
	if !lsmkv.StallTraceEnabled() {
		return
	}
	lsmkv.StallTraceObserve(kind, took)
	if took <= 20*time.Millisecond {
		return
	}
	over := ""
	if took > threshold {
		over = " OVER_THRESHOLD"
	}
	if tracedQueries.Add(1) > 1000 && over == "" {
		return
	}
	lsmkv.StallTraceRaw(start, kind, "worker=%d took=%.2fms end=%s%s", workerID,
		float64(took)/float64(time.Millisecond), start.Add(took).UTC().Format("15:04:05.000000"), over)
}

// schedStallDetector logs when a 5ms sleep overshoots by more than 20ms
// (CPU starvation, STW pauses, host steal).
func schedStallDetector(done chan struct{}) {
	logged := 0
	for {
		select {
		case <-done:
			return
		default:
		}
		before := time.Now()
		time.Sleep(5 * time.Millisecond)
		late := time.Since(before) - 5*time.Millisecond
		lsmkv.StallTraceObserve("sched.oversleep", late)
		if late > 20*time.Millisecond && logged < 500 {
			logged++
			lsmkv.StallTraceRaw(before, "sched_stall", "late=%.2fms", float64(late)/float64(time.Millisecond))
		}
	}
}

// gcPauseWatcher logs GC stop-the-world pauses above 1ms.
func gcPauseWatcher(done chan struct{}) {
	var lastNum int64
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
		}
		var st debug.GCStats
		st.Pause = make([]time.Duration, 0, 256)
		st.PauseEnd = make([]time.Time, 0, 256)
		debug.ReadGCStats(&st)
		newN := int(st.NumGC - lastNum)
		if newN > len(st.Pause) {
			newN = len(st.Pause)
		}
		for i := 0; i < newN; i++ { // most recent first
			lsmkv.StallTraceObserve("gc.pause", st.Pause[i])
			if st.Pause[i] > time.Millisecond {
				lsmkv.StallTraceRaw(st.PauseEnd[i].Add(-st.Pause[i]), "gc_pause", "pause=%.2fms numgc=%d",
					float64(st.Pause[i])/float64(time.Millisecond), st.NumGC)
			}
		}
		lastNum = st.NumGC
	}
}

// psiWatcher samples Linux pressure stall info every 250ms and logs
// intervals with more than 20ms of stalled time.
func psiWatcher(done chan struct{}) {
	read := func(res string) (some, full int64, ok bool) {
		b, err := os.ReadFile("/proc/pressure/" + res)
		if err != nil {
			return 0, 0, false
		}
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			v, _ := strconv.ParseInt(strings.TrimPrefix(fields[4], "total="), 10, 64)
			switch fields[0] {
			case "some":
				some = v
			case "full":
				full = v
			}
		}
		return some, full, true
	}
	type prev struct{ some, full int64 }
	last := map[string]prev{}
	for _, r := range []string{"io", "cpu", "memory"} {
		s, f, ok := read(r)
		if !ok {
			lsmkv.StallTraceRaw(time.Now(), "psi_unavailable", "res=%s", r)
			continue
		}
		last[r] = prev{s, f}
	}
	logged := 0
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-tick.C:
			for r, p := range last {
				s, f, ok := read(r)
				if !ok {
					continue
				}
				ds, df := s-p.some, f-p.full
				last[r] = prev{s, f}
				if (ds > 20_000 || df > 20_000) && logged < 1000 {
					logged++
					lsmkv.StallTraceRaw(now.Add(-250*time.Millisecond), "psi_"+r, "some=%.1fms full=%.1fms (per 250ms window)",
						float64(ds)/1000, float64(df)/1000)
				}
			}
		}
	}
}
