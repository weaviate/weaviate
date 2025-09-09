//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

func TestLSMKV_ReplaceBucket(t *testing.T) {
	putThreshold := 100 * time.Millisecond
	getThreshold := 50 * time.Millisecond

	writeDuration := time.Minute
	readDuration := time.Minute

	// avoid perfect synchronization with the write/read mode switch, otherwise
	// the cursor will only ever co-occur with one of the modes
	cursorStartInterval := 40 * time.Second
	cursorMaxDuration := 20 * time.Second

	trackWorstQueries := 10
	workers := 3

	dir := "./data"
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
	flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()
	compactionCycle := cyclemanager.NewManager(cyclemanager.CompactionCycleTicker(), compactionCallbacks.CycleCallback, logger)
	compactionCycle.Start()

	logger.Info("loading bucket")
	bucket, err := c.NewBucket(ctx, filepath.Join(dir, "my-bucket"), "", logger, nil,
		compactionCallbacks, flushCallbacks,
		lsmkv.WithPread(true),
	)
	if err != nil {
		panic(err)
	}

	logger.Info("bucket is ready")

	defer bucket.Shutdown(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	results := make([]result, workers)
	wg := sync.WaitGroup{}

	mode := newMode(logger, writeDuration, readDuration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go worker(ctx, t, mode, &wg, workerID, bucket, logger, putThreshold, getThreshold, trackWorstQueries, results)
	}

	wg.Add(1)
	// every minute start one cursor that will run for at most 30s
	go cursorWorker(ctx, t, &wg, 0, bucket, logger, cursorStartInterval, cursorMaxDuration)

	modeCtx, cancelMode := context.WithCancel(context.Background())
	go mode.alternate(modeCtx)

	wg.Wait()
	cancelMode()

	logger.WithField("concurrency", workers).Infof("%d workers completed", workers)

	var putOutsideThreshold []float32
	var getOutsideThreshold []float32

	totalIngested := 0
	totalSpotChecks := 0

	for _, r := range results {
		totalIngested += r.ingested
		totalSpotChecks += r.getSpotChecks

		fmt.Printf("Put\n")
		for r.worstPutQueries.Len() > 0 {
			tookMs := r.worstPutQueries.Pop().Dist * 1000
			fmt.Printf("tookMs: %.2f\n", tookMs)
			if tookMs > float32(putThreshold.Milliseconds()) {
				putOutsideThreshold = append(putOutsideThreshold, tookMs)
			}
		}

		fmt.Printf("Get\n")
		for r.worstGetQueries.Len() > 0 {
			tookMs := r.worstGetQueries.Pop().Dist * 1000
			fmt.Printf("tookMs: %.2f\n", tookMs)
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

type mode struct {
	mu            sync.Mutex
	write         bool
	writeDuration time.Duration
	readDuration  time.Duration
	logger        logrus.FieldLogger
}

func newMode(logger logrus.FieldLogger, writeDuration, readDuration time.Duration) *mode {
	return &mode{
		write:         true,
		writeDuration: writeDuration,
		readDuration:  readDuration,
		logger:        logger,
	}
}

func (m *mode) setWrite() {
	m.mu.Lock()
	m.write = true
	m.mu.Unlock()
	m.logger.Info("switched to WRITE mode")
}

func (m *mode) setRead() {
	m.mu.Lock()
	m.write = false
	m.mu.Unlock()
	m.logger.Info("switched to READ mode")
}

func (m *mode) isWrite() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.write
}

func (m *mode) alternate(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if m.isWrite() {
			time.Sleep(m.writeDuration)
			m.setRead()
		} else {
			time.Sleep(m.readDuration)
			m.setWrite()
		}
	}
}

func worker(ctx context.Context, t *testing.T, mode *mode, wg *sync.WaitGroup, workerID int, bucket *lsmkv.Bucket, logger logrus.FieldLogger,
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

		if mode.isWrite() {
			before := time.Now()
			bucket.Put([]byte(fmt.Sprintf("worker-%d-key-%d", workerID, i)), []byte(fmt.Sprintf("value-%d", i)))
			took := time.Since(before)
			trackWorstQuery(worstPutQueries, i, took, trackWorstQueries)
			if took > putThreshold {
				logger.Warnf("put took too long: %s", time.Since(before))
			}

			if i%100_000 == 0 {
				logger.WithField("current_id", i).Infof("worker %d inserted %d entries", workerID, i)
			}

			i++
			continue
		}

		// read mode
		j := 0
		for j < i {
			if mode.isWrite() || ctx.Err() != nil {
				break
			}

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

			if string(val) != fmt.Sprintf("value-%d", j) {
				t.Errorf("expected value-%d but got %s", j, val)
			}

			trackWorstQuery(worstGetQueries, j, took, trackWorstQueries)

			totalAsserted++
			j += rand.Intn(100)
		}

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

func cursorWorker(ctx context.Context, t *testing.T, wg *sync.WaitGroup, workerID int,
	bucket *lsmkv.Bucket, logger logrus.FieldLogger, startInterval, maxDuration time.Duration,
) {
	defer wg.Done()

	ticker := time.NewTicker(startInterval)

	for {
		select {

		case <-ctx.Done():
			ticker.Stop()
			return

		case <-ticker.C:
			cursorCtx, cancel := context.WithTimeout(ctx, maxDuration)
			defer cancel()

			c := bucket.Cursor()
			keys := 0
			bytesRead := 0
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if cursorCtx.Err() != nil {
					break
				}

				keys++
				bytesRead += len(k) + len(v)
			}
			c.Close()
			logger.WithField("cursor_worker_id", workerID).WithField("keys", keys).WithField("bytes_read", bytesRead).Infof("cursor completed")
		}
	}
}
