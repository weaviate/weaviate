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

package compressionhelpers

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type compressedParallelIterator[T byte | uint64] struct {
	bucket              *lsmkv.Bucket
	parallel            int
	logger              logrus.FieldLogger
	loadId              func([]byte) uint64
	fromCompressedBytes func(compressed []byte) []T

	// a simple counter that each routine can write to atomically. It is used to
	// track progress and display it to the user.
	loaded atomic.Int64
	// rather than tracking every tick which adds a lot of synchronization overhead,
	// only track every trackInterval-th tick.
	trackInterval int64
	// how often to report progress to the user (via logs)
	reportProgressInterval time.Duration
}

func NewCompressedParallelIterator[T byte | uint64](bucket *lsmkv.Bucket, parallel int, loadId func([]byte) uint64, fromCompressedBytes func(compressed []byte) []T,
	logger logrus.FieldLogger,
) *compressedParallelIterator[T] {
	return &compressedParallelIterator[T]{
		bucket:                 bucket,
		parallel:               parallel,
		logger:                 logger,
		loadId:                 loadId,
		fromCompressedBytes:    fromCompressedBytes,
		trackInterval:          1000,
		reportProgressInterval: 5 * time.Second,
	}
}

func (cpi *compressedParallelIterator[T]) IterateAll() chan []VecAndID[T] {
	if cpi.parallel <= 1 {
		// caller explicitly wants no parallelism, fallback to regular cursor
		return cpi.iterateAllNoConcurrency()
	}

	stopTracking := cpi.startTracking()
	defer func() {
	}()

	// We need one fewer seed than our desired parallel factor, that is because
	// we will add one routine that starts with cursor.First() and reads to the
	// first checkpoint, therefore we will have len(checkpoints) + 1 routines in
	// total.
	seedCount := cpi.parallel - 1
	seeds := cpi.bucket.QuantileKeys(seedCount)
	if len(seeds) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	out := make(chan []VecAndID[T])

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	extract := func(k, v []byte) VecAndID[T] {
		vc := make([]byte, len(v))
		copy(vc, v)
		id := cpi.loadId(k)
		vec := cpi.fromCompressedBytes(vc)
		return VecAndID[T]{id: id, vec: vec}
	}

	// S1: Read from beginning to first checkpoint:
	wg.Add(1)
	enterrors.GoWrapper(func() {
		c := cpi.bucket.Cursor()
		localResults := make([]VecAndID[T], 0, 10_000)
		defer c.Close()
		defer wg.Done()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			if len(k) == 0 {
				cpi.logger.WithFields(logrus.Fields{
					"action": "hnsw_compressed_vector_cache_prefill",
					"len":    len(v),
					"lenk":   len(k),
				}).Warn("skipping compressed vector with unexpected length")
				continue
			}

			localResults = append(localResults, extract(k, v))
			cpi.trackIndividual(len(localResults))
		}

		out <- localResults
	}, cpi.logger)

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		wg.Add(1)
		start := seeds[i]
		end := seeds[i+1]

		enterrors.GoWrapper(func() {
			defer wg.Done()
			localResults := make([]VecAndID[T], 0, 10_000)
			c := cpi.bucket.Cursor()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				if len(k) == 0 {
					cpi.logger.WithFields(logrus.Fields{
						"action": "hnsw_compressed_vector_cache_prefill",
						"len":    len(v),
						"lenk":   len(k),
					}).Warn("skipping compressed vector with unexpected length")
					continue
				}
				localResults = append(localResults, extract(k, v))
				cpi.trackIndividual(len(localResults))
			}

			out <- localResults
		}, cpi.logger)
	}

	// S3: Read from last checkpoint to end:
	wg.Add(1)
	enterrors.GoWrapper(func() {
		c := cpi.bucket.Cursor()
		defer c.Close()
		defer wg.Done()
		localResults := make([]VecAndID[T], 0, 10_000)

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			if len(k) == 0 {
				cpi.logger.WithFields(logrus.Fields{
					"action": "hnsw_compressed_vector_cache_prefill",
					"len":    len(v),
					"lenk":   len(k),
				}).Warn("skipping compressed vector with unexpected length")
				continue
			}

			localResults = append(localResults, extract(k, v))
			cpi.trackIndividual(len(localResults))
		}

		out <- localResults
	}, cpi.logger)

	enterrors.GoWrapper(func() {
		wg.Wait()
		close(out)
		stopTracking()
	}, cpi.logger)

	return out
}

func (cpi *compressedParallelIterator[T]) iterateAllNoConcurrency() chan []VecAndID[T] {
	out := make(chan []VecAndID[T])
	stopTracking := cpi.startTracking()
	enterrors.GoWrapper(func() {
		defer close(out)
		c := cpi.bucket.Cursor()
		defer c.Close()
		defer stopTracking()

		localResults := make([]VecAndID[T], 0, 10_000)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) == 0 {
				cpi.logger.WithFields(logrus.Fields{
					"action": "hnsw_compressed_vector_cache_prefill",
					"len":    len(v),
					"lenk":   len(k),
				}).Warn("skipping compressed vector with unexpected length")
				continue
			}
			vc := make([]byte, len(v))
			copy(vc, v)
			id := cpi.loadId(k)
			vec := cpi.fromCompressedBytes(vc)
			localResults = append(localResults, VecAndID[T]{id: id, vec: vec})
			cpi.trackIndividual(len(localResults))
		}

		out <- localResults
	}, cpi.logger)

	return out
}

func (cpi *compressedParallelIterator[T]) startTracking() func() {
	cpi.loaded.Store(0)

	t := time.NewTicker(cpi.reportProgressInterval)
	cancel := make(chan struct{})

	enterrors.GoWrapper(func() {
		start := time.Now()
		lastReported := start
		last := int64(0)

		p := message.NewPrinter(language.English)

		for {
			select {
			case now := <-t.C:
				loaded := cpi.loaded.Load()
				elapsed := now.Sub(start)
				elapsedSinceLast := now.Sub(lastReported)
				rate := float64(loaded-last) / elapsedSinceLast.Seconds()
				totalRate := float64(loaded) / elapsed.Seconds()

				cpi.logger.WithFields(logrus.Fields{
					"action":                "hnsw_compressed_vector_cache_prefill_progress",
					"loaded":                loaded,
					"rate_per_second":       rate,
					"total_rate_per_second": totalRate,
					"elapsed_total":         elapsed,
				}).Infof("loaded %s vectors in %s so far, current rate is %s vectors/s, total rate is %s vectors/s",
					p.Sprintf("%d", loaded), elapsed.Round(10*time.Millisecond), p.Sprintf("%.0f", rate), p.Sprintf("%.0f", totalRate))

				last = loaded
			case <-cancel:
				t.Stop()
				close(cancel)
				return
			}
		}
	}, cpi.logger)

	return func() {
		cancel <- struct{}{}
	}
}

func (cpi *compressedParallelIterator[T]) trackIndividual(loaded int) {
	// Possibly a premature optimization, the idea is to reduce the necessary
	// synchronization when we load from hundreds of goroutines in parallel.
	// Rather than tracking every single tick, we track in chunks of
	// cpi.trackInterval.
	if int64(loaded)%cpi.trackInterval == 0 {
		cpi.loaded.Add(cpi.trackInterval)
	}
}

type VecAndID[T uint64 | byte] struct {
	id  uint64
	vec []T
}
