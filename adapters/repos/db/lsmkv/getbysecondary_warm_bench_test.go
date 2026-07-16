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

package lsmkv

// Warm-shape (values-resident / page-cache-fast) wall-time microbenchmark for the
// batched secondary resolver (gh#309). QA's cold-cache A/B (PR 12164) found a
// 1.05-1.15x WARM BM25 regression that grows with batch size: when every value read is
// a page-cache hit the phase-2 concurrency machinery (offset sort + per-hit goroutine
// dispatch + semaphore) costs more than it saves. This bench isolates the resolution
// step (no BM25, no decode) and compares, under ONE shared consistent view so view
// churn is not a variable:
//   - serial-view: a per-key GetBySecondaryWithBufferAndView loop (the batch's serial twin)
//   - batch:       GetBySecondaryBatchWithView (phase 0..3, concurrent phase 2)
// at batch sizes 100 and 500. Run with -cpuprofile/-memprofile to attribute the delta.

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// benchWarmShape is the warm profiling shape: the LTK-profiled hot-shard segment
// count with a modest keys-per-segment so the whole corpus stays resident in the OS
// page cache across bench iterations (warm by construction).
func benchWarmShape() readOpsScale {
	s := scaledShape()
	s.name = "warm-resident"
	return s
}

// warmBenchKeys picks n distinct resolvable secondary keys (encoded docIDs) at random.
func warmBenchKeys(docIDs []uint64, n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	picked := pickRandom(rng, docIDs, n)
	keys := make([][]byte, len(picked))
	for i, id := range picked {
		keys[i] = encodeDocID(id)
	}
	return keys
}

// BenchmarkGetBySecondaryWarmResident is the attribution instrument for the warm
// regression. Sub-benchmarks: {serial-view, batch} x {100, 500}. The bucket is built
// once and reused across b.N iterations, so the page cache stays warm (values resident).
func BenchmarkGetBySecondaryWarmResident(b *testing.B) {
	ctx := context.Background()
	bucket, _, docIDs := buildReadOpsBucket(b, benchWarmShape())

	// prime the page cache: resolve every key once so all value pages are resident
	// before any timed iteration.
	view := bucket.GetConsistentView()
	for _, id := range docIDs {
		_, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, secondaryPos, encodeDocID(id), nil, view)
		if err != nil {
			b.Fatalf("warmup resolve: %v", err)
		}
	}
	view.ReleaseView()

	for _, batchSize := range []int{100, 500} {
		keys := warmBenchKeys(docIDs, batchSize, int64(batchSize))

		b.Run("serial-view/"+itoa(batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v := bucket.GetConsistentView()
				for _, k := range keys {
					if _, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, secondaryPos, k, nil, v); err != nil {
						b.Fatalf("serial-view resolve: %v", err)
					}
				}
				v.ReleaseView()
			}
		})

		// parallel-view mirrors QA's BASE storobj shape: 2xGOMAXPROCS workers, each
		// resolving a contiguous chunk serially under the SAME shared view. This is the
		// already-concurrent baseline the batch path must beat (or at least match) warm.
		b.Run("parallel-view/"+itoa(batchSize), func(b *testing.B) {
			b.ReportAllocs()
			parallel := 2 * runtime.GOMAXPROCS(0)
			for i := 0; i < b.N; i++ {
				v := bucket.GetConsistentView()
				chunk := (len(keys) + parallel - 1) / parallel
				var wg sync.WaitGroup
				for w := 0; w < parallel; w++ {
					start := w * chunk
					if start >= len(keys) {
						break
					}
					end := start + chunk
					if end > len(keys) {
						end = len(keys)
					}
					wg.Add(1)
					go func(start, end int) {
						defer wg.Done()
						for _, k := range keys[start:end] {
							if _, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, secondaryPos, k, nil, v); err != nil {
								b.Errorf("parallel-view resolve: %v", err)
								return
							}
						}
					}(start, end)
				}
				wg.Wait()
				v.ReleaseView()
			}
		})

		b.Run("batch/"+itoa(batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v := bucket.GetConsistentView()
				if _, err := bucket.GetBySecondaryBatchWithView(ctx, secondaryPos, keys, v); err != nil {
					b.Fatalf("batch resolve: %v", err)
				}
				v.ReleaseView()
			}
		})
	}
}

// BenchmarkGetBySecondaryWarmConcurrentBatches measures the shape the fix actually
// targets: many batches resolving at once on the same node (the LTK cold-load shape,
// warm here). Per-hit goroutine dispatch spawns len(hits) goroutines PER batch, so Q
// concurrent 500-key batches put ~500*Q goroutines through the scheduler; the fixed
// worker pool puts ~16*Q. Single-batch wall time on an idle box hides this (reads run on
// OS threads off the critical path); under concurrent load the scheduler pressure shows.
func BenchmarkGetBySecondaryWarmConcurrentBatches(b *testing.B) {
	ctx := context.Background()
	bucket, _, docIDs := buildReadOpsBucket(b, benchWarmShape())

	view := bucket.GetConsistentView()
	for _, id := range docIDs {
		if _, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, secondaryPos, encodeDocID(id), nil, view); err != nil {
			b.Fatalf("warmup resolve: %v", err)
		}
	}
	view.ReleaseView()

	const batchSize = 500
	keys := warmBenchKeys(docIDs, batchSize, 500)

	for _, q := range []int{8, 16, 32} {
		b.Run("Q="+itoa(q), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for j := 0; j < q; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if _, err := bucket.GetBySecondaryBatch(ctx, secondaryPos, keys); err != nil {
							b.Errorf("batch resolve: %v", err)
						}
					}()
				}
				wg.Wait()
			}
		})
	}
}

// itoa avoids importing strconv purely for a bench label.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
