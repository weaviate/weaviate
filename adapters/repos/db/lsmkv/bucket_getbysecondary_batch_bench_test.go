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

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/concurrency"
)

// distinctDocIDKeys draws n doc ids without replacement, so every key resolves
// to a stored value. A key that misses costs the same index descent with no
// value to read, which would report a speedup the lookups never deliver.
func distinctDocIDKeys(rng *rand.Rand, numDocs, n int) [][]byte {
	keys := make([][]byte, n)
	for i, docID := range rng.Perm(numDocs)[:n] {
		keys[i] = docIDKey(uint64(docID))
	}
	return keys
}

// requireAllResolve fails the benchmark if any key misses, outside the timed
// region so the check costs nothing per iteration.
func requireAllResolve(b *testing.B, bucket *Bucket, keys [][]byte) {
	b.Helper()
	// visit runs on every worker, so the count is a per-key flag rather than a
	// shared counter.
	resolved := make([]bool, len(keys))
	require.NoError(b, bucket.GetBySecondaryBatch(context.Background(), secondaryPos, keys,
		func(i int, _ []byte) error {
			resolved[i] = true
			return nil
		}))
	for i := range resolved {
		require.Truef(b, resolved[i], "key %d of %d must resolve to a stored value", i, len(keys))
	}

	var buffer []byte
	for _, key := range keys {
		value, grown, err := bucket.GetBySecondaryWithBuffer(context.Background(), secondaryPos, key, buffer)
		require.NoError(b, err)
		require.NotNil(b, value)
		buffer = grown
	}
}

func newBenchmarkSecondaryBucket(b *testing.B, pread bool, numDocs, segments, valueSize int) (*Bucket, *rand.Rand) {
	bucket := newSecondaryTestBucket(b, WithPread(pread))
	rng := rand.New(rand.NewSource(1))
	value := make([]byte, valueSize)
	for d := range uint64(numDocs) {
		rng.Read(value)
		putDoc(b, bucket, d, value)
		if (d+1)%uint64(numDocs/segments) == 0 {
			require.NoError(b, bucket.FlushAndSwitch())
		}
	}
	return bucket, rng
}

// BenchmarkGetBySecondaryBatch compares batched vs per-key lookups, including
// a slow-log variant that serializes each lookup under one shared mutex.
func BenchmarkGetBySecondaryBatch(b *testing.B) {
	const (
		numDocs   = 20_000
		segments  = 4
		valueSize = 1024
	)
	for _, pread := range []bool{false, true} {
		bucket, rng := newBenchmarkSecondaryBucket(b, pread, numDocs, segments, valueSize)

		for _, numKeys := range []int{1, 10, 25, 100, 500} {
			keys := distinctDocIDKeys(rng, numDocs, numKeys)
			requireAllResolve(b, bucket, keys)
			name := fmt.Sprintf("pread=%v/keys=%d", pread, numKeys)

			serial := func(ctx context.Context) error {
				var buffer []byte
				for _, key := range keys {
					_, grown, err := bucket.GetBySecondaryWithBuffer(ctx, secondaryPos, key, buffer)
					if err != nil {
						return err
					}
					buffer = grown
				}
				return nil
			}
			batch := func(ctx context.Context) error {
				return bucket.GetBySecondaryBatch(ctx, secondaryPos, keys, func(int, []byte) error { return nil })
			}
			plainCtx := func() context.Context { return context.Background() }
			slowLogCtx := func() context.Context { return helpers.InitSlowQueryDetails(context.Background()) }

			for _, variant := range []struct {
				name string
				run  func(context.Context) error
				ctx  func() context.Context
			}{
				{name: "serial", run: serial, ctx: plainCtx},
				{name: "batch", run: batch, ctx: plainCtx},
				{name: "serial-slowlog", run: serial, ctx: slowLogCtx},
				{name: "batch-slowlog", run: batch, ctx: slowLogCtx},
			} {
				b.Run(name+"/"+variant.name, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if err := variant.run(variant.ctx()); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkGetBySecondaryBatchShape sweeps the two constants the fan-out is
// sized by, so raising or lowering either one can be measured rather than
// argued.
func BenchmarkGetBySecondaryBatchShape(b *testing.B) {
	const (
		numDocs   = 20_000
		segments  = 4
		valueSize = 1024
		numKeys   = 500
	)
	for _, pread := range []bool{false, true} {
		bucket, rng := newBenchmarkSecondaryBucket(b, pread, numDocs, segments, valueSize)
		keys := distinctDocIDKeys(rng, numDocs, numKeys)
		requireAllResolve(b, bucket, keys)

		for _, workers := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("pread=%v/keys=%d/workers=%d", pread, numKeys, workers), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					ctx := concurrency.CtxWithBudget(context.Background(), workers)
					err := bucket.getBySecondaryBatch(ctx, secondaryPos, keys, secondaryBatchChunkSize, workers,
						func(int, []byte) error { return nil })
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}

		for _, chunkSize := range []int{8, 16, 32, 64, 128} {
			b.Run(fmt.Sprintf("pread=%v/keys=%d/chunk=%d", pread, numKeys, chunkSize), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					err := bucket.getBySecondaryBatch(context.Background(), secondaryPos, keys,
						chunkSize, secondaryBatchWorkers, func(int, []byte) error { return nil })
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
