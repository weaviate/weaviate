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
)

// BenchmarkGetBySecondaryBatch compares batched vs per-key lookups, including
// a slow-log variant that serializes each lookup under one shared mutex.
func BenchmarkGetBySecondaryBatch(b *testing.B) {
	const (
		numDocs   = 20_000
		segments  = 4
		valueSize = 1024
	)
	for _, pread := range []bool{false, true} {
		bucket := newSecondaryTestBucket(b, WithPread(pread))
		rng := rand.New(rand.NewSource(1))
		value := make([]byte, valueSize)
		for d := range uint64(numDocs) {
			rng.Read(value)
			putDoc(b, bucket, d, value)
			if (d+1)%(numDocs/segments) == 0 {
				require.NoError(b, bucket.FlushAndSwitch())
			}
		}

		for _, numKeys := range []int{10, 100, 500} {
			keys := make([][]byte, numKeys)
			for i := range keys {
				keys[i] = docIDKey(uint64(rng.Intn(numDocs)))
			}
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
