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

package hfresh

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// BenchmarkScanComponents measures the per-candidate cost of each piece of
// the posting-scan inner loop, to decide where optimization pays. Run with:
//
//	go test -bench BenchmarkScanComponents -benchtime 2s -run xxx ./adapters/repos/db/vector/hfresh/
func BenchmarkScanComponents(b *testing.B) {
	const postingLen = 512

	for _, dims := range []int{256, 1536} {
		quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, 42, distancer.NewCosineDistanceProvider())
		require.NoError(b, err)

		vectors, queries := testinghelpers.RandomVecsFixedSeed(postingLen, 1, dims)
		query := distancer.Normalize(queries[0])
		queryDistancer := quantizer.NewDistancer(query)

		posting := make(Posting, postingLen)
		for i, v := range vectors {
			code := quantizer.CompressedBytes(quantizer.Encode(distancer.Normalize(v)))
			posting[i] = NewVector(uint64(i), 1, code)
		}

		var decompressBuf []uint64

		b.Run(fmt.Sprintf("dims=%d/decode+distance", dims), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				v := posting[i%postingLen]
				decompressBuf = quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
				_, err := queryDistancer.Distance(decompressBuf)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("dims=%d/decode-only", dims), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				v := posting[i%postingLen]
				decompressBuf = quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			}
		})

		decoded := make([][]uint64, postingLen)
		for i, v := range posting {
			decoded[i] = quantizer.FromCompressedBytesInto(v.Data(), nil)
		}
		b.Run(fmt.Sprintf("dims=%d/distance-only", dims), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				_, err := queryDistancer.Distance(decoded[i%postingLen])
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// version-map lookup and visited-set costs are dimension-independent
	store := testinghelpers.NewDummyStore(b)
	bucket, err := NewSharedBucket(store, "bench", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(b, err)
	vm := NewVersionMap(bucket)
	for i := range uint64(postingLen) {
		_, err := vm.Increment(b.Context(), i, 1)
		require.NoError(b, err)
	}

	b.Run("versionmap-isdeleted", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, err := vm.IsDeleted(b.Context(), uint64(i%postingLen))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	pool := visited.NewPool(512)
	v := pool.Borrow()
	defer pool.Return(v)
	b.Run("visited-checkandvisit", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			v.CheckAndVisit(uint64(i % postingLen))
		}
	})
}
