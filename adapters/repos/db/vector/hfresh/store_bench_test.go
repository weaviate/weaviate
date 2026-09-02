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
	"context"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// A search fans MultiGet out over every selected centroid, so this is where
// per-access bucket pinning is felt: the pin is taken against a store-wide
// lock shared by every bucket on the shard. Run with -cpu to see it under
// concurrency, which is how it is actually reached.
func BenchmarkPostingStoreMultiGet(b *testing.B) {
	ctx := context.Background()

	const (
		postingCount    = 64
		vectorsPerPost  = 32
		vectorDimension = 8
	)

	store := testinghelpers.NewDummyStore(b)
	cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
	shared, err := NewSharedBucket(store, "bench", cfg)
	if err != nil {
		b.Fatal(err)
	}
	postings, err := NewPostingStore(store, shared, NewMetrics(nil, "n/a", "n/a"), "bench", cfg)
	if err != nil {
		b.Fatal(err)
	}

	ids := make([]uint64, 0, postingCount)
	for postingID := range uint64(postingCount) {
		var posting Posting
		for v := range uint64(vectorsPerPost) {
			posting = posting.AddVector(NewVector(postingID*vectorsPerPost+v, 1,
				make([]byte, vectorDimension)))
		}
		if err := postings.Put(ctx, postingID, posting); err != nil {
			b.Fatal(err)
		}
		ids = append(ids, postingID)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := postings.MultiGet(ctx, ids); err != nil {
				b.Fatal(err)
			}
		}
	})
}
