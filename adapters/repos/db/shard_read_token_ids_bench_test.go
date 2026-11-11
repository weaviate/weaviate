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

package db

import (
	"context"
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// BenchmarkShardTokenIDsByIndexID is the read the tachiomfresh index makes
// for every document it inserts: the document's token ids, from the stored
// object, by doc id. The objects hold one multi-vector of tokens x dims
// floats and its token ids, written as the shard writes them. Objects still
// in the memtable are the usual case, since the index queue inserts soon
// after the write; objects in a segment are the other.
func BenchmarkShardTokenIDsByIndexID(b *testing.B) {
	const (
		target  = "colbert"
		objects = 2000
		tokens  = 147
		dims    = 128
	)
	for _, tt := range []struct {
		name  string
		flush bool
	}{
		{name: "memtable"},
		{name: "segment", flush: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			ctx := context.Background()
			shd, _ := testShard(b, ctx, "TestClass")
			shard := shd.(*Shard)
			bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
			require.NotNil(b, bucket)

			rng := rand.New(rand.NewSource(1))
			ids := make([]uint32, tokens)
			for i := range ids {
				ids[i] = uint32(rng.Intn(30000))
			}
			for docID := range uint64(objects) {
				doc := make([][]float32, tokens)
				for i := range doc {
					doc[i] = make([]float32, dims)
					for d := range doc[i] {
						doc[i][d] = rng.Float32()
					}
				}
				obj := storobj.New(docID)
				uid := uuid.New()
				obj.Object = models.Object{
					ID: strfmt.UUID(uid.String()), Class: "TestClass",
					TokenIds: models.TokenIds{target: ids},
				}
				obj.MultiVectors = map[string][][]float32{target: doc}
				data, err := obj.MarshalBinaryDisk(false)
				require.NoError(b, err)
				require.NoError(b, bucket.Put(uid[:], data, lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDKey(docID))))
			}
			if tt.flush {
				require.NoError(b, bucket.FlushMemtable())
			}
			got, err := shard.tokenIDsByIndexID(ctx, 0, target)
			require.NoError(b, err)
			require.Equal(b, ids, got)

			b.ReportAllocs()
			var i uint64
			for b.Loop() {
				if _, err := shard.tokenIDsByIndexID(ctx, i%objects, target); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}
