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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Benchmarks for the vector-column rescore path. Both benchmarks A/B the
// same shard and the same HNSW graph: the column toggle only changes where
// rescoring reads its uncompressed vectors from (vector column vs object
// binary), so candidate sets are identical between the legs.

// BenchmarkVectorColumnRescoreRQ8 measures a full SearchByVector with RQ-8
// quantization, where every rescore candidate is a disk read (RQ drops the
// float cache). RescoreLimit is raised to make the fetch phase dominant —
// the "rerank wider candidate sets" scenario.
func BenchmarkVectorColumnRescoreRQ8(b *testing.B) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 1536
	n := 25_000
	k := 10
	queries := 64

	cfg := enthnsw.NewDefaultUserConfig()
	cfg.RQ.Enabled = true
	cfg.RQ.Bits = 8
	cfg.RQ.RescoreLimit = 200
	cfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(b, ctx, vectorColumnTestClass("BenchVectorColumnRQ8"),
		cfg, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(b, err)

	objs := createRandomObjects(r, "BenchVectorColumnRQ8", n, dims)
	for _, obj := range objs {
		require.NoError(b, shard.PutObject(ctx, obj))
	}
	require.NotNil(b, concrete.servableVectorColumnBucket(""))

	vidx, ok := shard.GetVectorIndex("")
	require.True(b, ok)

	qs := make([][]float32, queries)
	for i := range qs {
		qs[i] = randomVector(r, dims)
	}

	runSearches := func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := qs[i%len(qs)]
			ids, _, err := vidx.SearchByVector(ctx, q, k, nil)
			if err != nil || len(ids) != k {
				b.Fatalf("search: err=%v len=%d", err, len(ids))
			}
		}
	}

	b.Run("Columnar", runSearches)

	off := cfg
	off.ColumnarRescore = false
	concrete.applyVectorColumnConfig("", off)
	require.Nil(b, concrete.servableVectorColumnBucket(""))

	b.Run("Objects", runSearches)
}

// BenchmarkVectorColumnFetch1536d isolates the per-candidate fetch: one
// uncompressed 1536-dim vector by docID, column vs object binary. This is
// the unit cost the rescore loop pays RescoreLimit times per query.
func BenchmarkVectorColumnFetch1536d(b *testing.B) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 1536
	n := 10_000

	cfg := enthnsw.NewDefaultUserConfig()
	cfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(b, ctx, vectorColumnTestClass("BenchVectorColumnFetch"),
		cfg, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(b, err)

	objs := createRandomObjects(r, "BenchVectorColumnFetch", n, dims)
	for _, obj := range objs {
		require.NoError(b, shard.PutObject(ctx, obj))
	}
	require.NotNil(b, concrete.servableVectorColumnBucket(""))

	docIDs := make([]uint64, len(objs))
	for i, obj := range objs {
		docIDs[i] = obj.DocID
	}

	// flush the column so every row is segment-resident: this is the regime
	// that matters at scale, and the precondition for the zero-copy leg
	// (memtable rows fall back to the copy path by design)
	require.NoError(b, concrete.servableVectorColumnBucket("").FlushAndSwitch())

	view := concrete.GetObjectsBucketView()
	defer view.ReleaseView()

	b.Run("Objects", func(b *testing.B) {
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, err := concrete.readVectorByIndexIDIntoSliceWithView(ctx, docIDs[i%n], container, "", view)
			if err != nil || len(vec) != dims {
				b.Fatalf("fetch: err=%v len=%d", err, len(vec))
			}
		}
	})

	b.Run("Columnar", func(b *testing.B) {
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, err := concrete.readVectorColumnIntoSliceWithView(ctx, docIDs[i%n], container, "", view)
			if err != nil || len(vec) != dims {
				b.Fatalf("fetch: err=%v len=%d", err, len(vec))
			}
		}
	})

	// the rescore-pass shape: one composite view pins the column's segments,
	// every fetch returns a []float32 aliasing the pinned mmap (zero copy)
	b.Run("ColumnarZeroCopy", func(b *testing.B) {
		zcView := concrete.getVectorColumnRescoreView("")
		defer zcView.ReleaseView()
		if _, ok := zcView.(*vectorColumnRescoreView); !ok {
			b.Fatal("expected the composite rescore view")
		}
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, err := concrete.readVectorColumnIntoSliceWithView(ctx, docIDs[i%n], container, "", zcView)
			if err != nil || len(vec) != dims {
				b.Fatalf("fetch: err=%v len=%d", err, len(vec))
			}
		}
	})
}

// BenchmarkVectorColumnFetchRealistic is the fetch benchmark with
// production-shaped objects: a 1536-dim vector plus ~4KB of text props (the
// LLM-memory profile). The object-binary path pays for the full blob; the
// column path is unaffected by object width.
func BenchmarkVectorColumnFetchRealistic(b *testing.B) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 1536
	n := 10_000

	cfg := enthnsw.NewDefaultUserConfig()
	cfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(b, ctx, vectorColumnTestClass("BenchVectorColumnFetchReal"),
		cfg, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(b, err)

	text := make([]byte, 4096)
	for i := range text {
		text[i] = byte('a' + i%26)
	}
	objs := createRandomObjects(r, "BenchVectorColumnFetchReal", n, dims)
	for _, obj := range objs {
		obj.Object.Properties = map[string]interface{}{
			"conversation": string(text),
			"title":        "memory entry",
			"turn_count":   float64(obj.DocID % 50),
		}
		require.NoError(b, shard.PutObject(ctx, obj))
	}
	require.NotNil(b, concrete.servableVectorColumnBucket(""))

	docIDs := make([]uint64, len(objs))
	for i, obj := range objs {
		docIDs[i] = obj.DocID
	}

	view := concrete.GetObjectsBucketView()
	defer view.ReleaseView()

	b.Run("Objects", func(b *testing.B) {
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, err := concrete.readVectorByIndexIDIntoSliceWithView(ctx, docIDs[i%n], container, "", view)
			if err != nil || len(vec) != dims {
				b.Fatalf("fetch: err=%v len=%d", err, len(vec))
			}
		}
	})

	b.Run("Columnar", func(b *testing.B) {
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, err := concrete.readVectorColumnIntoSliceWithView(ctx, docIDs[i%n], container, "", view)
			if err != nil || len(vec) != dims {
				b.Fatalf("fetch: err=%v len=%d", err, len(vec))
			}
		}
	})
}

// BenchmarkVectorColumnMuvera measures SearchByMultiVector with MUVERA
// encoding, where exact reranking fetches full token matrices per
// candidate. The doc cache is capped far below the dataset size so the
// benchmark measures the disk path — the regime that matters at scale.
func BenchmarkVectorColumnMuvera(b *testing.B) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 128
	tokens := 32
	n := 5_000
	k := 10
	queries := 32
	target := "colbert"

	multiCfg := enthnsw.NewDefaultUserConfig()
	multiCfg.Multivector.Enabled = true
	multiCfg.Multivector.MuveraConfig.Enabled = true
	multiCfg.ColumnarRescore = true
	multiCfg.VectorCacheMaxObjects = 64 // dataset must not fit the doc cache

	shard, _ := testShardWithSettings(b, ctx, vectorColumnTestClass("BenchVectorColumnMuvera"),
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(idx *Index) {
			idx.vectorIndexUserConfigs[target] = multiCfg
		})
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(b, err)

	objs := make([]*storobj.Object, n)
	for i := range objs {
		matrix := make([][]float32, tokens)
		for j := range matrix {
			matrix[j] = randomVector(r, dims)
		}
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: "BenchVectorColumnMuvera",
			},
			MultiVectors: map[string][][]float32{target: matrix},
		}
		require.NoError(b, shard.PutObject(ctx, objs[i]))
	}
	require.NotNil(b, concrete.servableVectorColumnBucket(target))

	vidx, ok := shard.GetVectorIndex(target)
	require.True(b, ok)
	multiIdx, ok := vidx.(VectorIndexMulti)
	require.True(b, ok)

	qs := make([][][]float32, queries)
	for i := range qs {
		qs[i] = make([][]float32, 8)
		for j := range qs[i] {
			qs[i][j] = randomVector(r, dims)
		}
	}

	runSearches := func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := qs[i%len(qs)]
			ids, _, err := multiIdx.SearchByMultiVector(ctx, q, k, nil)
			if err != nil || len(ids) != k {
				b.Fatalf("search: err=%v len=%d", err, len(ids))
			}
		}
	}

	b.Run("Columnar", runSearches)

	off := multiCfg
	off.ColumnarRescore = false
	concrete.applyVectorColumnConfig(target, off)
	require.Nil(b, concrete.servableVectorColumnBucket(target))

	b.Run("Objects", runSearches)
}
