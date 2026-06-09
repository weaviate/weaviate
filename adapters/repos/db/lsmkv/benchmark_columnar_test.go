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

// Benchmarks comparing the two ways of reading a single numeric property per
// docID:
//
//  1. Objects bucket (Replace): fetch the full serialized object by docID,
//     selectively unmarshal one property — what filtered aggregations,
//     boost rescoring, and sort fallbacks pay on main.
//  2. Columnar bucket: 8-byte value access by docID.
//
// Two access shapes: random point lookups (boost rescoring over a candidate
// pool) and filtered scans across a selectivity sweep (aggregations).
//
// The "Realistic" object profile models the LLM-memory use case: a 1536-dim
// vector and ~4KB of text per object, so every object-bucket read drags in
// ~10KB to serve 8 bytes.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/go-openapi/strfmt"
)

const (
	benchObjCount  = 100_000
	benchPointGets = 1_000 // candidate-pool size for the rescore shape
)

type benchObjectProfile struct {
	name       string
	vectorDims int
	textSize   int
}

var benchProfiles = []benchObjectProfile{
	{name: "Small", vectorDims: 128, textSize: 200},
	{name: "Realistic", vectorDims: 1536, textSize: 4096},
}

func benchMakeObject(p benchObjectProfile, docID uint64, ts int64) *storobj.Object {
	const sentence = "The user asked about vector databases and how they handle semantic search with time decay for conversation memory retrieval. "
	repeats := p.textSize / len(sentence)
	if repeats < 1 {
		repeats = 1
	}

	vec := make([]float32, p.vectorDims)
	for i := range vec {
		vec[i] = float32(docID)*0.001 + float32(i)*0.0001
	}

	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.New().String()),
			Class: "ConversationMemory",
			Properties: map[string]interface{}{
				"title":        fmt.Sprintf("Conversation #%d", docID),
				"conversation": strings.Repeat(sentence, repeats),
				"updated":      ts,
				"turn_count":   float64(docID % 50),
			},
			CreationTimeUnix:   ts,
			LastUpdateTimeUnix: ts,
		},
		Vector: vec,
		DocID:  docID,
	}
}

type columnarBenchFixture struct {
	objects  *Bucket
	column   *Bucket
	docIDs   []uint64
	expected []int64
}

func setupColumnarBenchFixture(b *testing.B, p benchObjectProfile) *columnarBenchFixture {
	b.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	objects, err := NewBucketCreator().NewBucket(ctx, b.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(b, err)
	b.Cleanup(func() { require.Nil(b, objects.Shutdown(context.Background())) })

	column, err := NewBucketCreator().NewBucket(ctx, b.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(&columnar.Schema{
			Columns: []columnar.Column{{Name: "updated", Type: columnar.ColumnTypeInt64}},
		}))
	require.Nil(b, err)
	b.Cleanup(func() { require.Nil(b, column.Shutdown(context.Background())) })

	f := &columnarBenchFixture{
		objects:  objects,
		column:   column,
		docIDs:   make([]uint64, benchObjCount),
		expected: make([]int64, benchObjCount),
	}

	for i := uint64(0); i < benchObjCount; i++ {
		ts := int64(1_700_000_000_000_000_000) + int64(i)*1_000_000_000
		obj := benchMakeObject(p, i, ts)

		data, err := obj.MarshalBinary()
		require.Nil(b, err)
		docIDBuf := make([]byte, 8) // fresh per put — the memtable retains the slice
		binary.LittleEndian.PutUint64(docIDBuf, i)
		require.Nil(b, f.objects.Put([]byte(obj.ID()), data, WithSecondaryKey(0, docIDBuf)))

		require.Nil(b, f.column.ColumnarPutInt64(i, 0, ts))

		f.docIDs[i] = i
		f.expected[i] = ts
	}

	require.Nil(b, f.objects.FlushAndSwitch())
	require.Nil(b, f.column.FlushAndSwitch())
	return f
}

// readUpdatedFromObject is the object-bucket path: full object read by
// secondary key (docID) + selective property unmarshal.
func (f *columnarBenchFixture) readUpdatedFromObject(b *testing.B, docID uint64, keyBuf []byte) int64 {
	binary.LittleEndian.PutUint64(keyBuf, docID)
	data, err := f.objects.GetBySecondary(context.Background(), 0, keyBuf)
	require.Nil(b, err)
	require.NotNil(b, data)

	props := map[string]interface{}{}
	require.Nil(b, storobj.UnmarshalPropertiesFromObject(data, props, [][]string{{"updated"}}))
	switch v := props["updated"].(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		b.Fatalf("unexpected type %T for updated", v)
		return 0
	}
}

func BenchmarkColumnarVsObjects_PointLookup(b *testing.B) {
	for _, p := range benchProfiles {
		f := setupColumnarBenchFixture(b, p)

		rnd := rand.New(rand.NewSource(42))
		candidates := make([]uint64, benchPointGets)
		for i := range candidates {
			candidates[i] = uint64(rnd.Intn(benchObjCount))
		}

		b.Run(p.name+"/Objects", func(b *testing.B) {
			keyBuf := make([]byte, 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docID := candidates[i%len(candidates)]
				got := f.readUpdatedFromObject(b, docID, keyBuf)
				if got != f.expected[docID] {
					b.Fatalf("docID %d: got %d want %d", docID, got, f.expected[docID])
				}
			}
		})

		b.Run(p.name+"/Columnar", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docID := candidates[i%len(candidates)]
				got, ok := f.column.ColumnarLookupInt64(docID, 0)
				if !ok || got != f.expected[docID] {
					b.Fatalf("docID %d: got %d,%v want %d", docID, got, ok, f.expected[docID])
				}
			}
		})
	}
}

func BenchmarkColumnarVsObjects_FilteredSum(b *testing.B) {
	// aggregation shape: SUM(updated) over an allow list across selectivities
	p := benchProfiles[1] // Realistic
	f := setupColumnarBenchFixture(b, p)

	for _, selectivity := range []float64{0.01, 0.10, 0.50, 1.00} {
		rnd := rand.New(rand.NewSource(7))
		allow := sroar.NewBitmap()
		ids := make([]uint64, 0, int(float64(benchObjCount)*selectivity))
		for i := uint64(0); i < benchObjCount; i++ {
			if rnd.Float64() < selectivity {
				allow.Set(i)
				ids = append(ids, i)
			}
		}

		name := fmt.Sprintf("sel=%d%%", int(selectivity*100))

		b.Run(name+"/Objects", func(b *testing.B) {
			keyBuf := make([]byte, 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var sum int64
				for _, docID := range ids {
					sum += f.readUpdatedFromObject(b, docID, keyBuf)
				}
				if sum == 0 {
					b.Fatal("zero sum")
				}
			}
		})

		b.Run(name+"/Columnar", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var sum int64
				err := f.column.ColumnarScan(0, allow, func(_ uint64, bits uint64) bool {
					sum += int64(bits)
					return true
				})
				if err != nil || sum == 0 {
					b.Fatalf("scan: %v sum=%d", err, sum)
				}
			}
		})
	}
}
