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
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// This benchmark compares two approaches for reading a single numeric property
// (a timestamp) from stored objects:
//
//  1. Objects bucket (Replace strategy): read the full serialized object by
//     docID (uint64 key), then extract the "updated" property via
//     storobj.UnmarshalPropertiesFromObject.
//
//  2. Columnar bucket: read the int64 timestamp directly by docID.
//
// Both buckets are flushed to disk so the reads hit segments, not memtables.
//
// Two object profiles are tested:
//   - Small: 128-dim vector, ~200B text (baseline)
//   - Realistic: 1536-dim vector, ~4KB conversation text (LLM memory use case)

const (
	benchObjCount  = 100_000
	benchReadCount = 10_000
)

// objectProfile describes the shape of test objects for a benchmark variant.
type objectProfile struct {
	name       string
	vectorDims int
	textSize   int // approximate bytes of conversation text
}

var (
	profileSmall = objectProfile{
		name:       "Small",
		vectorDims: 128,
		textSize:   200,
	}
	profileRealistic = objectProfile{
		name:       "Realistic",
		vectorDims: 1536,
		textSize:   4096,
	}
)

// makeTestObject builds a storobj.Object matching the given profile.
func makeTestObject(p objectProfile, docID uint64, ts int64) *storobj.Object {
	id := uuid.New()

	// Build conversation text to the requested size.
	const sentence = "The user asked about vector databases and how they handle semantic search with time decay for conversation memory retrieval. "
	repeats := p.textSize / len(sentence)
	if repeats < 1 {
		repeats = 1
	}
	conversationText := strings.Repeat(sentence, repeats)

	props := map[string]interface{}{
		"title":        fmt.Sprintf("Conversation #%d", docID),
		"conversation": conversationText,
		"updated":      ts,
		"user_id":      fmt.Sprintf("user_%d", docID%1000),
		"session_id":   fmt.Sprintf("sess_%d", docID%10000),
		"turn_count":   float64(docID % 50),
		"active":       docID%2 == 0,
	}

	vec := make([]float32, p.vectorDims)
	for i := range vec {
		vec[i] = float32(docID)*0.001 + float32(i)*0.0001
	}

	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(id.String()),
			Class:              "ConversationMemory",
			Properties:         props,
			CreationTimeUnix:   ts,
			LastUpdateTimeUnix: ts,
		},
		Vector: vec,
		DocID:  docID,
	}
}

type benchFixture struct {
	objectsBucket  *Bucket
	columnarBucket *Bucket
	docIDs         []uint64
	timestamps     []int64
}

func setupBenchFixture(b *testing.B, p objectProfile) *benchFixture {
	b.Helper()

	objDir, err := os.MkdirTemp("", "bench-obj-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(objDir) })

	colDir, err := os.MkdirTemp("", "bench-col-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(colDir) })

	logger, _ := test.NewNullLogger()

	objBucket, err := NewBucketCreator().NewBucket(context.Background(), objDir, objDir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(b, err)
	b.Cleanup(func() { objBucket.Shutdown(context.Background()) })

	colSchema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "updated", Type: columnar.ColumnTypeInt64},
		},
	}
	colBucket, err := NewBucketCreator().NewBucket(context.Background(), colDir, colDir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(colSchema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(b, err)
	b.Cleanup(func() { colBucket.Shutdown(context.Background()) })

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	rng := rand.New(rand.NewSource(12345))
	timestamps := make([]int64, benchObjCount)

	for i := uint64(0); i < benchObjCount; i++ {
		ts := baseTime + int64(rng.Intn(365*24))*int64(time.Hour)
		timestamps[i] = ts

		obj := makeTestObject(p, i, ts)
		data, err := obj.MarshalBinary()
		require.NoError(b, err)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		require.NoError(b, objBucket.Put(key, data))
		require.NoError(b, colBucket.ColumnarPutInt64(i, 0, ts))
	}

	require.NoError(b, objBucket.FlushMemtable())
	require.NoError(b, colBucket.FlushMemtable())

	docIDs := make([]uint64, benchReadCount)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(benchObjCount))
	}

	return &benchFixture{
		objectsBucket:  objBucket,
		columnarBucket: colBucket,
		docIDs:         docIDs,
		timestamps:     timestamps,
	}
}

// ---------- benchmark helpers ----------

func benchObjectsSingle(b *testing.B, f *benchFixture) {
	propertyPaths := [][]string{{"updated"}}
	resultProps := make(map[string]interface{}, 1)
	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := f.docIDs[i%benchReadCount]
		binary.BigEndian.PutUint64(key, docID)

		data, err := f.objectsBucket.Get(key)
		if err != nil {
			b.Fatalf("docID %d: Get: %v", docID, err)
		}
		if len(data) == 0 {
			b.Fatalf("docID %d: empty data", docID)
		}
		if err := storobj.UnmarshalPropertiesFromObject(data, resultProps, propertyPaths); err != nil {
			b.Fatal(err)
		}
		_ = resultProps["updated"]
	}
}

func benchColumnarSingle(b *testing.B, f *benchFixture) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := f.docIDs[i%benchReadCount]
		val, ok := f.columnarBucket.ColumnarLookupInt64(docID, 0)
		if !ok {
			b.Fatalf("docID %d not found", docID)
		}
		_ = val
	}
}

func benchObjectsBatch(b *testing.B, f *benchFixture, batchSize int) {
	rng := rand.New(rand.NewSource(88))
	pool := make([]uint64, 50_000)
	for i := range pool {
		pool[i] = uint64(rng.Intn(benchObjCount))
	}

	propertyPaths := [][]string{{"updated"}}
	resultProps := make(map[string]interface{}, 1)
	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		off := (i * batchSize) % (len(pool) - batchSize)
		candidates := pool[off : off+batchSize]

		var sum int64
		for _, docID := range candidates {
			binary.BigEndian.PutUint64(key, docID)
			data, err := f.objectsBucket.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			if err := storobj.UnmarshalPropertiesFromObject(data, resultProps, propertyPaths); err != nil {
				b.Fatal(err)
			}
			if v, ok := resultProps["updated"]; ok {
				if ts, ok := v.(float64); ok {
					sum += int64(ts)
				}
			}
		}
		_ = sum
	}
}

func benchColumnarBatch(b *testing.B, f *benchFixture, batchSize int) {
	rng := rand.New(rand.NewSource(88))
	pool := make([]uint64, 50_000)
	for i := range pool {
		pool[i] = uint64(rng.Intn(benchObjCount))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		off := (i * batchSize) % (len(pool) - batchSize)
		candidates := pool[off : off+batchSize]

		var sum int64
		for _, docID := range candidates {
			if val, ok := f.columnarBucket.ColumnarLookupInt64(docID, 0); ok {
				sum += val
			}
		}
		_ = sum
	}
}

// ---------- Small profile (128-dim, ~200B text) ----------

func BenchmarkSmall_Objects_Single(b *testing.B) {
	benchObjectsSingle(b, setupBenchFixture(b, profileSmall))
}

func BenchmarkSmall_Columnar_Single(b *testing.B) {
	benchColumnarSingle(b, setupBenchFixture(b, profileSmall))
}

func BenchmarkSmall_Objects_Batch100(b *testing.B) {
	benchObjectsBatch(b, setupBenchFixture(b, profileSmall), 100)
}

func BenchmarkSmall_Columnar_Batch100(b *testing.B) {
	benchColumnarBatch(b, setupBenchFixture(b, profileSmall), 100)
}

func BenchmarkSmall_Objects_Batch1000(b *testing.B) {
	benchObjectsBatch(b, setupBenchFixture(b, profileSmall), 1000)
}

func BenchmarkSmall_Columnar_Batch1000(b *testing.B) {
	benchColumnarBatch(b, setupBenchFixture(b, profileSmall), 1000)
}

// ---------- Realistic profile (1536-dim, ~4KB text) ----------

func BenchmarkRealistic_Objects_Single(b *testing.B) {
	benchObjectsSingle(b, setupBenchFixture(b, profileRealistic))
}

func BenchmarkRealistic_Columnar_Single(b *testing.B) {
	benchColumnarSingle(b, setupBenchFixture(b, profileRealistic))
}

func BenchmarkRealistic_Objects_Batch100(b *testing.B) {
	benchObjectsBatch(b, setupBenchFixture(b, profileRealistic), 100)
}

func BenchmarkRealistic_Columnar_Batch100(b *testing.B) {
	benchColumnarBatch(b, setupBenchFixture(b, profileRealistic), 100)
}

func BenchmarkRealistic_Objects_Batch1000(b *testing.B) {
	benchObjectsBatch(b, setupBenchFixture(b, profileRealistic), 1000)
}

func BenchmarkRealistic_Columnar_Batch1000(b *testing.B) {
	benchColumnarBatch(b, setupBenchFixture(b, profileRealistic), 1000)
}
