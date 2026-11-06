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
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func reversed(vec []float32) []float32 {
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[len(vec)-1-i] = v
	}
	return out
}

// hnswCommitLogBytesOnDisk sums the size of every file in every HNSW commit
// log directory under root. Only bytes that reached the file through write()
// are counted: this is exactly what survives a process crash (kill -9), since
// the page cache outlives the process but a user-space bufio buffer does not.
func hnswCommitLogBytesOnDisk(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(filepath.Dir(path), ".hnsw.commitlog.d") {
			total += info.Size()
		}
		return nil
	})
	require.NoError(t, err)
	return total
}

// TestPutObjectBatch_HNSWCommitLogFlushedBeforeAck pins that once a write to a
// shard with synchronous indexing (ASYNC_INDEXING=false) returns, its HNSW
// commit log entries are no longer held only in memory. Otherwise a process
// crash right after the acknowledgement loses the vectors of the last batch:
// the objects come back (their LSM WAL was flushed), but the vector index has
// no node for them, and with synchronous indexing nothing repairs it at
// start-up (Shard.RepairIndex is a no-op), so vector search silently skips
// them while flat search over an allow list skips the nil nodes too.
//
// The invariant is checked by flushing the commit log once more after the
// write returned: if the write path already flushed everything it wrote, the
// extra flush has nothing left to write.
func TestPutObjectBatch_HNSWCommitLogFlushedBeforeAck(t *testing.T) {
	tests := []struct {
		name  string
		index flushTestIndex
		// batches imported back to back; the flush is checked after the last
		batches   int
		batchSize int
		write     flushTestWrite
	}{
		// smaller than the commit log's 32 KiB buffer: nothing of the batch
		// reaches the file, so after a crash none of its nodes exist
		{name: "batch, one object", batches: 1, batchSize: 1, write: flushTestPutBatch},
		{name: "batch, below buffer size", batches: 1, batchSize: 50, write: flushTestPutBatch},
		// larger than the buffer: the buffer spills mid-batch, so a crash leaves
		// some nodes of the batch with only part of their links (unreachable)
		{name: "batch, above buffer size", batches: 1, batchSize: 2000, write: flushTestPutBatch},
		// an earlier batch is flushed by the next one, only the last one is lost
		{name: "several batches, last one", batches: 4, batchSize: 200, write: flushTestPutBatch},
		{name: "named vector batch", index: flushTestNamedVector, batches: 1, batchSize: 80, write: flushTestPutBatch},
		{name: "geo property batch", index: flushTestGeoProperty, batches: 1, batchSize: 80, write: flushTestPutBatch},
		// single-object put already flushes after inserting the vector
		{name: "single object put", batches: 1, batchSize: 20, write: flushTestPutOneByOne},
		{name: "named vector single object put", index: flushTestNamedVector, batches: 1, batchSize: 20, write: flushTestPutOneByOne},
		{name: "geo property single object put", index: flushTestGeoProperty, batches: 1, batchSize: 20, write: flushTestPutOneByOne},
		{name: "merge", batches: 1, batchSize: 20, write: flushTestPutThenMerge},
		{name: "named vector merge", index: flushTestNamedVector, batches: 1, batchSize: 20, write: flushTestPutThenMerge},
		{name: "geo property merge", index: flushTestGeoProperty, batches: 1, batchSize: 20, write: flushTestPutThenMerge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			shd, idx := newFlushTestShard(t, ctx, tc.index)
			defer func() { require.NoError(t, idx.drop()) }()

			r := getRandomSeed()
			for b := 0; b < tc.batches; b++ {
				tc.write(t, ctx, shd, newFlushTestObjects(r, tc.index, tc.batchSize))
			}

			// the write was acknowledged: this is what a crash now would keep
			acknowledged := hnswCommitLogBytesOnDisk(t, idx.Config.RootPath)

			flush := func(_ string, q *VectorIndexQueue) error { return q.Flush() }
			require.NoError(t, shd.ForEachVectorQueue(flush))
			require.NoError(t, shd.ForEachGeoQueue(flush))
			flushed := hnswCommitLogBytesOnDisk(t, idx.Config.RootPath)

			require.NotZero(t, flushed, "the import wrote no HNSW commit log at all")
			require.Equal(t, flushed, acknowledged,
				"%d bytes of HNSW commit log were still buffered in memory after the write returned; "+
					"a crash now would lose those vectors", flushed-acknowledged)
		})
	}
}

// flushTestIndex is the HNSW index the objects of a flush test row go into.
type flushTestIndex int

const (
	// the legacy unnamed vector
	flushTestLegacyVector flushTestIndex = iota
	// the named vector "title"
	flushTestNamedVector
	// no vector index, only the geo property "location" (its index is an HNSW too)
	flushTestGeoProperty
)

const flushTestClassName = "TestClass"

func newFlushTestShard(t *testing.T, ctx context.Context, index flushTestIndex) (ShardLike, *Index) {
	uc := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	uc.SetDefaults()

	switch index {
	case flushTestGeoProperty:
		class := &models.Class{
			Class: flushTestClassName,
			Properties: []*models.Property{
				{Name: "location", DataType: []string{string(schema.DataTypeGeoCoordinates)}},
			},
		}
		return testShardWithSettings(t, ctx, class, hnsw.UserConfig{Skip: true}, false, false)
	case flushTestNamedVector:
		return testShardWithSettings(t, ctx, &models.Class{Class: flushTestClassName}, nil, false, false,
			func(i *Index) {
				i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": uc}
			})
	default:
		return testShardWithSettings(t, ctx, &models.Class{Class: flushTestClassName}, uc, false, false)
	}
}

func newFlushTestObjects(r *rand.Rand, index flushTestIndex, n int) []*storobj.Object {
	objs := createRandomObjects(r, flushTestClassName, n, 64)
	for _, obj := range objs {
		switch index {
		case flushTestGeoProperty:
			obj.Vector = nil
			obj.Object.Properties = map[string]interface{}{"location": &models.GeoCoordinates{
				Latitude: ptFloat32(r.Float32()*180 - 90), Longitude: ptFloat32(r.Float32()*360 - 180),
			}}
		case flushTestNamedVector:
			obj.Vectors = map[string][]float32{"title": obj.Vector}
			obj.Vector = nil
		case flushTestLegacyVector:
		}
	}
	return objs
}

type flushTestWrite func(t *testing.T, ctx context.Context, shd ShardLike, objs []*storobj.Object)

func flushTestPutBatch(t *testing.T, ctx context.Context, shd ShardLike, objs []*storobj.Object) {
	for _, err := range shd.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}
}

func flushTestPutOneByOne(t *testing.T, ctx context.Context, shd ShardLike, objs []*storobj.Object) {
	for _, obj := range objs {
		require.NoError(t, shd.PutObject(ctx, obj))
	}
}

// flushTestPutThenMerge puts every object (already flushed, see above), then
// patches it with a changed vector or geo property, so the merge writes the
// indices again
func flushTestPutThenMerge(t *testing.T, ctx context.Context, shd ShardLike, objs []*storobj.Object) {
	flushTestPutOneByOne(t, ctx, shd, objs)
	for _, obj := range objs {
		require.NoError(t, shd.MergeObject(ctx, flushTestMergeDocument(obj)))
	}
}

func flushTestMergeDocument(obj *storobj.Object) objects.MergeDocument {
	merge := objects.MergeDocument{Class: obj.Class().String(), ID: obj.ID(), UpdateTime: time.Now().UnixMilli()}
	props, _ := obj.Properties().(map[string]interface{})
	if loc, ok := props["location"].(*models.GeoCoordinates); ok {
		merge.PrimitiveSchema = map[string]interface{}{"location": &models.GeoCoordinates{
			Latitude: ptFloat32(-*loc.Latitude), Longitude: ptFloat32(-*loc.Longitude),
		}}
	}
	if obj.Vector != nil {
		merge.Vector = reversed(obj.Vector)
	}
	for name, vec := range obj.Vectors {
		merge.Vectors = models.Vectors{name: reversed(vec)}
	}
	return merge
}
