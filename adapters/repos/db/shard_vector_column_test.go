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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func vectorColumnTestClass(className string) *models.Class {
	return &models.Class{
		Class:               className,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
}

func waitForVectorColumnReady(t *testing.T, s *Shard, targetVector string) {
	t.Helper()
	require.Eventually(t, func() bool {
		st := s.peekVectorColumnState(targetVector)
		return st != nil && st.ready.Load()
	}, 30*time.Second, 10*time.Millisecond, "vector column backfill did not complete")
}

func vectorColumnPayload(t *testing.T, s *Shard, targetVector string, docID uint64) ([]float32, bool) {
	t.Helper()
	bucket := s.store.Bucket(helpers.GetVectorColumnBucketName(targetVector))
	if bucket == nil {
		return nil, false
	}
	payload, ok, err := bucket.ColumnarGetVectorPayload(docID, nil)
	if err != nil || !ok {
		return nil, false
	}
	return lsmkv.BytesToFloat32s(payload, nil), true
}

// TestVectorColumn_WriteLifecycle covers the synchronous write hooks: puts
// feed the column, docID-preserving updates keep the entry, docID-changing
// updates move it, deletes tombstone it.
func TestVectorColumn_WriteLifecycle(t *testing.T) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 8

	cfg := enthnsw.NewDefaultUserConfig()
	cfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(t, ctx, vectorColumnTestClass("VectorColumnLifecycle"), cfg, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	// a fresh shard's backfill trivially completes with zero objects
	waitForVectorColumnReady(t, concrete, "")

	objs := createRandomObjects(r, "VectorColumnLifecycle", 10, dims)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	t.Run("puts feed the column synchronously", func(t *testing.T) {
		for _, obj := range objs {
			vec, ok := vectorColumnPayload(t, concrete, "", obj.DocID)
			require.True(t, ok, "doc %d missing from column", obj.DocID)
			assert.Equal(t, obj.Vector, vec)
		}
	})

	t.Run("docID-preserving update keeps the entry", func(t *testing.T) {
		obj := objs[0]
		prevDocID := obj.DocID

		updated := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         obj.ID(),
				Class:      obj.Object.Class,
				Properties: map[string]interface{}{"description": "changed"},
			},
			Vector: obj.Vector, // unchanged vector → docID preserved
		}
		require.NoError(t, shard.PutObject(ctx, updated))
		require.Equal(t, prevDocID, updated.DocID, "expected docID-preserving update")

		vec, ok := vectorColumnPayload(t, concrete, "", prevDocID)
		require.True(t, ok)
		assert.Equal(t, obj.Vector, vec)
	})

	t.Run("docID-changing update moves the entry", func(t *testing.T) {
		obj := objs[1]
		prevDocID := obj.DocID
		newVec := randomVector(r, dims)

		updated := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    obj.ID(),
				Class: obj.Object.Class,
			},
			Vector: newVec,
		}
		require.NoError(t, shard.PutObject(ctx, updated))
		require.NotEqual(t, prevDocID, updated.DocID, "expected docID-changing update")

		_, ok := vectorColumnPayload(t, concrete, "", prevDocID)
		assert.False(t, ok, "old docID should be tombstoned in the column")

		vec, ok := vectorColumnPayload(t, concrete, "", updated.DocID)
		require.True(t, ok)
		assert.Equal(t, newVec, vec)
	})

	t.Run("delete tombstones the entry", func(t *testing.T) {
		obj := objs[2]
		require.NoError(t, shard.DeleteObject(ctx, obj.ID(), time.Time{}))

		_, ok := vectorColumnPayload(t, concrete, "", obj.DocID)
		assert.False(t, ok, "deleted docID should be tombstoned in the column")
	})

	t.Run("batch delete tombstones the entry", func(t *testing.T) {
		obj := objs[3]
		res := shard.DeleteObjectBatch(ctx, []strfmt.UUID{obj.ID()}, time.Time{}, false)
		for _, r := range res {
			require.NoError(t, r.Err)
		}

		_, ok := vectorColumnPayload(t, concrete, "", obj.DocID)
		assert.False(t, ok, "batch-deleted docID should be tombstoned in the column")
	})
}

// TestVectorColumn_BackfillOnRuntimeEnable covers flipping columnarRescore
// on a running shard that already holds objects: the config update starts
// the backfill, which converges and writes the sentinel.
func TestVectorColumn_BackfillOnRuntimeEnable(t *testing.T) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 8

	cfgOff := enthnsw.NewDefaultUserConfig() // columnarRescore off

	shard, _ := testShardWithSettings(t, ctx, vectorColumnTestClass("VectorColumnEnable"), cfgOff, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	objs := createRandomObjects(r, "VectorColumnEnable", 50, dims)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// flag off: no column state, no bucket
	require.Nil(t, concrete.peekVectorColumnState(""))
	require.Nil(t, concrete.store.Bucket(helpers.GetVectorColumnBucketName("")))

	cfgOn := cfgOff
	cfgOn.ColumnarRescore = true
	require.NoError(t, shard.UpdateVectorIndexConfig(ctx, cfgOn))

	waitForVectorColumnReady(t, concrete, "")

	for _, obj := range objs {
		vec, ok := vectorColumnPayload(t, concrete, "", obj.DocID)
		require.True(t, ok, "doc %d missing after backfill", obj.DocID)
		assert.Equal(t, obj.Vector, vec)
	}

	// sentinel must exist and record the dimensionality so a restart can
	// reload the bucket without rescanning the objects bucket
	data, err := os.ReadFile(concrete.vectorColumnSentinelPath(""))
	require.NoError(t, err)
	var sentinel vectorColumnSentinel
	require.NoError(t, json.Unmarshal(data, &sentinel))
	assert.Equal(t, dims, sentinel.Dims)
	assert.False(t, sentinel.Multi)

	// disabling stops serving immediately
	require.NoError(t, shard.UpdateVectorIndexConfig(ctx, cfgOff))
	assert.Nil(t, concrete.servableVectorColumnBucket(""))

	// re-enabling resumes serving without a rescan (sentinel short-circuit)
	require.NoError(t, shard.UpdateVectorIndexConfig(ctx, cfgOn))
	require.Eventually(t, func() bool {
		return concrete.servableVectorColumnBucket("") != nil
	}, 30*time.Second, 10*time.Millisecond)
}

// TestVectorColumn_BackfillOnRestart covers the restart pattern: a shard
// directory with objects but no column bucket is re-initialized with the
// flag on — the init-time backfill converges; a second restart reloads the
// bucket from the sentinel alone.
func TestVectorColumn_BackfillOnRestart(t *testing.T) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 8
	class := vectorColumnTestClass("VectorColumnRestart")

	cfgOff := enthnsw.NewDefaultUserConfig()

	shard, idx := testShardWithSettings(t, ctx, class, cfgOff, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)
	shardName := concrete.Name()

	objs := createRandomObjects(r, "VectorColumnRestart", 30, dims)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	require.NoError(t, shard.Shutdown(ctx))

	// "restart" with the flag enabled
	cfgOn := cfgOff
	cfgOn.ColumnarRescore = true
	idx.vectorIndexUserConfig = cfgOn

	shard2, err := idx.initShard(ctx, shardName, class, nil, false, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, shard2)
	concrete2, err := unwrapShard(ctx, shard2)
	require.NoError(t, err)

	waitForVectorColumnReady(t, concrete2, "")
	for _, obj := range objs {
		vec, ok := vectorColumnPayload(t, concrete2, "", obj.DocID)
		require.True(t, ok, "doc %d missing after restart backfill", obj.DocID)
		assert.Equal(t, obj.Vector, vec)
	}

	// second restart: the sentinel short-circuits the backfill and the
	// bucket reloads from disk with the schema recorded in the sentinel
	require.NoError(t, shard2.Shutdown(ctx))
	shard3, err := idx.initShard(ctx, shardName, class, nil, false, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, shard3)
	concrete3, err := unwrapShard(ctx, shard3)
	require.NoError(t, err)

	waitForVectorColumnReady(t, concrete3, "")
	require.Eventually(t, func() bool {
		return concrete3.servableVectorColumnBucket("") != nil
	}, 30*time.Second, 10*time.Millisecond)
	for _, obj := range objs {
		vec, ok := vectorColumnPayload(t, concrete3, "", obj.DocID)
		require.True(t, ok, "doc %d missing after sentinel reload", obj.DocID)
		assert.Equal(t, obj.Vector, vec)
	}
}

// TestVectorColumn_RescoreEquivalenceRQ8 pins that RQ-8 rescoring served
// from the vector column returns the same results as the objects-bucket
// path. Same shard, same index graph: only the rescore vector source flips
// between the two searches, so ids must match exactly and distances within
// float tolerance.
func TestVectorColumn_RescoreEquivalenceRQ8(t *testing.T) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 32
	n := 300
	k := 10

	cfg := enthnsw.NewDefaultUserConfig()
	cfg.RQ.Enabled = true
	cfg.RQ.Bits = 8
	cfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(t, ctx, vectorColumnTestClass("VectorColumnRQ8"), cfg, false, false, false)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)
	waitForVectorColumnReady(t, concrete, "")

	objs := createRandomObjects(r, "VectorColumnRQ8", n, dims)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// the column must actually be servable, otherwise this test would
	// trivially compare the fallback path against itself
	require.NotNil(t, concrete.servableVectorColumnBucket(""))
	for _, obj := range objs[:10] {
		_, ok := vectorColumnPayload(t, concrete, "", obj.DocID)
		require.True(t, ok)
	}

	vidx, ok := shard.GetVectorIndex("")
	require.True(t, ok)

	queries := make([][]float32, 5)
	for i := range queries {
		queries[i] = randomVector(r, dims)
	}

	search := func() ([][]uint64, [][]float32) {
		ids := make([][]uint64, len(queries))
		dists := make([][]float32, len(queries))
		for i, q := range queries {
			var err error
			ids[i], dists[i], err = vidx.SearchByVector(ctx, q, k, nil)
			require.NoError(t, err)
			require.Len(t, ids[i], k)
		}
		return ids, dists
	}

	columnIDs, columnDists := search()

	// flip the rescore source to the objects bucket (per-call enabled
	// check inside the injected thunks)
	concrete.applyVectorColumnConfig("", func() schemaConfig.VectorIndexConfig {
		off := cfg
		off.ColumnarRescore = false
		return off
	}())
	require.Nil(t, concrete.servableVectorColumnBucket(""))

	objectIDs, objectDists := search()

	for i := range queries {
		assert.Equal(t, objectIDs[i], columnIDs[i], "query %d: result ids differ", i)
		require.Len(t, columnDists[i], len(objectDists[i]))
		for j := range objectDists[i] {
			assert.InDelta(t, objectDists[i][j], columnDists[i][j], 1e-6,
				"query %d result %d: distance differs", i, j)
		}
	}
}

// TestVectorColumn_MultiVectorMuvera covers the multi-vector wiring: writes
// feed the multi-vector column for a named target vector, the column-backed
// readers return token matrices identical to the objects-bucket readers,
// and MUVERA search results are identical with the column on and off.
func TestVectorColumn_MultiVectorMuvera(t *testing.T) {
	r := getRandomSeed()
	ctx := context.Background()
	dims := 16
	n := 80
	k := 10
	target := "colbert"

	multiCfg := enthnsw.NewDefaultUserConfig()
	multiCfg.Multivector.Enabled = true
	multiCfg.Multivector.MuveraConfig.Enabled = true
	multiCfg.ColumnarRescore = true

	shard, _ := testShardWithSettings(t, ctx, vectorColumnTestClass("VectorColumnMuvera"),
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(idx *Index) {
			idx.vectorIndexUserConfigs[target] = multiCfg
		})
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)
	waitForVectorColumnReady(t, concrete, target)

	objs := make([]*storobj.Object, n)
	for i := range objs {
		tokens := 3 + r.Intn(4)
		matrix := make([][]float32, tokens)
		for j := range matrix {
			matrix[j] = randomVector(r, dims)
		}
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: "VectorColumnMuvera",
			},
			MultiVectors: map[string][][]float32{target: matrix},
		}
		require.NoError(t, shard.PutObject(ctx, objs[i]))
	}

	require.NotNil(t, concrete.servableVectorColumnBucket(target))

	t.Run("column readers match objects-bucket readers", func(t *testing.T) {
		for _, obj := range objs {
			container := &common.VectorSlice{Buff8: make([]byte, 8)}
			colVecs, err := concrete.readMultiVectorColumnIntoSlice(ctx, obj.DocID, container, target)
			require.NoError(t, err)

			container2 := &common.VectorSlice{Buff8: make([]byte, 8)}
			objVecs, err := concrete.readMultiVectorByIndexIDIntoSlice(ctx, obj.DocID, container2, target)
			require.NoError(t, err)

			require.Equal(t, objVecs, colVecs, "doc %d: token matrix differs", obj.DocID)
			require.Equal(t, obj.MultiVectors[target], colVecs)
		}
	})

	t.Run("muvera search equivalence column on vs off", func(t *testing.T) {
		vidx, ok := shard.GetVectorIndex(target)
		require.True(t, ok)
		multiIdx, ok := vidx.(VectorIndexMulti)
		require.True(t, ok)

		queries := make([][][]float32, 5)
		for i := range queries {
			queries[i] = make([][]float32, 4)
			for j := range queries[i] {
				queries[i][j] = randomVector(r, dims)
			}
		}

		search := func() ([][]uint64, [][]float32) {
			ids := make([][]uint64, len(queries))
			dists := make([][]float32, len(queries))
			for i, q := range queries {
				var err error
				ids[i], dists[i], err = multiIdx.SearchByMultiVector(ctx, q, k, nil)
				require.NoError(t, err)
				require.Len(t, ids[i], k)
			}
			return ids, dists
		}

		columnIDs, columnDists := search()

		off := multiCfg
		off.ColumnarRescore = false
		concrete.applyVectorColumnConfig(target, off)
		require.Nil(t, concrete.servableVectorColumnBucket(target))

		objectIDs, objectDists := search()

		for i := range queries {
			assert.Equal(t, objectIDs[i], columnIDs[i], "query %d: result ids differ", i)
			require.Len(t, columnDists[i], len(objectDists[i]))
			for j := range objectDists[i] {
				assert.InDelta(t, objectDists[i][j], columnDists[i][j], 1e-6,
					"query %d result %d: distance differs", i, j)
			}
		}
	})
}
