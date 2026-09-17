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

package integrationslowtest

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	hnswindex "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const vectorIndexQueueClassName = "TestClass"

// vectorIndexQueueCase is one vector index layout the repair and fill-queue
// journeys run against. targetVector is empty for the legacy vector.
type vectorIndexQueueCase struct {
	name         string
	targetVector string
	multiVector  bool
	class        *models.Class
}

func vectorIndexQueueCases() []vectorIndexQueueCase {
	return []vectorIndexQueueCase{
		{
			name:  "hnsw",
			class: &models.Class{Class: vectorIndexQueueClassName, InvertedIndexConfig: invertedConfig(), VectorIndexConfig: hnsw.UserConfig{}},
		},
		{
			name:         "hnsw with target vectors",
			targetVector: "foo",
			class: &models.Class{
				Class:               vectorIndexQueueClassName,
				InvertedIndexConfig: invertedConfig(),
				VectorIndexConfig:   hnsw.UserConfig{},
				VectorConfig: map[string]models.VectorConfig{
					"foo": {VectorIndexConfig: hnsw.UserConfig{}},
				},
			},
		},
		{
			name:         "hnsw with multi vectors",
			targetVector: "foo",
			multiVector:  true,
			class: &models.Class{
				Class:               vectorIndexQueueClassName,
				InvertedIndexConfig: invertedConfig(),
				VectorIndexConfig:   hnsw.UserConfig{},
				VectorConfig: map[string]models.VectorConfig{
					"foo": {VectorIndexConfig: hnsw.UserConfig{
						Multivector: hnsw.MultivectorConfig{Enabled: true},
					}},
				},
			},
		},
		{
			name:  "flat",
			class: &models.Class{Class: vectorIndexQueueClassName, InvertedIndexConfig: invertedConfig(), VectorIndexConfig: flat.NewDefaultUserConfig()},
		},
	}
}

// seedVectorIndexQueueShard opens an async-indexing shard for the case, writes
// amount objects with doc ids 0..amount-1 and waits for the queue to drain.
func seedVectorIndexQueueShard(t *testing.T, tc vectorIndexQueueCase, amount int) (db.ShardLike, db.VectorIndex, *db.VectorIndexQueue) {
	t.Helper()
	ctx := context.Background()

	repo, _ := newRepo(t, repoParams{config: func(c *db.Config) {
		c.AsyncIndexingEnabled = true
	}}, tc.class)
	shd := singleShard(t, repo, tc.class.Class)

	objs := make([]*storobj.Object, 0, amount)
	for i := 0; i < amount; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: tc.class.Class,
			},
		}
		switch {
		case tc.multiVector:
			obj.MultiVectors = map[string][][]float32{tc.targetVector: {{1, 2, 3}, {4, 5, 6}}}
		case tc.targetVector != "":
			obj.Vectors = map[string][]float32{tc.targetVector: {1, 2, 3}}
		default:
			obj.Vector = randVector(3)
		}
		objs = append(objs, obj)
	}
	for _, err := range shd.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}

	vidx, releaseIdx, vok := shd.AcquireVectorIndex(tc.targetVector)
	if vok {
		t.Cleanup(releaseIdx)
	}
	q, releaseQ, qok := shd.AcquireVectorIndexQueue(tc.targetVector)
	if qok {
		t.Cleanup(releaseQ)
	}
	require.True(t, vok && qok)

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Zero(t, q.Size())
	}, 10*time.Second, 100*time.Millisecond)

	return shd, vidx, q
}

func deleteFromVectorIndex(t *testing.T, tc vectorIndexQueueCase, vidx db.VectorIndex, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		if tc.multiVector {
			require.NoError(t, vidx.(db.VectorIndexMulti).DeleteMulti(uint64(i)))
		} else {
			require.NoError(t, vidx.Delete(uint64(i)))
		}
	}
}

func TestShard_RepairIndex(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	for _, tc := range vectorIndexQueueCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const amount = 1000
			shd, vidx, q := seedVectorIndexQueueShard(t, tc, amount)

			// Docs 400..599 are missing from the vector index only; the repair
			// must put them back.
			deleteFromVectorIndex(t, tc, vidx, 400, 600)

			// Docs 100..299 are deleted from the object store only; the repair
			// must take them out of the vector index.
			bucket := shd.Store().Bucket(helpers.ObjectsBucketLSM)
			buf := make([]byte, 8)
			for i := 100; i < 300; i++ {
				binary.LittleEndian.PutUint64(buf, uint64(i))
				v, err := bucket.GetBySecondary(ctx, 0, buf)
				require.NoError(t, err)
				obj, err := storobj.FromBinaryDisk(v, tc.class.Class)
				require.NoError(t, err)
				idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
				require.NoError(t, err)
				require.NoError(t, bucket.Delete(idBytes, lsmkv.WithSecondaryKey(0, buf)))
			}

			require.NoError(t, shd.RepairIndex(ctx, tc.targetVector))

			require.EventuallyWithT(t, func(t *assert.CollectT) {
				assert.Zero(t, q.Size())
			}, 10*time.Second, 100*time.Millisecond)
			time.Sleep(500 * time.Millisecond)

			for i := 0; i < amount; i++ {
				if i >= 100 && i < 300 {
					require.Falsef(t, vidx.ContainsDoc(uint64(i)), "doc %d should not be in the vector index", i)
					continue
				}
				require.Truef(t, vidx.ContainsDoc(uint64(i)), "doc %d should be in the vector index", i)
			}
		})
	}
}

func TestShard_FillQueue(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	for _, tc := range vectorIndexQueueCases() {
		t.Run(tc.name, func(t *testing.T) {
			const amount = 1000
			shd, vidx, q := seedVectorIndexQueueShard(t, tc, amount)

			deleteFromVectorIndex(t, tc, vidx, 100, amount)

			if hnswindex.IsHNSWIndex(vidx) {
				require.NoError(t, hnswindex.AsHNSWIndex(vidx).CleanUpTombstonedNodes(func() bool { return false }))
			}

			require.NoError(t, shd.FillQueue(tc.targetVector, 150))

			require.EventuallyWithT(t, func(t *assert.CollectT) {
				assert.Zero(t, q.Size())
			}, 5*time.Second, 100*time.Millisecond)
			time.Sleep(500 * time.Millisecond)

			for i := 0; i < amount; i++ {
				if 100 <= i && i < 150 {
					require.Falsef(t, vidx.ContainsDoc(uint64(i)), "doc %d should not be in the vector index", i)
					continue
				}
				require.Truef(t, vidx.ContainsDoc(uint64(i)), "doc %d should be in the vector index", i)
			}
		})
	}
}
