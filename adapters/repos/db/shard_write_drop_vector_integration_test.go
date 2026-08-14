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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
)

const dropVecClassName = "DropVectorWriteRejectClass"

// setupDropVectorShard builds a shard with named vectors "foo" (hnsw) and "mv"
// (multivector) plus a "label" property. The returned *models.Class is the live
// schema object the shard reads via getClass(), so markDropped simulates the
// drop marker applying before queue teardown has happened.
func setupDropVectorShard(t *testing.T, ctx context.Context) (*Shard, *models.Class) {
	t.Helper()
	class := &models.Class{
		Class: dropVecClassName,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "label",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"foo": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			"mv":  {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
		},
	}
	vic := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, true, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"foo": hnsw.NewDefaultUserConfig(),
			"mv":  hnsw.NewDefaultMultiVectorUserConfig(),
		}
	})

	switch s := shardLike.(type) {
	case *Shard:
		return s, class
	case *LazyLoadShard:
		require.NoError(t, s.Load(ctx))
		return s.shard, class
	default:
		t.Fatalf("unexpected shard type %T", shardLike)
		return nil, nil
	}
}

func dropVecObject(t *testing.T, label string, withFoo bool) *storobj.Object {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      dropVecClassName,
			Properties: map[string]interface{}{"label": label},
		},
	}
	if withFoo {
		obj.Vectors = map[string][]float32{"foo": {1, 2, 3}}
	}
	return obj
}

func dropVecMultiObject(t *testing.T, label string) *storobj.Object {
	t.Helper()
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      dropVecClassName,
			Properties: map[string]interface{}{"label": label},
		},
		MultiVectors: map[string][][]float32{"mv": {{1, 2}, {3, 4}}},
	}
}

func markDropped(class *models.Class, targetVector string) {
	class.VectorConfig[targetVector] = models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
}

// TestDropVectorIndex_PutRejected covers the put path (PutObject -> putOne),
// including the window where the marker is visible but the queue is still live.
func TestDropVectorIndex_PutRejected(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))

	// Drop the marker but leave the queue alive (the race window).
	markDropped(class, "foo")

	err := shard.PutObject(ctx, dropVecObject(t, "b", true))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector index not found")
	require.Contains(t, err.Error(), "foo")

	// A write not carrying the dropped vector still succeeds.
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "c", false)))
}

// TestDropVectorIndex_MergeRejected pins both halves of the merge contract: a
// client explicitly supplying the dropped vector is rejected, while a
// property-only merge on an object that still carries the dropped vector
// (carried over by mergeProps) succeeds — the carried-over vector is skipped,
// not errored. The latter is a regression guard for the pre-existing bug where
// such merges failed with "vector index not found" after a partial write.
func TestDropVectorIndex_MergeRejected(t *testing.T) {
	ctx := testCtx()

	t.Run("client-supplied dropped vector is rejected", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecObject(t, "a", true)
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "foo")

		err := shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			Vectors:         models.Vectors{"foo": []float32{4, 5, 6}},
			UpdateTime:      2_000,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "vector index not found")
		require.Contains(t, err.Error(), "foo")
	})

	t.Run("property-only merge carrying a dropped vector succeeds", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecObject(t, "a", true)
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "foo")
		// Tear the queue down too, for the real post-drop state: the stored
		// object still carries foo, but mergeProps copies it forward and the
		// re-index loop must skip it rather than error on the missing queue.
		require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

		require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			UpdateTime:      2_000,
		}))

		retrieved, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "b", retrieved.Object.Properties.(map[string]interface{})["label"])
	})

	t.Run("client-supplied dropped multivector is rejected", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecMultiObject(t, "a")
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "mv")

		err := shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			Vectors:         models.Vectors{"mv": [][]float32{{5, 6}, {7, 8}}},
			UpdateTime:      2_000,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "vector index not found")
		require.Contains(t, err.Error(), "mv")
	})

	t.Run("property-only merge carrying a dropped multivector succeeds", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecMultiObject(t, "a")
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "mv")
		require.NoError(t, shard.DropVectorIndex(ctx, "mv"))

		// mergeProps copies MultiVectors forward too; the re-index loop must skip
		// the carried-over dropped multivector instead of erroring.
		require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			UpdateTime:      2_000,
		}))

		retrieved, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "b", retrieved.Object.Properties.(map[string]interface{})["label"])
	})
}

// TestDropVectorIndex_BatchRejected covers the batch path
// (storeObjectOfBatchInLSM): only the item carrying the dropped vector fails.
func TestDropVectorIndex_BatchRejected(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "warmup", true)))

	markDropped(class, "foo")

	withFoo := dropVecObject(t, "withfoo", true)
	withoutFoo := dropVecObject(t, "withoutfoo", false)

	errs := shard.PutObjectBatch(ctx, []*storobj.Object{withFoo, withoutFoo})
	require.Len(t, errs, 2)
	require.Error(t, errs[0])
	require.Contains(t, errs[0].Error(), "vector index not found")
	require.Contains(t, errs[0].Error(), "foo")
	require.NoError(t, errs[1])
}
