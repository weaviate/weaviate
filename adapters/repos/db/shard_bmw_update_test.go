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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestUpdateDocBMWIndex(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "bmw_test",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: true,
		},
	}
	createShard := func(t *testing.T) (ShardLike, *VectorIndexQueue) {
		vectorIndexConfig := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
		shard, _ := testShardWithSettings(t, ctx, class, vectorIndexConfig, true, true)
		queue, ok := shard.GetVectorIndexQueue("")
		require.True(t, ok)
		return shard, queue
	}
	search := func(t *testing.T, shard ShardLike, filter *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, props []string) []*storobj.Object {
		searchLimit := 10
		found, _, err := shard.ObjectSearch(ctx, searchLimit, filter,
			keywordRanking, nil, nil, additional.Properties{}, props)
		require.NoError(t, err)
		return found
	}

	uuid_ := strfmt.UUID(uuid.NewString())
	origCreateTimeUnix := int64(1704161045)
	origUpdateTimeUnix := int64(1704161046)
	updUpdateTimeUnix := int64(1704161048)
	updUpdateTimeUnix2 := int64(1704161049)
	vector := []float32{1, 2, 3}

	createOrigObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    uuid_,
				Class: class.Class,
				Properties: map[string]interface{}{
					"name": "word1 word2",
				},
				CreationTimeUnix:   origCreateTimeUnix,
				LastUpdateTimeUnix: origUpdateTimeUnix,
			},
			Vector: vector,
		}
	}
	createMergeDoc := func() objects.MergeDocument {
		return objects.MergeDocument{
			ID:    uuid_,
			Class: class.Class,
			PrimitiveSchema: map[string]interface{}{
				"name": "word2 word3",
			},
			UpdateTime: updUpdateTimeUnix,
			Vector:     vector,
		}
	}
	createMergeDoc2 := func() objects.MergeDocument {
		return objects.MergeDocument{
			ID:    uuid_,
			Class: class.Class,
			PrimitiveSchema: map[string]interface{}{
				"name": "word3 word4",
			},
			UpdateTime: updUpdateTimeUnix2,
			Vector:     vector,
		}
	}
	shard, queue := createShard(t)

	store := shard.Store()
	require.NotNil(t, store)

	err := shard.PutObject(ctx, createOrigObj())
	require.NoError(t, err)

	store = shard.Store()
	require.NotNil(t, store)

	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)

	thresh := 0
	opts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyInverted),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		lsmkv.WithMinWalThreshold(int64(thresh)),
	}
	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)

	store = shard.Store()
	require.NotNil(t, store)

	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)

	thresh = 4096
	opts = []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyInverted),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		lsmkv.WithMinWalThreshold(int64(thresh)),
	}
	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)

	mergeDoc := createMergeDoc()

	err = shard.MergeObject(ctx, mergeDoc)
	require.NoError(t, err)

	store = shard.Store()
	require.NotNil(t, store)

	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)

	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)

	queue.Scheduler().Schedule(context.Background())
	time.Sleep(50 * time.Millisecond)
	queue.Wait()

	expectedNextDocID := uint64(1)
	require.Equal(t, expectedNextDocID, shard.Counter().Get())
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word2"}
	found := search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word3"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word1"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)

	mergeDoc2 := createMergeDoc2()
	err = shard.MergeObject(ctx, mergeDoc2)
	require.NoError(t, err)

	bucket2 := store.Bucket("property_name_searchable")
	require.NotNil(t, bucket2)

	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word2"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word1"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word3"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word4"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)

	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)

	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)

	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word2"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word1"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word3"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word4"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)

	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)

	thresh = 0
	opts = []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyInverted),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		lsmkv.WithMinWalThreshold(int64(thresh)),
	}
	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)
	err = store.ShutdownBucket(ctx, "property_name_searchable")
	require.NoError(t, err)
	err = store.CreateOrLoadBucket(ctx, "property_name_searchable", opts...)
	require.NoError(t, err)

	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word2"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word1"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 0)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word3"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"name"}, Query: "word4"}
	found = search(t, shard, nil, kwr, []string{"name"})
	require.Len(t, found, 1)
}
