//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

	thresh = 4096
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

func TestUpdateBMWIndex(t *testing.T) {
	/*
		dirName := t.TempDir()
		logger, _ := test.NewNullLogger()
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
				UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
			},
		}
		fakeSchema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					class,
				},
			},
		}
		// create index with data
		shardState := singleShardState()
		scheduler := queue.NewScheduler(queue.SchedulerOptions{
			Logger:  logger,
			Workers: 1,
		})

		index, err := NewIndex(testCtx(), IndexConfig{
			RootPath:          dirName,
			ClassName:         schema.ClassName(class.Class),
			ReplicationFactor: 1,
			ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
			hnsw.NewDefaultUserConfig(), nil, nil, &fakeSchemaGetter{
				schema: fakeSchema, shardState: shardState,
			}, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil,
			NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
		require.Nil(t, err)

		productsIds := []strfmt.UUID{
			"1295c052-263d-4aae-99dd-920c5a370d06",
			"1295c052-263d-4aae-99dd-920c5a370d07",
		}

		products := []map[string]interface{}{
			{"name": "one two"},
			{"name": "two three"},
		}

		err = index.addProperty(context.TODO(), &models.Property{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		})
		require.Nil(t, err)

		for i, p := range products {
			product := models.Object{
				Class:      class.Class,
				ID:         productsIds[i],
				Properties: p,
			}

			err := index.putObject(context.TODO(), storobj.FromObject(
				&product, []float32{0.1, 0.2, 0.01, 0.2}, nil, nil), nil, 0)
			require.Nil(t, err)
		}

		indexFilesBeforeDelete, err := getIndexFilenames(dirName, class.Class)
		require.Nil(t, err)

		beforeDeleteObj1, err := index.objectByID(context.TODO(),
			productsIds[0], nil, additional.Properties{}, nil, "")
		require.Nil(t, err)

		beforeDeleteObj2, err := index.objectByID(context.TODO(),
			productsIds[1], nil, additional.Properties{}, nil, "")
		require.Nil(t, err)


			// drop the index
			err = index.drop()
			require.Nil(t, err)

			indexFilesAfterDelete, err := getIndexFilenames(dirName, class.Class)
			require.Nil(t, err)

			// recreate the index
			index, err = NewIndex(testCtx(), IndexConfig{
				RootPath:          dirName,
				ClassName:         schema.ClassName(class.Class),
				ReplicationFactor: 1,
				ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
			}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, nil, &fakeSchemaGetter{
					schema:     fakeSchema,
					shardState: shardState,
				}, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil,
				NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
			require.Nil(t, err)

			err = index.addProperty(context.TODO(), &models.Property{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			})
			require.Nil(t, err)

			indexFilesAfterRecreate, err := getIndexFilenames(dirName, class.Class)
			require.Nil(t, err)

			afterRecreateObj1, err := index.objectByID(context.TODO(),
				productsIds[0], nil, additional.Properties{}, nil, "")
			require.Nil(t, err)

			afterRecreateObj2, err := index.objectByID(context.TODO(),
				productsIds[1], nil, additional.Properties{}, nil, "")
			require.Nil(t, err)

			// insert some data in the recreated index
			for i, p := range products {
				thing := models.Object{
					Class:      class.Class,
					ID:         productsIds[i],
					Properties: p,
				}

				err := index.putObject(context.TODO(), storobj.FromObject(
					&thing, []float32{0.1, 0.2, 0.01, 0.2}, nil, nil), nil, 0)
				require.Nil(t, err)
			}

			afterRecreateAndInsertObj1, err := index.objectByID(context.TODO(),
				productsIds[0], nil, additional.Properties{}, nil, "")
			require.Nil(t, err)

			afterRecreateAndInsertObj2, err := index.objectByID(context.TODO(),
				productsIds[1], nil, additional.Properties{}, nil, "")
			require.Nil(t, err)

			// update the index vectorIndexUserConfig
			beforeVectorConfig, ok := index.GetVectorIndexConfig("").(hnsw.UserConfig)
			require.Equal(t, -1, beforeVectorConfig.EF)
			require.True(t, ok)
			beforeVectorConfig.EF = 99
			err = index.updateVectorIndexConfig(context.TODO(), beforeVectorConfig)
			require.Nil(t, err)
			afterVectorConfig, ok := index.GetVectorIndexConfig("").(hnsw.UserConfig)
			require.True(t, ok)
			require.Equal(t, 99, afterVectorConfig.EF)

			assert.Equal(t, 6, len(indexFilesBeforeDelete))
			assert.Equal(t, 0, len(indexFilesAfterDelete))
			assert.Equal(t, 6, len(indexFilesAfterRecreate))
			assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
			assert.NotNil(t, beforeDeleteObj1)
			assert.NotNil(t, beforeDeleteObj2)
			assert.Empty(t, afterRecreateObj1)
			assert.Empty(t, afterRecreateObj2)
			assert.NotNil(t, afterRecreateAndInsertObj1)
			assert.NotNil(t, afterRecreateAndInsertObj2)
	*/
}
