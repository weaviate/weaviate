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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"

	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestVectorDistanceQuery(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()

	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
		DisableLazyLoadShards:     true, // need access to the shard directly to convert UUIDs to docIds
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)

	class := &models.Class{
		Class:               "Test",
		InvertedIndexConfig: invertedConfig(),
		VectorConfig: map[string]models.VectorConfig{
			"custom1": {VectorIndexConfig: hnsw.UserConfig{}},
			"custom2": {VectorIndexType: "hnsw", VectorIndexConfig: hnsw.UserConfig{}},
			"custom3": {VectorIndexType: "flat", VectorIndexConfig: flatent.UserConfig{}},
			//"custom4": {VectorIndexType: "dynamic", VectorIndexConfig: dynamicent.UserConfig{}},  // async only
		},
		Properties: []*models.Property{},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}},
		shardState: singleShardState(),
	}
	repo.SetSchemaGetter(schemaGetter)
	migrator := NewMigrator(repo, logger)

	require.Nil(t,
		migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	ids := make([]strfmt.UUID, 5)
	for i := range ids {
		uid := uuid.New()
		ids[i] = strfmt.UUID(uid.String())
	}

	vectors := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
	}
	index := repo.GetIndex(schema.ClassName(class.Class))

	var shards []ShardLike
	index.shards.Range(func(_ string, shard ShardLike) error {
		shards = append(shards, shard)
		return nil
	})

	t.Run("error cases", func(t *testing.T) {
		require.Nil(t, repo.PutObject(
			context.Background(),
			&models.Object{ID: ids[0], Class: class.Class},
			nil,
			map[string][]float32{"custom1": vectors[0], "custom2": vectors[1], "custom3": vectors[2]},
			nil,
			nil,
			0),
		)
		require.Nil(t, err)

		docId, err := docIdFromUUID(shards[0].(*Shard), ids[0])
		require.Nil(t, err)

		_, err = shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], vectors[2], vectors[3]},
			[]string{"custom1", "custom2"},
		)
		require.NotNil(t, err)

		_, err = shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{},
			[]string{},
		)
		require.NotNil(t, err)

		_, err = shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], vectors[2]},
			[]string{"custom1", "doesNotExist"})
		require.NotNil(t, err)

		_, err = shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], []float32{1, 0}},
			[]string{"custom1", "custom2"})
		require.NotNil(t, err)
	})

	t.Run("object with all vectors", func(t *testing.T) {
		require.Nil(t, repo.PutObject(
			context.Background(),
			&models.Object{ID: ids[1], Class: class.Class},
			nil,
			map[string][]float32{"custom1": vectors[0], "custom2": vectors[1], "custom3": vectors[2]},
			nil,
			nil,
			0),
		)
		docId, err := docIdFromUUID(shards[0].(*Shard), ids[1])
		require.Nil(t, err)

		distances, err := shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], vectors[2], vectors[3]},
			[]string{"custom1", "custom2", "custom3"})
		require.Nil(t, err)
		require.Len(t, distances, 3)
		require.Equal(t, float32(1), distances[0])
		require.Equal(t, float32(1), distances[1])
		require.Equal(t, float32(1), distances[2])
	})

	t.Run("Missing one vector", func(t *testing.T) {
		require.Nil(t, repo.PutObject(
			context.Background(),
			&models.Object{ID: ids[2], Class: class.Class},
			nil,
			map[string][]float32{"custom1": vectors[0], "custom2": vectors[1]},
			nil,
			nil,
			0),
		)

		docId, err := docIdFromUUID(shards[0].(*Shard), ids[2])
		require.Nil(t, err)

		// querying for existing target vectors works
		distances, err := shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], vectors[2]},

			[]string{"custom1", "custom2"})
		require.Nil(t, err)
		require.Len(t, distances, 2)
		require.Equal(t, float32(1), distances[0])
		require.Equal(t, float32(1), distances[1])

		// error for non-existing target vector
		_, err = shards[0].VectorDistanceForQuery(
			context.Background(),
			docId,
			[]models.Vector{vectors[1], vectors[2]},
			[]string{"custom1", "custom3"})
		require.NotNil(t, err)
	})
}

func docIdFromUUID(s *Shard, id strfmt.UUID) (uint64, error) {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return 0, err
	}
	docIdBytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).Get(idBytes)
	if err != nil {
		return 0, err
	}

	docId, err := storobj.DocIDFromBinary(docIdBytes)
	if err != nil {
		return 0, err
	}
	return docId, nil
}
