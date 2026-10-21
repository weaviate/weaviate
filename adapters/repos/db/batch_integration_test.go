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
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestBatchPutObjectsWithDimensions(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, "ThingForBatching", 123)

	dimAfter := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 369, dimAfter, "Dimensions are present after import")
}

func TestBatchPutObjects(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjects(repo))
	t.Run("batch import things with geo props", testBatchImportGeoObjects(repo))
}

func TestBatchPutObjectsWithNamedVectors(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	className := "NamedVectors"

	t.Run("create class", func(t *testing.T) {
		class := &models.Class{
			Class:               className,
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"bringYourOwnVector": {
					Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
					VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
					VectorIndexType:   "hnsw",
				},
				"colbert": {
					Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
					VectorIndexConfig: enthnsw.NewDefaultMultiVectorUserConfig(),
					VectorIndexType:   "hnsw",
				},
			},
		}

		require.Nil(t, migrator.AddClass(context.Background(), class))

		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{class},
		}
	})

	t.Run("batch import", func(t *testing.T) {
		batch := objects.BatchObjects{
			objects.BatchObject{
				OriginalIndex: 0,
				Err:           nil,
				Object: &models.Object{
					Class: className,
					Properties: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					Vectors: models.Vectors{
						"bringYourOwnVector": []float32{1, 2, 3},
						"colbert":            [][]float32{{0.5, 0.52, 0.53}, {0.511, 0.522, 0.533}, {0.5111, 0.5222, 0.5333}},
					},
				},
				UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
			},
			objects.BatchObject{
				OriginalIndex: 1,
				Err:           nil,
				Object: &models.Object{
					Class: className,
					Properties: map[string]interface{}{
						"stringProp": "second element",
					},
					ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					Vectors: models.Vectors{
						"bringYourOwnVector": []float32{1, 2, 3},
						"colbert":            [][]float32{{0.001, 0.002, 0.003}, {0.0011, 0.0022, 0.0033}, {0.00111, 0.00222, 0.00333}},
					},
				},
				UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
			},
			objects.BatchObject{
				OriginalIndex: 2,
				Err:           nil,
				Object: &models.Object{
					Class: className,
					Properties: map[string]interface{}{
						"stringProp": "third element",
					},
					ID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
					Vectors: models.Vectors{
						"bringYourOwnVector": []float32{1, 2, 3},
						"colbert":            [][]float32{{0.00000001, 0.00000002, 0.00000003}, {0.000000011, 0.000000022, 0.000000033}, {0.111, 0.222, 0.333}},
					},
				},
				UUID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
			},
		}

		t.Run("batch import", func(t *testing.T) {
			batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
			require.Nil(t, err)

			assert.Nil(t, batchRes[0].Err)
			assert.Nil(t, batchRes[1].Err)
			assert.Nil(t, batchRes[2].Err)
		})

		params := dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    nil,
		}
		res, err := repo.Search(context.Background(), params)
		require.Nil(t, err)

		t.Run("contains first element", func(t *testing.T) {
			item, ok := findID(res, batch[0].Object.ID)
			require.Equal(t, true, ok, "results should contain our desired id")
			assert.Equal(t, "first element", item.Schema.(map[string]interface{})["stringProp"])

			t.Run("contains named vector", func(t *testing.T) {
				require.Len(t, item.Vectors, 2)
				assert.IsType(t, []float32{}, item.Vectors["bringYourOwnVector"])
				assert.Equal(t, batch[0].Object.Vectors["bringYourOwnVector"], item.Vectors["bringYourOwnVector"])
			})
			t.Run("contains named multi vector", func(t *testing.T) {
				require.Len(t, item.Vectors, 2)
				assert.IsType(t, [][]float32{}, item.Vectors["colbert"])
				assert.Equal(t, batch[0].Object.Vectors["colbert"], item.Vectors["colbert"])
			})
		})

		t.Run("can be queried through the inverted index", func(t *testing.T) {
			filter := buildFilter("stringProp", "third", eq, schema.DataTypeText)
			params := dto.GetParams{
				ClassName:  className,
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    filter,
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)

			require.Len(t, res, 1)
			assert.Equal(t, strfmt.UUID("90ade18e-2b99-4903-aa34-1d5d648c932d"),
				res[0].ID)
		})

		// Vector search
		t.Run("can perform vector search using regular embeddings", func(t *testing.T) {
			res, err := repo.VectorSearch(context.Background(), dto.GetParams{
				ClassName: className,
				Pagination: &filters.Pagination{
					Offset: 0,
					Limit:  10,
				},
			}, []string{"bringYourOwnVector"}, []models.Vector{[]float32{1, 2, 3}})
			require.Nil(t, err)
			assert.Len(t, res, 3)
		})

		t.Run("can perform vector search using ColBERT embeddings", func(t *testing.T) {
			res, err := repo.VectorSearch(context.Background(), dto.GetParams{
				ClassName: className,
				Pagination: &filters.Pagination{
					Offset: 0,
					Limit:  10,
				},
			}, []string{"colbert"}, []models.Vector{[][]float32{{0.5, 0.52, 0.53}, {0.511, 0.522, 0.533}, {0.5111, 0.5222, 0.5333}}})
			require.NoError(t, err)
			assert.Len(t, res, 3)
			assert.Equal(t, "8d5a3aa2-3c8d-4589-9ae1-3f638f506970", res[0].ID.String())
		})
	})
}

func TestBatchPutObjectsNoVectorsWithDimensions(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator,
		schemaGetter))

	dimensions := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 0, dimensions, "Dimensions are empty before import")

	t.Run("batch import things", testBatchImportObjectsNoVector(repo))

	dimAfter := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 0, dimAfter, "Dimensions are empty after import (no vectors in import)")
}

func TestBatchPutObjectsNoVectors(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjectsNoVector(repo))
}

func TestBatchDeleteObjectsWithDimensions(t *testing.T) {
	className := "ThingForBatching"
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  1,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the test class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, className, 103)

	dimAfter := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 309, dimAfter, "Dimensions are present before delete")

	delete2Objects(t, repo, className)

	dimFinal := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 303, dimFinal, "2 objects have been deleted")
}

func delete2Objects(t *testing.T, repo *DB, className string) {
	batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), objects.BatchDeleteParams{
		ClassName: "ThingForBatching",
		Filters: &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorOr,
				Operands: []filters.Clause{
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "ThingForBatching",
							Property: schema.PropertyName("id"),
						},
						Value: &filters.Value{
							Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506003",
							Type:  schema.DataTypeText,
						},
					},
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "ThingForBatching",
							Property: schema.PropertyName("id"),
						},
						Value: &filters.Value{
							Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506004",
							Type:  schema.DataTypeText,
						},
					},
				},
			},
		},
		DryRun: false,
		Output: "verbose",
	}, time.Now(), nil, "", 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(batchDeleteRes.Objects), "Objects deleted")
}

func TestBatchDeleteObjects(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjects(repo))

	t.Run("batch delete things", testBatchDeleteObjects(repo))
}

func TestBatchDeleteObjects_JourneyWithDimensions(t *testing.T) {
	dirName := t.TempDir()

	queryMaximumResults := int64(200)
	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       queryMaximumResults,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, "ThingForBatching", 103)

	dimAfter := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 309, dimAfter, "Dimensions are present before delete")

	delete2Objects(t, repo, "ThingForBatching")

	dimFinal := getDimensionsFromRepo(context.Background(), repo, "ThingForBatching")
	require.Equal(t, 303, dimFinal, "Dimensions have been deleted")
}

func TestBatchPutObjects_CollectionPoisoningPrevention(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	className := "PoisonPreventionBatch"
	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	require.NoError(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	dimBefore := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 0, dimBefore, "Dimensions are 0 before import")

	id1 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506001")
	id2 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506002")
	id3 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506003")

	conflictingBatch := objects.BatchObjects{
		objects.BatchObject{
			OriginalIndex: 0,
			Object: &models.Object{
				Class:  className,
				ID:     id1,
				Vector: []float32{1, 2, 3},
				Properties: map[string]interface{}{
					"text": "item 1 with 3D vector",
				},
			},
			UUID: id1,
		},
		objects.BatchObject{
			OriginalIndex: 1,
			Object: &models.Object{
				Class:  className,
				ID:     id2,
				Vector: []float32{1, 2, 3, 4},
				Properties: map[string]interface{}{
					"text": "item 2 with 4D vector",
				},
			},
			UUID: id2,
		},
		objects.BatchObject{
			OriginalIndex: 2,
			Object: &models.Object{
				Class: className,
				ID:    id3,
				Properties: map[string]interface{}{
					"text": "item 3 without vector",
				},
			},
			UUID: id3,
		},
	}

	batchRes, err := repo.BatchPutObjects(context.Background(), conflictingBatch, nil, 0)
	require.NoError(t, err)
	require.Len(t, batchRes, 3)

	expectedErr := `target vector "": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
	require.Error(t, batchRes[0].Err)
	assert.Equal(t, expectedErr, batchRes[0].Err.Error())
	require.Error(t, batchRes[1].Err)
	assert.Equal(t, expectedErr, batchRes[1].Err.Error())
	require.NoError(t, batchRes[2].Err, "non-vectorized object must succeed")

	// Verify LSM: conflicting objects must not be found, non-vectorized object must be found
	params := dto.GetParams{
		ClassName:  className,
		Pagination: &filters.Pagination{Limit: 10},
	}
	searchRes, err := repo.Search(context.Background(), params)
	require.NoError(t, err)
	_, found1 := findID(searchRes, id1)
	assert.False(t, found1, "conflicting object 1 must not exist in LSM")
	_, found2 := findID(searchRes, id2)
	assert.False(t, found2, "conflicting object 2 must not exist in LSM")
	_, found3 := findID(searchRes, id3)
	assert.True(t, found3, "non-vectorized object must exist in LSM")

	// Dimensions must remain 0
	dimAfterPoison := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 0, dimAfterPoison, "Dimensions must remain 0 after rejected conflicting batch")

	// Subsequent batch with uniform 4D vectors succeeds
	subId1 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506004")
	subId2 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506005")
	validBatch := objects.BatchObjects{
		objects.BatchObject{
			OriginalIndex: 0,
			Object: &models.Object{
				Class:  className,
				ID:     subId1,
				Vector: []float32{1, 2, 3, 4},
				Properties: map[string]interface{}{
					"text": "valid item 1",
				},
			},
			UUID: subId1,
		},
		objects.BatchObject{
			OriginalIndex: 1,
			Object: &models.Object{
				Class:  className,
				ID:     subId2,
				Vector: []float32{5, 6, 7, 8},
				Properties: map[string]interface{}{
					"text": "valid item 2",
				},
			},
			UUID: subId2,
		},
	}

	validRes, err := repo.BatchPutObjects(context.Background(), validBatch, nil, 0)
	require.NoError(t, err)
	require.Len(t, validRes, 2)
	require.NoError(t, validRes[0].Err)
	require.NoError(t, validRes[1].Err)

	// Dimension is now established to 8 (2 objects * 4 dim)
	dimFinal := getDimensionsFromRepo(context.Background(), repo, className)
	require.Equal(t, 8, dimFinal, "Dimensions must reflect uniform batch vectors (2 * 4)")

	// Vector search succeeds
	vSearchRes, err := repo.VectorSearch(context.Background(), dto.GetParams{
		ClassName:  className,
		Pagination: &filters.Pagination{Limit: 10},
	}, []string{""}, []models.Vector{[]float32{1, 2, 3, 4}})
	require.NoError(t, err)
	assert.Len(t, vSearchRes, 2)
}

func TestBatchDeleteObjects_Journey(t *testing.T) {
	dirName := t.TempDir()

	queryMaximumResults := int64(20)
	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       queryMaximumResults,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator,
		schemaGetter))
	t.Run("batch import things", testBatchImportObjects(repo))
	t.Run("batch delete journey things", testBatchDeleteObjectsJourney(repo, queryMaximumResults))
}

func testAddBatchObjectClass(repo *DB, migrator *Migrator,
	schemaGetter *fakeSchemaGetter,
) func(t *testing.T) {
	return func(t *testing.T) {
		class := &models.Class{
			Class:               "ThingForBatching",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		require.Nil(t, migrator.AddClass(context.Background(), class))

		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{class},
		}
	}
}

func testBatchImportObjectsNoVector(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("with a prior validation error, but nothing to cause errors in the db", func(t *testing.T) {
			batch := objects.BatchObjects{
				objects.BatchObject{
					OriginalIndex: 0,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					},
					UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
				},
				objects.BatchObject{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already has a validation error"),
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					},
					UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
				},
				objects.BatchObject{
					OriginalIndex: 2,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "third element",
						},
						ID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
					},
					UUID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			_, err := repo.Search(context.Background(), params)
			require.Nil(t, err)
		})
	}
}

func simpleInsertObjects(t *testing.T, repo *DB, class string, count int) {
	simpleInsertObjectsForTenant(t, repo, class, "", count)
}

func simpleInsertObjectsForTenant(t *testing.T, repo *DB, class, tenant string, count int) {
	batch := make(objects.BatchObjects, count)
	for i := 0; i < count; i++ {
		id := strfmt.UUID(fmt.Sprintf("8d5a3aa2-3c8d-4589-9ae1-3f638f506%03d", i))
		batch[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object: &models.Object{
				Class: class,
				Properties: map[string]interface{}{
					"stringProp": fmt.Sprintf("element %d", i),
				},
				ID:     id,
				Tenant: tenant,
				Vector: []float32{1, 2, 3},
			},
			UUID: id,
		}
	}

	res, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
	require.NoError(t, err)
	assertAllItemsErrorFree(t, res)
}

func testBatchImportObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("with a prior validation error, but nothing to cause errors in the db", func(t *testing.T) {
			batch := objects.BatchObjects{
				objects.BatchObject{
					OriginalIndex: 0,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "first element",
						},
						ID:     "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
						Vector: []float32{1, 2, 3},
					},
					UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
				},
				objects.BatchObject{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already has a validation error"),
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "second element",
						},
						ID:     "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
						Vector: []float32{1, 2, 3},
					},
					UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
				},
				objects.BatchObject{
					OriginalIndex: 2,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "third element",
						},
						ID:     "90ade18e-2b99-4903-aa34-1d5d648c932d",
						Vector: []float32{1, 2, 3},
					},
					UUID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)

			t.Run("contains first element", func(t *testing.T) {
				item, ok := findID(res, batch[0].Object.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "first element", item.Schema.(map[string]interface{})["stringProp"])
			})

			t.Run("contains third element", func(t *testing.T) {
				item, ok := findID(res, batch[2].Object.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "third element", item.Schema.(map[string]interface{})["stringProp"])
			})

			t.Run("can be queried through the inverted index", func(t *testing.T) {
				filter := buildFilter("stringProp", "third", eq, schema.DataTypeText)
				params := dto.GetParams{
					ClassName:  "ThingForBatching",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    filter,
				}
				res, err := repo.Search(context.Background(), params)
				require.Nil(t, err)

				require.Len(t, res, 1)
				assert.Equal(t, strfmt.UUID("90ade18e-2b99-4903-aa34-1d5d648c932d"),
					res[0].ID)
			})
		})

		t.Run("with an import which will fail", func(t *testing.T) {
			batch := objects.BatchObjects{
				objects.BatchObject{
					OriginalIndex: 0,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
					},
					UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
				},
				objects.BatchObject{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already had a prior error"),
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
					},
					UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
				},
				objects.BatchObject{
					OriginalIndex: 2,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "third element",
						},
						ID: "", // ID can't be empty in es, this should produce an error
					},
					UUID: "",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 3)
					assert.NotNil(t, batchRes[1].Err) // from validation
					assert.NotNil(t, batchRes[2].Err) // from db
				})
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)

			t.Run("does not contain second element (validation error)", func(t *testing.T) {
				_, ok := findID(res, batch[1].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[2].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})
		})

		t.Run("upserting the same objects over and over again", func(t *testing.T) {
			for i := 0; i < 20; i++ {
				batch := objects.BatchObjects{
					objects.BatchObject{
						OriginalIndex: 0,
						Err:           nil,
						Object: &models.Object{
							Class: "ThingForBatching",
							Properties: map[string]interface{}{
								"stringProp": "first element",
							},
							ID:     "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
							Vector: []float32{1, 2, 3},
						},
						UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					},
					objects.BatchObject{
						OriginalIndex: 1,
						Err:           nil,
						Object: &models.Object{
							Class: "ThingForBatching",
							Properties: map[string]interface{}{
								"stringProp": "third element",
							},
							ID:     "90ade18e-2b99-4903-aa34-1d5d648c932d",
							Vector: []float32{1, 1, -3},
						},
						UUID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
					},
				}

				t.Run("can import", func(t *testing.T) {
					batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
					require.Nil(t, err)

					assert.Nil(t, batchRes[0].Err)
					assert.Nil(t, batchRes[1].Err)
				})

				t.Run("a vector search returns the correct number of elements", func(t *testing.T) {
					res, err := repo.VectorSearch(context.Background(), dto.GetParams{
						ClassName: "ThingForBatching",
						Pagination: &filters.Pagination{
							Offset: 0,
							Limit:  10,
						},
					}, []string{""}, []models.Vector{[]float32{1, 2, 3}})
					require.Nil(t, err)
					assert.Len(t, res, 2)
				})

			}
		})

		t.Run("with a duplicate UUID", func(t *testing.T) {
			// it should ignore the first one as the second one would overwrite the
			// first one anyway
			batch := make(objects.BatchObjects, 53)

			batch[0] = objects.BatchObject{
				OriginalIndex: 0,
				Err:           nil,
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element",
					},
					ID:     "79aebd44-7486-4fed-9334-3a74cc09a1c3",
					Vector: []float32{7, 8, 9},
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
			}

			// add 50 more nonsensical items, so we cross the transaction threshold

			for i := 1; i < 51; i++ {
				uid, err := uuid.NewRandom()
				require.Nil(t, err)
				id := strfmt.UUID(uid.String())
				batch[i] = objects.BatchObject{
					OriginalIndex: i,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "ignore me",
						},
						ID:     id,
						Vector: []float32{0.05, 0.1, 0.2},
					},
					UUID: id,
				}
			}

			batch[51] = objects.BatchObject{
				OriginalIndex: 51,
				Err:           fmt.Errorf("already had a prior error"),
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element",
					},
					ID:     "1c2d8ce6-32da-4081-9794-a81e23e673e4",
					Vector: []float32{3, 2, 1},
				},
				UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
			}
			batch[52] = objects.BatchObject{
				OriginalIndex: 52,
				Err:           nil,
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element, imported a second time",
					},
					ID:     "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
					Vector: []float32{1, 2, 3},
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 53)
					assert.NotNil(t, batchRes[51].Err) // from validation
				})
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
			require.Nil(t, err)

			t.Run("does not contain second element (validation error)", func(t *testing.T) {
				_, ok := findID(res, batch[51].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[52].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})
		})

		t.Run("when a context expires", func(t *testing.T) {
			// it should ignore the first one as the second one would overwrite the
			// first one anyway
			size := 50
			batch := make(objects.BatchObjects, size)
			// add 50 more nonsensical items, so we cross the transaction threshold

			for i := 0; i < size; i++ {
				uid, err := uuid.NewRandom()
				require.Nil(t, err)
				id := strfmt.UUID(uid.String())
				batch[i] = objects.BatchObject{
					Err: nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "ignore me",
						},
						ID:     id,
						Vector: []float32{0.05, 0.1, 0.2},
					},
					UUID: id,
				}
			}

			t.Run("can import", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				defer cancel()

				batchRes, err := repo.BatchPutObjects(ctx, batch, nil, 0)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

				t.Run("some elements have error'd due to context", func(t *testing.T) {
					require.Len(t, batchRes, 50)

					errCount := 0
					for _, elem := range batchRes {
						if elem.Err != nil {
							errCount++
							assert.Contains(t, elem.Err.Error(), "context deadline exceeded")
						}
					}

					assert.True(t, errCount > 0)
				})
			})
		})
	}
}

// geo props are the first props with property specific indices, so making sure
// that they work with batches at scale adds value beyond the regular batch
// import tests
func testBatchImportGeoObjects(repo *DB) func(t *testing.T) {
	r := getRandomSeed()
	return func(t *testing.T) {
		size := 500
		batchSize := 50

		objs := make([]*models.Object, size)

		t.Run("generate random vectors", func(t *testing.T) {
			for i := 0; i < size; i++ {
				id, _ := uuid.NewRandom()
				objs[i] = &models.Object{
					Class: "ThingForBatching",
					ID:    strfmt.UUID(id.String()),
					Properties: map[string]interface{}{
						"location": randGeoCoordinates(r),
					},
					Vector: []float32{0.123, 0.234, rand.Float32()}, // does not matter for this test
				}
			}
		})

		t.Run("import vectors in batches", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(objects.BatchObjects, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = objects.BatchObject{
						OriginalIndex: j,
						Object:        objs[i+j],
					}
				}

				res, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err)
				assertAllItemsErrorFree(t, res)
			}
		})

		const km = 1000
		distances := []float32{
			0.1,
			1,
			10,
			100,
			1000,
			2000,
			5000,
			7500,
			10000,
			12500,
			15000,
			20000,
			35000,
			100000, // larger than the circumference of the earth, should contain all
		}

		t.Run("query for expected results", func(t *testing.T) {
			queryGeo := randGeoCoordinates(r)

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.Search(context.Background(), dto.GetParams{
						ClassName:  "ThingForBatching",
						Pagination: &filters.Pagination{Limit: 500},
						Filters: buildFilter("location", filters.GeoRange{
							GeoCoordinates: queryGeo,
							Distance:       maxDist * km,
						}, filters.OperatorWithinGeoRange, schema.DataTypeGeoCoordinates),
					})
					require.Nil(t, err)

					retrieved += len(res)
					relevant += matchesInUUIDLists(controlList, resToUUIDs(res))

					if relevant == 0 {
						// skip, as we risk dividing by zero, if both relevant and retrieved
						// are zero, however, we want to fail with a divide-by-zero if only
						// retrieved is 0 and relevant was more than 0
						return
					}
					recall := float32(relevant) / float32(retrieved)
					assert.True(t, recall >= 0.99)
				})
			}
		})

		t.Run("renew vector positions to test batch geo updates", func(t *testing.T) {
			for i, obj := range objs {
				obj.Properties = map[string]interface{}{
					"location": randGeoCoordinates(r),
				}
				objs[i] = obj
			}
		})

		t.Run("import in batches again (as update - same IDs!)", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(objects.BatchObjects, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = objects.BatchObject{
						OriginalIndex: j,
						Object:        objs[i+j],
					}
				}

				res, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
				require.Nil(t, err)
				assertAllItemsErrorFree(t, res)
			}
		})

		t.Run("query again to verify updates worked", func(t *testing.T) {
			queryGeo := randGeoCoordinates(r)

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.Search(context.Background(), dto.GetParams{
						ClassName:  "ThingForBatching",
						Pagination: &filters.Pagination{Limit: 500},
						Filters: buildFilter("location", filters.GeoRange{
							GeoCoordinates: queryGeo,
							Distance:       maxDist * km,
						}, filters.OperatorWithinGeoRange, schema.DataTypeGeoCoordinates),
					})
					require.Nil(t, err)

					retrieved += len(res)
					relevant += matchesInUUIDLists(controlList, resToUUIDs(res))

					if relevant == 0 {
						// skip, as we risk dividing by zero, if both relevant and retrieved
						// are zero, however, we want to fail with a divide-by-zero if only
						// retrieved is 0 and relevant was more than 0
						return
					}
					recall := float32(relevant) / float32(retrieved)
					t.Logf("recall is %f\n", recall)
					assert.True(t, recall >= 0.99)
				})
			}
		})
	}
}

func testBatchDeleteObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		getParams := func(dryRun bool, output string) objects.BatchDeleteParams {
			return objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLike,
						Value: &filters.Value{
							Value: "*",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Property: schema.PropertyName("id"),
						},
					},
				},
				DryRun: dryRun,
				Output: output,
			}
		}
		performClassSearch := func() ([]search.Result, error) {
			return repo.Search(context.Background(), dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10000},
			})
		}
		t.Run("batch delete with dryRun set to true", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), time.Now(), nil, "", 0)
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete, len(res))
		})

		t.Run("batch delete with dryRun set to true and output to minimal", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "minimal"), time.Now(), nil, "", 0)
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete, len(res))
		})

		t.Run("batch delete only 2 given objects", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorOr,
						Operands: []filters.Clause{
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    "ThingForBatching",
									Property: schema.PropertyName("id"),
								},
								Value: &filters.Value{
									Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
									Type:  schema.DataTypeText,
								},
							},
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    "ThingForBatching",
									Property: schema.PropertyName("id"),
								},
								Value: &filters.Value{
									Value: "90ade18e-2b99-4903-aa34-1d5d648c932d",
									Type:  schema.DataTypeText,
								},
							},
						},
					},
				},
				DryRun: false,
				Output: "verbose",
			}, time.Now(), nil, "", 0)
			require.Nil(t, err)
			require.Equal(t, int64(2), batchDeleteRes.Matches)
			require.Equal(t, 2, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete-2, len(res))
		})

		t.Run("batch delete with dryRun set to false", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(false, "verbose"), time.Now(), nil, "", 0)
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, 0, len(res))
		})
	}
}

func testBatchDeleteObjectsJourney(repo *DB, queryMaximumResults int64) func(t *testing.T) {
	return func(t *testing.T) {
		getParams := func(dryRun bool, output string) objects.BatchDeleteParams {
			return objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLike,
						Value: &filters.Value{
							Value: "*",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Property: schema.PropertyName("id"),
						},
					},
				},
				DryRun: dryRun,
				Output: output,
			}
		}
		performClassSearch := func() ([]search.Result, error) {
			return repo.Search(context.Background(), dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 20},
			})
		}
		t.Run("batch delete journey", func(t *testing.T) {
			// The import's expired-context batch stores anywhere from none to all of its
			// objects, so the journey counts what is stored instead of assuming a number.
			stored := storedObjectCount(t, repo, "ThingForBatching")
			require.Greater(t, int64(stored), queryMaximumResults,
				"the journey needs more objects than one call deletes")

			// delete objects to limit
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), time.Now(), nil, "", 0)
			require.Nil(t, err)
			objectsMatches := batchDeleteRes.Matches

			leftToDelete := objectsMatches
			deleteIterationCount := 0
			deletedObjectsCount := 0
			for {
				// delete objects to limit
				batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(false, "verbose"), time.Now(), nil, "", 0)
				require.Nil(t, err)
				matches, deleted := batchDeleteRes.Matches, len(batchDeleteRes.Objects)
				require.Equal(t, leftToDelete, matches)
				require.True(t, deleted > 0)
				require.LessOrEqual(t, int64(deleted), queryMaximumResults)
				deletedObjectsCount += deleted

				batchDeleteRes, err = repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), time.Now(), nil, "", 0)
				require.Nil(t, err)
				leftToDelete = batchDeleteRes.Matches

				res, err := performClassSearch()
				require.Nil(t, err)
				afterDelete := len(res)
				require.True(t, afterDelete >= 0)
				if afterDelete == 0 {
					// where have deleted all objects
					break
				}
				deleteIterationCount += 1
				if deleteIterationCount > 100 {
					// something went wrong
					break
				}
			}
			require.False(t, deleteIterationCount > 100, "Batch delete journey tests didn't stop properly")
			require.Equal(t, queryMaximumResults+1, objectsMatches,
				"the first dry run stops one past the limit")
			require.Equal(t, stored, deletedObjectsCount,
				"every stored object is deleted across the calls")
			require.Zero(t, storedObjectCount(t, repo, "ThingForBatching"))
		})
	}
}

func storedObjectCount(t *testing.T, repo *DB, className string) int {
	t.Helper()

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)

	stored := 0
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		count, err := shard.ObjectCount(context.Background())
		stored += count
		return err
	}))
	return stored
}

func assertAllItemsErrorFree(t *testing.T, res objects.BatchObjects) {
	for _, elem := range res {
		assert.Nil(t, elem.Err)
	}
}

func bruteForceMaxDist(inputs []*models.Object, query []float32, maxDist float32) []strfmt.UUID {
	type distanceAndIndex struct {
		distance float32
		index    int
	}

	distances := make([]distanceAndIndex, len(inputs))

	distancer := distancer.NewGeoProvider().New(query)
	for i, elem := range inputs {
		coord := elem.Properties.(map[string]interface{})["location"].(*models.GeoCoordinates)
		vec := []float32{*coord.Latitude, *coord.Longitude}

		dist, _ := distancer.Distance(vec)
		distances[i] = distanceAndIndex{
			index:    i,
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]strfmt.UUID, len(distances))
	i := 0
	for _, elem := range distances {
		if elem.distance > maxDist {
			break
		}
		out[i] = inputs[distances[i].index].ID
		i++
	}

	return out[:i]
}

func randGeoCoordinates(r *rand.Rand) *models.GeoCoordinates {
	maxLat := float32(90.0)
	minLat := float32(-90.0)
	maxLon := float32(180)
	minLon := float32(-180)

	lat := minLat + (maxLat-minLat)*r.Float32()
	lon := minLon + (maxLon-minLon)*r.Float32()
	return &models.GeoCoordinates{
		Latitude:  &lat,
		Longitude: &lon,
	}
}

func resToUUIDs(in []search.Result) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))
	for i, obj := range in {
		out[i] = obj.ID
	}

	return out
}

func matchesInUUIDLists(control []strfmt.UUID, results []strfmt.UUID) int {
	desired := map[strfmt.UUID]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func TestBatchPutObjects_EstablishedCollection_DimensionMismatch(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	testAddBatchObjectClass(repo, migrator, schemaGetter)(t)

	// Step 1: Establish dimension = 3 with initial batch
	validID1 := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506970")
	initialBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: "ThingForBatching",
				Properties: map[string]interface{}{
					"stringProp": "initial element",
				},
				ID:     validID1,
				Vector: []float32{1, 2, 3},
			},
			UUID: validID1,
		},
	}
	batchRes, err := repo.BatchPutObjects(context.Background(), initialBatch, nil, 0)
	require.NoError(t, err)
	require.NoError(t, batchRes[0].Err)

	// Step 2: Mixed batch targeting established collection (dimension = 3)
	validID2 := strfmt.UUID("86a380e9-cb60-4b2a-bc48-51f52acd72d6")
	invalidID1 := strfmt.UUID("90ade18e-2b99-4903-aa34-1d5d648c932d")
	invalidID2 := strfmt.UUID("79aebd44-7486-4fed-9334-3a74cc09a1c3")
	mixedBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: "ThingForBatching",
				Properties: map[string]interface{}{
					"stringProp": "valid element 2",
				},
				ID:     validID2,
				Vector: []float32{4, 5, 6},
			},
			UUID: validID2,
		},
		{
			OriginalIndex: 1,
			Object: &models.Object{
				Class: "ThingForBatching",
				Properties: map[string]interface{}{
					"stringProp": "invalid element 1 (dim 4)",
				},
				ID:     invalidID1,
				Vector: []float32{1, 2, 3, 4},
			},
			UUID: invalidID1,
		},
		{
			OriginalIndex: 2,
			Object: &models.Object{
				Class: "ThingForBatching",
				Properties: map[string]interface{}{
					"stringProp": "invalid element 2 (dim 2)",
				},
				ID:     invalidID2,
				Vector: []float32{1, 2},
			},
			UUID: invalidID2,
		},
	}

	batchRes, err = repo.BatchPutObjects(context.Background(), mixedBatch, nil, 0)
	require.NoError(t, err)
	require.Len(t, batchRes, 3)

	// Assert per-object errors
	require.NoError(t, batchRes[0].Err)
	require.Error(t, batchRes[1].Err)
	assert.Equal(t, `target vector "": dimension mismatch: expected 3, got 4`, batchRes[1].Err.Error())
	require.Error(t, batchRes[2].Err)
	assert.Equal(t, `target vector "": dimension mismatch: expected 3, got 2`, batchRes[2].Err.Error())

	// Step 3: Verify rejected objects are NEVER written to LSM storage (Ghost Object Prevention)
	invalidObj1, err := repo.ObjectByID(context.Background(), invalidID1, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Nil(t, invalidObj1, "rejected object 1 must not exist in storage")

	invalidObj2, err := repo.ObjectByID(context.Background(), invalidID2, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Nil(t, invalidObj2, "rejected object 2 must not exist in storage")

	// Valid objects must exist in LSM storage
	validObj1, err := repo.ObjectByID(context.Background(), validID1, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.NotNil(t, validObj1, "valid object 1 must exist in storage")

	validObj2, err := repo.ObjectByID(context.Background(), validID2, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.NotNil(t, validObj2, "valid object 2 must exist in storage")

	// Step 4: Vector search works without errors after the rejected batch
	searchRes, err := repo.VectorSearch(context.Background(), dto.GetParams{
		ClassName:  "ThingForBatching",
		Pagination: &filters.Pagination{Limit: 10},
	}, []string{""}, []models.Vector{[]float32{1, 2, 3}})
	require.NoError(t, err)
	require.NotEmpty(t, searchRes)

	// Step 5: Subsequent valid batch succeeds
	validID3 := strfmt.UUID("1c2d8ce6-32da-4081-9794-a81e23e673e4")
	subsequentBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: "ThingForBatching",
				Properties: map[string]interface{}{
					"stringProp": "subsequent valid element",
				},
				ID:     validID3,
				Vector: []float32{7, 8, 9},
			},
			UUID: validID3,
		},
	}
	subsequentRes, err := repo.BatchPutObjects(context.Background(), subsequentBatch, nil, 0)
	require.NoError(t, err)
	require.NoError(t, subsequentRes[0].Err)
}

func TestBatchPutObjects_NamedVectors_DimensionValidation(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	className := "NamedVectorsBatchTest"
	class := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"custom1": {
				VectorIndexType:   "hnsw",
				VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			},
			"custom2": {
				VectorIndexType:   "hnsw",
				VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			},
		},
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	require.NoError(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	// Step 1: Establish dimensions = 3 for custom1 and 4 for custom2
	initID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506001")
	initBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: className,
				ID:    initID,
				Vectors: models.Vectors{
					"custom1": []float32{1, 2, 3},
					"custom2": []float32{1, 2, 3, 4},
				},
			},
			UUID: initID,
		},
	}
	initRes, err := repo.BatchPutObjects(context.Background(), initBatch, nil, 0)
	require.NoError(t, err)
	require.NoError(t, initRes[0].Err)

	// Step 2: Batch containing valid object and object with mismatch on custom2
	validID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506002")
	invalidID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506003")
	mixedBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: className,
				ID:    validID,
				Vectors: models.Vectors{
					"custom1": []float32{4, 5, 6},
					"custom2": []float32{5, 6, 7, 8},
				},
			},
			UUID: validID,
		},
		{
			OriginalIndex: 1,
			Object: &models.Object{
				Class: className,
				ID:    invalidID,
				Vectors: models.Vectors{
					"custom1": []float32{4, 5, 6},
					"custom2": []float32{5, 6, 7}, // 3D != 4D
				},
			},
			UUID: invalidID,
		},
	}

	batchRes, err := repo.BatchPutObjects(context.Background(), mixedBatch, nil, 0)
	require.NoError(t, err)
	require.Len(t, batchRes, 2)

	require.NoError(t, batchRes[0].Err)
	require.Error(t, batchRes[1].Err)
	assert.Equal(t, `target vector "custom2": dimension mismatch: expected 4, got 3`, batchRes[1].Err.Error())

	// Step 3: Verify LSM storage
	validObj, err := repo.ObjectByID(context.Background(), validID, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.NotNil(t, validObj, "valid object must exist in storage")

	invalidObj, err := repo.ObjectByID(context.Background(), invalidID, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Nil(t, invalidObj, "rejected object must not exist in storage")
}

func TestBatchPutObjects_MultiVectors_DimensionValidation(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger, "node1")

	className := "MultiVectorsBatchTest"
	class := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"colbert": {
				VectorIndexType:   "hnsw",
				VectorIndexConfig: enthnsw.NewDefaultMultiVectorUserConfig(),
			},
		},
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	require.NoError(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	// Test 1: Inconsistent token lengths rejected before LSM
	badTokenID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506001")
	badBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: className,
				ID:    badTokenID,
				Vectors: models.Vectors{
					"colbert": [][]float32{
						{1, 2, 3},
						{4, 5}, // 2D != 3D
					},
				},
			},
			UUID: badTokenID,
		},
	}
	badRes, err := repo.BatchPutObjects(context.Background(), badBatch, nil, 0)
	require.NoError(t, err)
	require.Error(t, badRes[0].Err)
	assert.Equal(t, `target vector "colbert": multi vector array consists of vectors with varying dimensions`, badRes[0].Err.Error())

	badObj, err := repo.ObjectByID(context.Background(), badTokenID, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Nil(t, badObj, "object with inconsistent tokens must not exist in storage")

	// Test 2: Establish dimension = 3 and verify dimension mismatch rejection
	initID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506002")
	initBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: className,
				ID:    initID,
				Vectors: models.Vectors{
					"colbert": [][]float32{
						{1, 2, 3},
						{4, 5, 6},
					},
				},
			},
			UUID: initID,
		},
	}
	initRes, err := repo.BatchPutObjects(context.Background(), initBatch, nil, 0)
	require.NoError(t, err)
	require.NoError(t, initRes[0].Err)

	validID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506003")
	mismatchID := strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506004")
	mixedBatch := objects.BatchObjects{
		{
			OriginalIndex: 0,
			Object: &models.Object{
				Class: className,
				ID:    validID,
				Vectors: models.Vectors{
					"colbert": [][]float32{
						{7, 8, 9},
					},
				},
			},
			UUID: validID,
		},
		{
			OriginalIndex: 1,
			Object: &models.Object{
				Class: className,
				ID:    mismatchID,
				Vectors: models.Vectors{
					"colbert": [][]float32{
						{1, 2, 3, 4}, // 4D != 3D
					},
				},
			},
			UUID: mismatchID,
		},
	}

	batchRes, err := repo.BatchPutObjects(context.Background(), mixedBatch, nil, 0)
	require.NoError(t, err)
	require.NoError(t, batchRes[0].Err)
	require.Error(t, batchRes[1].Err)
	assert.Equal(t, `target vector "colbert": dimension mismatch: expected 3, got 4`, batchRes[1].Err.Error())

	validObj, err := repo.ObjectByID(context.Background(), validID, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.NotNil(t, validObj, "valid object must exist in storage")

	mismatchObj, err := repo.ObjectByID(context.Background(), mismatchID, search.SelectProperties{}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Nil(t, mismatchObj, "mismatched object must not exist in storage")
}
