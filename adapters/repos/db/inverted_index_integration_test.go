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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestIndexByTimestampsNullStatePropLength_AddClass(t *testing.T) {
	dirName := t.TempDir()
	vFalse := false
	vTrue := true

	class := &models.Class{
		Class:             "TestClass",
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexTimestamps:     true,
			IndexNullState:      true,
			IndexPropertyLength: true,
			UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "initialWithIINil",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:            "initialWithIITrue",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "initialWithoutII",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
			},
		},
	}
	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		return readerFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()
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
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger, "node1")

	require.Nil(t, migrator.AddProperty(context.Background(), class.Class, &models.Property{
		Name:         "updateWithIINil",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	}))
	require.Nil(t, migrator.AddProperty(context.Background(), class.Class, &models.Property{
		Name:            "updateWithIITrue",
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWhitespace,
		IndexFilterable: &vTrue,
		IndexSearchable: &vTrue,
	}))
	require.Nil(t, migrator.AddProperty(context.Background(), class.Class, &models.Property{
		Name:            "updateWithoutII",
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWhitespace,
		IndexFilterable: &vFalse,
		IndexSearchable: &vFalse,
	}))

	t.Run("check for additional buckets", func(t *testing.T) {
		for _, idx := range migrator.db.indices {
			idx.ForEachShard(func(_ string, shd ShardLike) error {
				createBucket := shd.Store().Bucket("property__creationTimeUnix")
				assert.NotNil(t, createBucket)

				updateBucket := shd.Store().Bucket("property__lastUpdateTimeUnix")
				assert.NotNil(t, updateBucket)

				cases := []struct {
					prop        string
					compareFunc func(t assert.TestingT, object interface{}, msgAndArgs ...interface{}) bool
				}{
					{prop: "initialWithIINil", compareFunc: assert.NotNil},
					{prop: "initialWithIITrue", compareFunc: assert.NotNil},
					{prop: "initialWithoutII", compareFunc: assert.Nil},
					{prop: "updateWithIINil", compareFunc: assert.NotNil},
					{prop: "updateWithIITrue", compareFunc: assert.NotNil},
					{prop: "updateWithoutII", compareFunc: assert.Nil},
				}
				for _, tt := range cases {
					tt.compareFunc(t, shd.Store().Bucket("property_"+tt.prop+filters.InternalNullIndex))
					tt.compareFunc(t, shd.Store().Bucket("property_"+tt.prop+filters.InternalPropertyLength))
				}
				return nil
			})
		}
	})

	t.Run("Add Objects", func(t *testing.T) {
		testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
		objWithProperty := &models.Object{
			ID:         testID1,
			Class:      "TestClass",
			Properties: map[string]interface{}{"initialWithIINil": "0", "initialWithIITrue": "0", "initialWithoutII": "1", "updateWithIINil": "2", "updateWithIITrue": "2", "updateWithoutII": "3"},
		}
		vec := []float32{1, 2, 3}
		require.Nil(t, repo.PutObject(context.Background(), objWithProperty, vec, nil, nil, nil, 0))

		testID2 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a63")
		objWithoutProperty := &models.Object{
			ID:         testID2,
			Class:      "TestClass",
			Properties: map[string]interface{}{},
		}
		require.Nil(t, repo.PutObject(context.Background(), objWithoutProperty, vec, nil, nil, nil, 0))

		testID3 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a64")
		objWithNilProperty := &models.Object{
			ID:         testID3,
			Class:      "TestClass",
			Properties: map[string]interface{}{"initialWithIINil": nil, "initialWithIITrue": nil, "initialWithoutII": nil, "updateWithIINil": nil, "updateWithIITrue": nil, "updateWithoutII": nil},
		}
		require.Nil(t, repo.PutObject(context.Background(), objWithNilProperty, vec, nil, nil, nil, 0))
	})

	t.Run("delete class", func(t *testing.T) {
		require.Nil(t, migrator.DropClass(context.Background(), class.Class, false))
		for _, idx := range migrator.db.indices {
			idx.ForEachShard(func(name string, shd ShardLike) error {
				require.Nil(t, shd.Store().Bucket("property__creationTimeUnix"))
				require.Nil(t, shd.Store().Bucket("property_name"+filters.InternalNullIndex))
				require.Nil(t, shd.Store().Bucket("property_name"+filters.InternalPropertyLength))
				return nil
			})
		}
	})
}

func TestIndexNullState_GetClass(t *testing.T) {
	dirName := t.TempDir()

	testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	testID2 := strfmt.UUID("65be32cc-bb74-49c7-833e-afb14f957eae")
	refID1 := strfmt.UUID("f2e42a9f-e0b5-46bd-8a9c-e70b6330622c")
	refID2 := strfmt.UUID("92d5920c-1c20-49da-9cdc-b765813e175b")

	var repo *DB
	var schemaGetter *fakeSchemaGetter

	t.Run("init repo", func(t *testing.T) {
		shardState := singleShardState()
		schemaGetter = &fakeSchemaGetter{
			shardState: shardState,
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readerFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
		mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
		var err error
		repo, err = New(logrus.New(), "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
		require.Nil(t, err)
		repo.SetSchemaGetter(schemaGetter)
		require.Nil(t, repo.WaitForStartup(testCtx()))
	})

	defer repo.Shutdown(testCtx())

	t.Run("add classes", func(t *testing.T) {
		class := &models.Class{
			Class:             "TestClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexNullState:      true,
				IndexTimestamps:     true,
				IndexPropertyLength: true,
				UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
			},
		}

		refClass := &models.Class{
			Class:             "RefClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps:     true,
				IndexPropertyLength: true,
				UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:     "toTest",
					DataType: []string{"TestClass"},
				},
			},
		}

		migrator := NewMigrator(repo, repo.logger, "node1")
		err := migrator.AddClass(context.Background(), class)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass)
		require.Nil(t, err)
		schemaGetter.schema.Objects.Classes = append(schemaGetter.schema.Objects.Classes, class, refClass)
	})

	t.Run("insert test objects", func(t *testing.T) {
		vec := []float32{1, 2, 3}
		for _, obj := range []*models.Object{
			{
				ID:    testID1,
				Class: "TestClass",
				Properties: map[string]interface{}{
					"name": "object1",
				},
			},
			{
				ID:    testID2,
				Class: "TestClass",
				Properties: map[string]interface{}{
					"name": nil,
				},
			},
			{
				ID:    refID1,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref1",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID1)),
						},
					},
				},
			},
			{
				ID:    refID2,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref2",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID2)),
						},
					},
				},
			},
		} {
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
	})

	t.Run("check buckets exist", func(t *testing.T) {
		index := repo.indices["testclass"]
		n := 0
		index.ForEachShard(func(_ string, shard ShardLike) error {
			bucketNull := shard.Store().Bucket(helpers.BucketFromPropNameNullLSM("name"))
			require.NotNil(t, bucketNull)
			n++
			return nil
		})
		require.Equal(t, 1, n)
	})

	type testCase struct {
		name        string
		filter      *filters.LocalFilter
		expectedIds []strfmt.UUID
	}

	t.Run("get object with null filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "is null",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorIsNull,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "name",
						},
						Value: &filters.Value{
							Value: false,
							Type:  schema.DataTypeBoolean,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "is not null",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorIsNull,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "name",
						},
						Value: &filters.Value{
							Value: true,
							Type:  schema.DataTypeBoolean,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "TestClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})

	t.Run("get referencing object with null filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "is null",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorIsNull,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "name",
							},
						},
						Value: &filters.Value{
							Value: false,
							Type:  schema.DataTypeBoolean,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "is not null",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorIsNull,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "name",
							},
						},
						Value: &filters.Value{
							Value: true,
							Type:  schema.DataTypeBoolean,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "RefClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})
}

func TestIndexPropLength_GetClass(t *testing.T) {
	dirName := t.TempDir()

	testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	testID2 := strfmt.UUID("65be32cc-bb74-49c7-833e-afb14f957eae")
	refID1 := strfmt.UUID("f2e42a9f-e0b5-46bd-8a9c-e70b6330622c")
	refID2 := strfmt.UUID("92d5920c-1c20-49da-9cdc-b765813e175b")

	var repo *DB
	var schemaGetter *fakeSchemaGetter

	t.Run("init repo", func(t *testing.T) {
		shardState := singleShardState()
		schemaGetter = &fakeSchemaGetter{
			shardState: shardState,
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readerFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
		mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
		var err error
		repo, err = New(logrus.New(), "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
		require.Nil(t, err)
		repo.SetSchemaGetter(schemaGetter)
		require.Nil(t, repo.WaitForStartup(testCtx()))
	})

	defer repo.Shutdown(testCtx())

	t.Run("add classes", func(t *testing.T) {
		class := &models.Class{
			Class:             "TestClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexPropertyLength: true,
				IndexTimestamps:     true,
				UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:     "int_array",
					DataType: schema.DataTypeIntArray.PropString(),
				},
			},
		}

		refClass := &models.Class{
			Class:             "RefClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps:   true,
				UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:     "toTest",
					DataType: []string{"TestClass"},
				},
			},
		}

		migrator := NewMigrator(repo, repo.logger, "node1")
		err := migrator.AddClass(context.Background(), class)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass)
		require.Nil(t, err)
		schemaGetter.schema.Objects.Classes = append(schemaGetter.schema.Objects.Classes, class, refClass)
	})

	t.Run("insert test objects", func(t *testing.T) {
		vec := []float32{1, 2, 3}
		for _, obj := range []*models.Object{
			{
				ID:    testID1,
				Class: "TestClass",
				Properties: map[string]interface{}{
					"name":      "short",
					"int_array": []float64{},
				},
			},
			{
				ID:    testID2,
				Class: "TestClass",
				Properties: map[string]interface{}{
					"name":      "muchLongerName",
					"int_array": []float64{1, 2, 3},
				},
			},
			{
				ID:    refID1,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref1",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID1)),
						},
					},
				},
			},
			{
				ID:    refID2,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref2",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID2)),
						},
					},
				},
			},
		} {
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
	})

	t.Run("check buckets exist", func(t *testing.T) {
		index := repo.indices["testclass"]
		n := 0
		index.ForEachShard(func(_ string, shard ShardLike) error {
			bucketPropLengthName := shard.Store().Bucket(helpers.BucketFromPropNameLengthLSM("name"))
			require.NotNil(t, bucketPropLengthName)
			bucketPropLengthIntArray := shard.Store().Bucket(helpers.BucketFromPropNameLengthLSM("int_array"))
			require.NotNil(t, bucketPropLengthIntArray)
			n++
			return nil
		})
		require.Equal(t, 1, n)
	})

	type testCase struct {
		name        string
		filter      *filters.LocalFilter
		expectedIds []strfmt.UUID
	}

	t.Run("get object with prop length filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "name length = 5",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "len(name)",
						},
						Value: &filters.Value{
							Value: 5,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "name length >= 6",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "len(name)",
						},
						Value: &filters.Value{
							Value: 6,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID2},
			},
			{
				name: "array length = 0",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "len(int_array)",
						},
						Value: &filters.Value{
							Value: 0,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "array length < 4",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLessThan,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "len(int_array)",
						},
						Value: &filters.Value{
							Value: 4,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1, testID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "TestClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})

	t.Run("get referencing object with prop length filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "name length = 5",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "len(name)",
							},
						},
						Value: &filters.Value{
							Value: 5,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "name length >= 6",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "len(name)",
							},
						},
						Value: &filters.Value{
							Value: 6,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID2},
			},
			{
				name: "array length = 0",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "len(int_array)",
							},
						},
						Value: &filters.Value{
							Value: 0,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "array length < 4",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLessThan,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "len(int_array)",
							},
						},
						Value: &filters.Value{
							Value: 4,
							Type:  schema.DataTypeInt,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1, refID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "RefClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})
}

func TestIndexByTimestamps_GetClass(t *testing.T) {
	dirName := t.TempDir()

	time1 := time.Now()
	time2 := time1.Add(-time.Hour)
	timestamp1 := time1.UnixMilli()
	timestamp2 := time2.UnixMilli()

	testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	testID2 := strfmt.UUID("65be32cc-bb74-49c7-833e-afb14f957eae")
	refID1 := strfmt.UUID("f2e42a9f-e0b5-46bd-8a9c-e70b6330622c")
	refID2 := strfmt.UUID("92d5920c-1c20-49da-9cdc-b765813e175b")

	var repo *DB
	var schemaGetter *fakeSchemaGetter

	t.Run("init repo", func(t *testing.T) {
		shardState := singleShardState()
		schemaGetter = &fakeSchemaGetter{
			shardState: shardState,
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readerFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
		mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
		var err error
		repo, err = New(logrus.New(), "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
		require.Nil(t, err)
		repo.SetSchemaGetter(schemaGetter)
		require.Nil(t, repo.WaitForStartup(testCtx()))
	})

	defer repo.Shutdown(testCtx())

	t.Run("add classes", func(t *testing.T) {
		class := &models.Class{
			Class:             "TestClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps:     true,
				IndexPropertyLength: true,
				UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
			},
		}

		refClass := &models.Class{
			Class:             "RefClass",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps:     true,
				IndexPropertyLength: true,
				UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:     "toTest",
					DataType: []string{"TestClass"},
				},
			},
		}

		migrator := NewMigrator(repo, repo.logger, "node1")
		err := migrator.AddClass(context.Background(), class)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass)
		require.Nil(t, err)
		schemaGetter.schema.Objects.Classes = append(schemaGetter.schema.Objects.Classes, class, refClass)
	})

	t.Run("insert test objects", func(t *testing.T) {
		vec := []float32{1, 2, 3}
		for _, obj := range []*models.Object{
			{
				ID:                 testID1,
				Class:              "TestClass",
				CreationTimeUnix:   timestamp1,
				LastUpdateTimeUnix: timestamp1,
				Properties: map[string]interface{}{
					"name": "object1",
				},
			},
			{
				ID:                 testID2,
				Class:              "TestClass",
				CreationTimeUnix:   timestamp2,
				LastUpdateTimeUnix: timestamp2,
				Properties: map[string]interface{}{
					"name": "object2",
				},
			},
			{
				ID:    refID1,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref1",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID1)),
						},
					},
				},
			},
			{
				ID:    refID2,
				Class: "RefClass",
				Properties: map[string]interface{}{
					"name": "ref2",
					"toTest": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/TestClass/%s", testID2)),
						},
					},
				},
			},
		} {
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
	})

	t.Run("check buckets exist", func(t *testing.T) {
		index := repo.indices["testclass"]
		n := 0
		index.ForEachShard(func(_ string, shard ShardLike) error {
			bucketCreated := shard.Store().Bucket("property_" + filters.InternalPropCreationTimeUnix)
			require.NotNil(t, bucketCreated)
			bucketUpdated := shard.Store().Bucket("property_" + filters.InternalPropLastUpdateTimeUnix)
			require.NotNil(t, bucketUpdated)
			n++
			return nil
		})
		require.Equal(t, 1, n)
	})

	type testCase struct {
		name        string
		filter      *filters.LocalFilter
		expectedIds []strfmt.UUID
	}

	t.Run("get object with timestamp filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "by creation timestamp 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_creationTimeUnix",
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp1),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "by creation timestamp 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_creationTimeUnix",
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp2),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID2},
			},
			{
				name: "by creation date 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_creationTimeUnix",
						},
						Value: &filters.Value{
							Value: time1.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "by creation date 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_creationTimeUnix",
						},
						Value: &filters.Value{
							Value: time2.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1, testID2},
			},

			{
				name: "by updated timestamp 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_lastUpdateTimeUnix",
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp1),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "by updated timestamp 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_lastUpdateTimeUnix",
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp2),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID2},
			},
			{
				name: "by updated date 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_lastUpdateTimeUnix",
						},
						Value: &filters.Value{
							Value: time1.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1},
			},
			{
				name: "by updated date 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "TestClass",
							Property: "_lastUpdateTimeUnix",
						},
						Value: &filters.Value{
							Value: time2.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{testID1, testID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "TestClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})

	t.Run("get referencing object with timestamp filters", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "by creation timestamp 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_creationTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp1),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "by creation timestamp 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_creationTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp2),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID2},
			},
			{
				name: "by creation date 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_creationTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: time1.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "by creation date 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_creationTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: time2.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1, refID2},
			},

			{
				name: "by updated timestamp 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_lastUpdateTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp1),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "by updated timestamp 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_lastUpdateTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: fmt.Sprint(timestamp2),
							Type:  schema.DataTypeText,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID2},
			},
			{
				name: "by updated date 1",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_lastUpdateTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: time1.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1},
			},
			{
				name: "by updated date 2",
				filter: &filters.LocalFilter{
					Root: &filters.Clause{
						// since RFC3339 is limited to seconds,
						// >= operator is used to match object with timestamp containing milliseconds
						Operator: filters.OperatorGreaterThanEqual,
						On: &filters.Path{
							Class:    "RefClass",
							Property: "toTest",
							Child: &filters.Path{
								Class:    "TestClass",
								Property: "_lastUpdateTimeUnix",
							},
						},
						Value: &filters.Value{
							Value: time2.Format(time.RFC3339),
							Type:  schema.DataTypeDate,
						},
					},
				},
				expectedIds: []strfmt.UUID{refID1, refID2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:  "RefClass",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    tc.filter,
				})
				require.Nil(t, err)
				require.Len(t, res, len(tc.expectedIds))

				ids := make([]strfmt.UUID, len(res))
				for i := range res {
					ids[i] = res[i].ID
				}
				assert.ElementsMatch(t, ids, tc.expectedIds)
			})
		}
	})
}

// Cannot filter for property length without enabling in the InvertedIndexConfig
func TestFilterPropertyLengthError(t *testing.T) {
	class := createClassWithEverything(false, false)
	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(context.Background())
	err := migrator.AddClass(context.Background(), class)
	require.Nil(t, err)
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	LengthFilter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    schema.ClassName(carClass.Class),
				Property: "len(" + schema.PropertyName(class.Properties[0].Name) + ")",
			},
			Value: &filters.Value{
				Value: 1,
				Type:  dtInt,
			},
		},
	}

	params := dto.GetParams{
		ClassName:  class.Class,
		Pagination: &filters.Pagination{Limit: 5},
		Filters:    LengthFilter,
	}
	_, err = repo.Search(context.Background(), params)
	require.NotNil(t, err)
}

// createDescriptionCounterClass registers a class with a searchable
// "description" text property and a filterable "counter" int property.
func createDescriptionCounterClass(t *testing.T, ctx context.Context, migrator *Migrator, schemaGetter *fakeSchemaGetter, className string, invertedIndexConfig *models.InvertedIndexConfig) {
	t.Helper()

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedIndexConfig,
		Properties: []*models.Property{
			{
				Name:            "description",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexSearchable: boolPtr(true),
			},
			{
				Name:            "counter",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: boolPtr(true),
			},
		},
	}
	require.Nil(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
}

// Test_PatchNoOpSkipsSearchableBucketRewrite checks that a PATCH to another
// property writes nothing to an unchanged searchable bucket, and that a
// token-preserving edit still updates len().
func Test_PatchNoOpSkipsSearchableBucketRewrite(t *testing.T) {
	ctx := context.Background()
	className := "UnchangedSearchableSkip"
	textID := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0a01")
	lengthID := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0a02")

	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(ctx)

	createDescriptionCounterClass(t, ctx, migrator, schemaGetter, className, invertedConfig())

	require.Nil(t, repo.PutObject(ctx, &models.Object{
		ID:    textID,
		Class: className,
		Properties: map[string]interface{}{
			"description": "the quick brown fox jumps over a lazy dog while ravens watch silently",
			"counter":     float64(1),
		},
	}, []float32{0.1}, nil, nil, nil, 0))

	index := repo.GetIndex(schema.ClassName(className))
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		return shard.Store().PauseCompaction(ctx)
	}))
	defer func() {
		_ = index.ForEachShard(func(_ string, shard ShardLike) error {
			return shard.Store().ResumeCompaction(ctx)
		})
	}()

	searchableFiles := func() []string {
		var files []string
		require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
			bucket := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM("description"))
			require.NotNil(t, bucket)
			if err := bucket.FlushMemtable(); err != nil {
				return err
			}
			f, err := bucket.ListFiles(ctx, index.Config.RootPath)
			files = f
			return err
		}))
		return files
	}

	filesBefore := searchableFiles()

	t.Run("unrelated-property PATCH leaves the searchable bucket untouched", func(t *testing.T) {
		require.Nil(t, repo.Merge(ctx, objects.MergeDocument{
			Class: className,
			ID:    textID,
			PrimitiveSchema: map[string]interface{}{
				"counter": float64(2),
			},
		}, nil, "", 0))

		assert.ElementsMatch(t, filesBefore, searchableFiles(),
			"an unrelated-property PATCH must not write a new segment to the unchanged searchable bucket")
	})

	t.Run("a token-preserving edit that changes Length still updates len()", func(t *testing.T) {
		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:    lengthID,
			Class: className,
			Properties: map[string]interface{}{
				"description": "alpha",
				"counter":     float64(1),
			},
		}, []float32{0.2}, nil, nil, nil, 0))

		matches := func(n int) bool {
			res, err := repo.Search(ctx, dto.GetParams{
				ClassName:  className,
				Pagination: &filters.Pagination{Limit: 5},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On:       &filters.Path{Class: schema.ClassName(className), Property: "len(description)"},
						Value:    &filters.Value{Value: n, Type: dtInt},
					},
				},
			})
			require.Nil(t, err)
			for _, obj := range res {
				if obj.ID == lengthID {
					return true
				}
			}
			return false
		}

		require.True(t, matches(5), "len(description) = 5 must match before the edit")
		require.False(t, matches(6), "len(description) = 6 must not match before the edit")

		// "alpha!" tokenizes to the same single "alpha" term as "alpha" under
		// word tokenization (punctuation is a field separator, not a
		// character), so Items stays identical and only Length (a rune count
		// over the raw value, not derived from Items) changes.
		require.Nil(t, repo.Merge(ctx, objects.MergeDocument{
			Class: className,
			ID:    lengthID,
			PrimitiveSchema: map[string]interface{}{
				"description": "alpha!",
			},
		}, nil, "", 0))

		assert.True(t, matches(6), "len(description) = 6 must match after the token-preserving edit")
		assert.False(t, matches(5), "len(description) = 5 must not match after the token-preserving edit")
	})
}

// Test_AbsentZeroTokenTransitionsKeepLenAndIsNullCurrent checks that len()
// and isNull reflect only the current state across absent<->whitespace/empty
// transitions. Absent is only reachable via PUT; PATCH/merge cannot unset a
// property.
func Test_AbsentZeroTokenTransitionsKeepLenAndIsNullCurrent(t *testing.T) {
	ctx := context.Background()
	className := "AbsentZeroTokenTransitions"

	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(ctx)

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:            "description",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(true),
			},
		},
	}
	require.Nil(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	isNull := func(t *testing.T, id strfmt.UUID, want bool) bool {
		t.Helper()
		res, err := repo.Search(ctx, dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: 5},
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorIsNull,
					On:       &filters.Path{Class: schema.ClassName(className), Property: "description"},
					Value:    &filters.Value{Value: want, Type: schema.DataTypeBoolean},
				},
			},
		})
		require.Nil(t, err)
		for _, obj := range res {
			if obj.ID == id {
				return true
			}
		}
		return false
	}

	lenMatches := func(t *testing.T, id strfmt.UUID, n int) bool {
		t.Helper()
		res, err := repo.Search(ctx, dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: 5},
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On:       &filters.Path{Class: schema.ClassName(className), Property: "len(description)"},
					Value:    &filters.Value{Value: n, Type: dtInt},
				},
			},
		})
		require.Nil(t, err)
		for _, obj := range res {
			if obj.ID == id {
				return true
			}
		}
		return false
	}

	t.Run("absent -> whitespace-only via PATCH", func(t *testing.T) {
		id := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0c01")
		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:         id,
			Class:      className,
			Properties: map[string]interface{}{},
		}, []float32{0.1}, nil, nil, nil, 0))

		require.True(t, isNull(t, id, true), "absent must read as null before the update")
		require.False(t, isNull(t, id, false), "absent must not read as not-null before the update")
		require.True(t, lenMatches(t, id, 0), "absent reads len()=0 before the update")

		require.Nil(t, repo.Merge(ctx, objects.MergeDocument{
			Class: className,
			ID:    id,
			PrimitiveSchema: map[string]interface{}{
				"description": " ",
			},
		}, nil, "", 0))

		assert.False(t, isNull(t, id, true), "whitespace-only value must not read as null after absent->' '")
		assert.True(t, isNull(t, id, false), "whitespace-only value must read as not-null after absent->' '")
		assert.True(t, lenMatches(t, id, 1), "len(description)=1 must match after absent->' '")
		assert.False(t, lenMatches(t, id, 0), "len(description)=0 must not match after absent->' '")
	})

	t.Run("whitespace-only -> absent via PUT", func(t *testing.T) {
		id := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0c02")
		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:    id,
			Class: className,
			Properties: map[string]interface{}{
				"description": " ",
			},
		}, []float32{0.2}, nil, nil, nil, 0))

		require.True(t, lenMatches(t, id, 1), "len(description)=1 must match before the update")
		require.False(t, lenMatches(t, id, 0), "len(description)=0 must not match before the update")
		require.False(t, isNull(t, id, true), "' ' must not read as null before the update")
		require.True(t, isNull(t, id, false), "' ' must read as not-null before the update")

		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:         id,
			Class:      className,
			Properties: map[string]interface{}{},
		}, []float32{0.2}, nil, nil, nil, 0))

		assert.True(t, isNull(t, id, true), "absent must read as null after ' '->absent")
		assert.False(t, isNull(t, id, false), "absent must not read as not-null after ' '->absent")
		assert.True(t, lenMatches(t, id, 0), "len(description)=0 must match after ' '->absent")
		assert.False(t, lenMatches(t, id, 1), "len(description)=1 must not still match after ' '->absent")
	})

	t.Run("absent -> empty string via PATCH", func(t *testing.T) {
		id := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0c03")
		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:         id,
			Class:      className,
			Properties: map[string]interface{}{},
		}, []float32{0.3}, nil, nil, nil, 0))

		require.True(t, isNull(t, id, true), "absent must read as null before the update")
		require.True(t, lenMatches(t, id, 0), "absent reads len()=0 before the update")

		require.Nil(t, repo.Merge(ctx, objects.MergeDocument{
			Class: className,
			ID:    id,
			PrimitiveSchema: map[string]interface{}{
				"description": "",
			},
		}, nil, "", 0))

		assert.True(t, lenMatches(t, id, 0), "len(description)=0 must still match after absent->''")
	})

	t.Run("empty string -> absent via PUT", func(t *testing.T) {
		id := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0c04")
		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:    id,
			Class: className,
			Properties: map[string]interface{}{
				"description": "",
			},
		}, []float32{0.4}, nil, nil, nil, 0))

		require.True(t, lenMatches(t, id, 0), "len(description)=0 must match before the update")

		require.Nil(t, repo.PutObject(ctx, &models.Object{
			ID:         id,
			Class:      className,
			Properties: map[string]interface{}{},
		}, []float32{0.4}, nil, nil, nil, 0))

		assert.True(t, isNull(t, id, true), "absent must read as null after ''->absent")
		assert.False(t, isNull(t, id, false), "absent must not read as not-null after ''->absent")
		assert.True(t, lenMatches(t, id, 0), "len(description)=0 must match after ''->absent")
	})
}

// Test_PatchUnrelatedPropertyKeepsAveragePropertyLength checks that PATCHing
// an unrelated property does not change a BlockMax searchable bucket's
// average property length.
func Test_PatchUnrelatedPropertyKeepsAveragePropertyLength(t *testing.T) {
	ctx := context.Background()
	className := "AveragePropertyLengthUnchanged"
	shortID := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0b01")
	longID := strfmt.UUID("6f7f2b0e-df32-4f9d-9f8f-9e6c9f6d0b02")

	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(ctx)

	cfg := invertedConfig()
	cfg.UsingBlockMaxWAND = true
	createDescriptionCounterClass(t, ctx, migrator, schemaGetter, className, cfg)

	longDescription := ""
	for i := 0; i < 20; i++ {
		longDescription += fmt.Sprintf("word%02d ", i)
	}

	require.Nil(t, repo.PutObject(ctx, &models.Object{
		ID:    shortID,
		Class: className,
		Properties: map[string]interface{}{
			"description": "alpha bravo",
			"counter":     float64(0),
		},
	}, []float32{0.1}, nil, nil, nil, 0))
	require.Nil(t, repo.PutObject(ctx, &models.Object{
		ID:    longID,
		Class: className,
		Properties: map[string]interface{}{
			"description": longDescription,
			"counter":     float64(0),
		},
	}, []float32{0.2}, nil, nil, nil, 0))

	index := repo.GetIndex(schema.ClassName(className))
	averagePropertyLength := func() float64 {
		var avg float64
		require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
			bucket := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM("description"))
			require.NotNil(t, bucket)
			avg, _ = bucket.GetAveragePropertyLength()
			return nil
		}))
		return avg
	}

	const wantAverage = 11.00 // (2-term + 20-term) / 2 objects
	require.Equal(t, wantAverage, averagePropertyLength(),
		"average property length before the PATCHes must be (2+20)/2")

	for i := 1; i <= 10; i++ {
		require.Nil(t, repo.Merge(ctx, objects.MergeDocument{
			Class: className,
			ID:    longID,
			PrimitiveSchema: map[string]interface{}{
				"counter": float64(i),
			},
		}, nil, "", 0))
	}

	assert.Equal(t, wantAverage, averagePropertyLength(),
		"PATCHing an unrelated property must not change the average property length")
}
