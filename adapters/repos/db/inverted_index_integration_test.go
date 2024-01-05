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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestIndexByTimestampsNullStatePropLength_AddClass(t *testing.T) {
	dirName := t.TempDir()
	vFalse := false
	vTrue := true

	class := &models.Class{
		Class:             "TestClass",
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexTimestamps:     true,
			IndexNullState:      true,
			IndexPropertyLength: true,
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
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)
	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))

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
		require.Nil(t, repo.PutObject(context.Background(), objWithProperty, vec, nil))

		testID2 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a63")
		objWithoutProperty := &models.Object{
			ID:         testID2,
			Class:      "TestClass",
			Properties: map[string]interface{}{},
		}
		require.Nil(t, repo.PutObject(context.Background(), objWithoutProperty, vec, nil))

		testID3 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a64")
		objWithNilProperty := &models.Object{
			ID:         testID3,
			Class:      "TestClass",
			Properties: map[string]interface{}{"initialWithIINil": nil, "initialWithIITrue": nil, "initialWithoutII": nil, "updateWithIINil": nil, "updateWithIITrue": nil, "updateWithoutII": nil},
		}
		require.Nil(t, repo.PutObject(context.Background(), objWithNilProperty, vec, nil))
	})

	t.Run("delete class", func(t *testing.T) {
		require.Nil(t, migrator.DropClass(context.Background(), class.Class))
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
		schemaGetter = &fakeSchemaGetter{
			shardState: singleShardState(),
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		var err error
		repo, err = New(logrus.New(), Config{
			MemtablesFlushIdleAfter:   60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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

		migrator := NewMigrator(repo, repo.logger)
		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass, schemaGetter.shardState)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
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
		schemaGetter = &fakeSchemaGetter{
			shardState: singleShardState(),
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		var err error
		repo, err = New(logrus.New(), Config{
			MemtablesFlushIdleAfter:   60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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
				IndexTimestamps: true,
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

		migrator := NewMigrator(repo, repo.logger)
		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass, schemaGetter.shardState)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
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
		schemaGetter = &fakeSchemaGetter{
			shardState: singleShardState(),
			schema: schema.Schema{
				Objects: &models.Schema{},
			},
		}
		var err error
		repo, err = New(logrus.New(), Config{
			MemtablesFlushIdleAfter:   60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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

		migrator := NewMigrator(repo, repo.logger)
		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
		err = migrator.AddClass(context.Background(), refClass, schemaGetter.shardState)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
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
	err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
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
		SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
		ClassName:    class.Class,
		Pagination:   &filters.Pagination{Limit: 5},
		Filters:      LengthFilter,
	}
	_, err = repo.Search(context.Background(), params)
	require.NotNil(t, err)
}
