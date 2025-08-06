//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/usecases/cluster"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/entities/search"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRestartJourney(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "Class",
		Properties: []*models.Property{
			{
				Name:         "description",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
		},
	}
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), thingclass))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{thingclass},
		},
	}

	t.Run("import some data", func(t *testing.T) {
		err := repo.PutObject(context.Background(), &models.Object{
			Class: "Class",
			ID:    "9d64350e-5027-40ea-98db-e3b97e6f6f8f",
			Properties: map[string]interface{}{
				"description": "the band is just fantastic that is really what I think",
			},
		}, []float32{0.1, 0.2, 0.3}, nil, nil, nil, 0)
		require.Nil(t, err)

		err = repo.PutObject(context.Background(), &models.Object{
			Class: "Class",
			ID:    "46ebcce8-fb77-413b-ade6-26c427af3f33",
			Properties: map[string]interface{}{
				"description": "oh by the way, which one's pink?",
			},
		}, []float32{-0.1, 0.2, -0.3}, nil, nil, nil, 0)
		require.Nil(t, err)
	})

	t.Run("control", func(t *testing.T) {
		t.Run("verify object by id", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), "46ebcce8-fb77-413b-ade6-26c427af3f33", nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, "oh by the way, which one's pink?",
				res.Schema.(map[string]interface{})["description"])
		})

		t.Run("find object by id through filter", func(t *testing.T) {
			res, err := repo.ObjectSearch(context.Background(), 0, 10,
				&filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: "9d64350e-5027-40ea-98db-e3b97e6f6f8f",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "id",
						},
					},
				}, nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "the band is just fantastic that is really what I think",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through regular inverted index", func(t *testing.T) {
			res, err := repo.ObjectSearch(context.Background(), 0, 10,
				&filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: "pink",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "description",
						},
					},
				}, nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "oh by the way, which one's pink?",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through vector index", func(t *testing.T) {
			res, err := repo.VectorSearch(context.Background(),
				dto.GetParams{
					ClassName: "Class",
					Pagination: &filters.Pagination{
						Limit: 1,
					},
					Properties: search.SelectProperties{{Name: "description"}},
				}, []string{""}, []models.Vector{[]float32{0.05, 0.1, 0.15}})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "the band is just fantastic that is really what I think",
				res[0].Schema.(map[string]interface{})["description"])
		})
	})

	var newRepo *DB
	t.Run("shutdown and recreate", func(t *testing.T) {
		require.Nil(t, repo.Shutdown(context.Background()))
		repo = nil

		newRepo, err = New(logger, "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
		require.Nil(t, err)
		newRepo.SetSchemaGetter(schemaGetter)
		require.Nil(t, newRepo.WaitForStartup(testCtx()))
	})

	t.Run("verify after restart", func(t *testing.T) {
		t.Run("verify object by id", func(t *testing.T) {
			res, err := newRepo.ObjectByID(context.Background(), "46ebcce8-fb77-413b-ade6-26c427af3f33", nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, "oh by the way, which one's pink?",
				res.Schema.(map[string]interface{})["description"])
		})

		t.Run("find object by id through filter", func(t *testing.T) {
			res, err := newRepo.ObjectSearch(context.Background(), 0, 10,
				&filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: "9d64350e-5027-40ea-98db-e3b97e6f6f8f",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "id",
						},
					},
				}, nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "the band is just fantastic that is really what I think",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through regular inverted index", func(t *testing.T) {
			res, err := newRepo.ObjectSearch(context.Background(), 0, 10,
				&filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: "pink",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "description",
						},
					},
				}, nil, additional.Properties{}, "")
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "oh by the way, which one's pink?",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through vector index", func(t *testing.T) {
			res, err := newRepo.VectorSearch(context.Background(),
				dto.GetParams{
					ClassName: "Class",
					Pagination: &filters.Pagination{
						Limit: 1,
					},
					Properties: search.SelectProperties{{Name: "description"}},
				}, []string{""}, []models.Vector{[]float32{0.05, 0.1, 0.15}})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "the band is just fantastic that is really what I think",
				res[0].Schema.(map[string]interface{})["description"])
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		require.Nil(t, newRepo.Shutdown(context.Background()))
	})
}
