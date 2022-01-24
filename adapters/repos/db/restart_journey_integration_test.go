//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "Class",
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
	}
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{shardState: shardState}
	repo := New(logger, Config{RootPath: dirName, QueryMaximumResults: 10000}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), thingclass,
				shardState))
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
		},
			[]float32{0.1, 0.2, 0.3})
		require.Nil(t, err)

		err = repo.PutObject(context.Background(), &models.Object{
			Class: "Class",
			ID:    "46ebcce8-fb77-413b-ade6-26c427af3f33",
			Properties: map[string]interface{}{
				"description": "oh by the way, which one's pink?",
			},
		},
			[]float32{-0.1, 0.2, -0.3})
		require.Nil(t, err)
	})

	t.Run("control", func(t *testing.T) {
		t.Run("verify object by id", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(),
				"46ebcce8-fb77-413b-ade6-26c427af3f33", nil, additional.Properties{})
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
							Type:  schema.DataTypeString,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "id",
						},
					},
				}, additional.Properties{})
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
				}, additional.Properties{})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "oh by the way, which one's pink?",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through vector index", func(t *testing.T) {
			res, err := repo.VectorClassSearch(context.Background(),
				traverser.GetParams{
					ClassName:    "Class",
					SearchVector: []float32{0.05, 0.1, 0.15},
					Pagination: &filters.Pagination{
						Limit: 1,
					},
				})
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

		newRepo = New(logger, Config{RootPath: dirName, QueryMaximumResults: 10000}, &fakeRemoteClient{},
			&fakeNodeResolver{})
		newRepo.SetSchemaGetter(schemaGetter)
		err := newRepo.WaitForStartup(testCtx())
		require.Nil(t, err)
	})

	t.Run("verify after restart", func(t *testing.T) {
		t.Run("verify object by id", func(t *testing.T) {
			res, err := newRepo.ObjectByID(context.Background(),
				"46ebcce8-fb77-413b-ade6-26c427af3f33", nil, additional.Properties{})
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
							Type:  schema.DataTypeString,
						},
						On: &filters.Path{
							Class:    "Class",
							Property: "id",
						},
					},
				}, additional.Properties{})
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
				}, additional.Properties{})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "oh by the way, which one's pink?",
				res[0].Schema.(map[string]interface{})["description"])
		})

		t.Run("find object through vector index", func(t *testing.T) {
			res, err := newRepo.VectorClassSearch(context.Background(),
				traverser.GetParams{
					ClassName:    "Class",
					SearchVector: []float32{0.05, 0.1, 0.15},
					Pagination: &filters.Pagination{
						Limit: 1,
					},
				})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, "the band is just fantastic that is really what I think",
				res[0].Schema.(map[string]interface{})["description"])
		})
	})
}
