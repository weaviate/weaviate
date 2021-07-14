//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MultiShardJourneys(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}()

	logger, _ := test.NewNullLogger()
	class := &models.Class{
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "TestClass",
		Properties: []*models.Property{
			{
				Name:     "boolProp",
				DataType: []string{string(schema.DataTypeBoolean)},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: multiShardState()}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	data := multiShardTestData()
	queryVec := exampleQueryVec()
	groundTruth := bruteForceObjectsByQuery(data, queryVec)

	t.Run("import all individually", func(t *testing.T) {
		for _, obj := range data {
			require.Nil(t, repo.PutObject(context.Background(), obj, obj.Vector))
		}
	})

	t.Run("retrieve all individually", func(t *testing.T) {
		for _, desired := range data {
			res, err := repo.ObjectByID(context.Background(), desired.ID,
				traverser.SelectProperties{}, traverser.AdditionalProperties{})
			assert.Nil(t, err)

			assert.Equal(t, desired.Properties.(map[string]interface{})["boolProp"].(bool),
				res.Object().Properties.(map[string]interface{})["boolProp"].(bool))
			assert.Equal(t, desired.ID, res.Object().ID)
		}
	})

	t.Run("retrieve through filter (object search)", func(t *testing.T) {
		do := func(limit, expected int) {
			filters := &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					Value: &filters.Value{
						Value: true,
						Type:  schema.DataTypeBoolean,
					},
					On: &filters.Path{
						Property: "boolProp",
					},
				},
			}
			res, err := repo.ObjectSearch(context.Background(), limit, filters,
				traverser.AdditionalProperties{})
			assert.Nil(t, err)

			assert.Len(t, res, expected)
			for _, obj := range res {
				assert.Equal(t, true, obj.Schema.(map[string]interface{})["boolProp"].(bool))
			}
		}

		t.Run("with high limit", func(t *testing.T) {
			do(100, 10)
		})

		t.Run("with low limit", func(t *testing.T) {
			do(3, 3)
		})
	})

	t.Run("retrieve through filter (class search)", func(t *testing.T) {
		do := func(limit, expected int) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					Value: &filters.Value{
						Value: true,
						Type:  schema.DataTypeBoolean,
					},
					On: &filters.Path{
						Property: "boolProp",
					},
				},
			}
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters: filter,
				Pagination: &filters.Pagination{
					Limit: limit,
				},
				ClassName: "TestClass",
			})
			assert.Nil(t, err)

			assert.Len(t, res, expected)
			for _, obj := range res {
				assert.Equal(t, true, obj.Schema.(map[string]interface{})["boolProp"].(bool))
			}
		}

		t.Run("with high limit", func(t *testing.T) {
			do(100, 10)
		})

		t.Run("with low limit", func(t *testing.T) {
			do(3, 3)
		})
	})

	t.Run("retrieve through class-level vector search", func(t *testing.T) {
		do := func(t *testing.T, limit, expected int) {
			res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
				SearchVector: queryVec,
				Pagination: &filters.Pagination{
					Limit: limit,
				},
				ClassName: "TestClass",
			})
			assert.Nil(t, err)
			assert.Len(t, res, expected)
			for i, obj := range res {
				assert.Equal(t, groundTruth[i].ID, obj.ID)
			}
		}

		t.Run("with high limit", func(t *testing.T) {
			do(t, 100, 20)
		})

		t.Run("with low limit", func(t *testing.T) {
			do(t, 3, 3)
		})
	})

	t.Run("retrieve through inter-class vector search", func(t *testing.T) {
		do := func(t *testing.T, limit, expected int) {
			res, err := repo.VectorSearch(context.Background(), queryVec, limit, nil)
			assert.Nil(t, err)
			assert.Len(t, res, expected)
			for i, obj := range res {
				assert.Equal(t, groundTruth[i].ID, obj.ID)
			}
		}

		t.Run("with high limit", func(t *testing.T) {
			do(t, 100, 20)
		})

		t.Run("with low limit", func(t *testing.T) {
			do(t, 3, 3)
		})
	})
}

func exampleQueryVec() []float32 {
	dim := 10
	vec := make([]float32, dim)
	for j := range vec {
		vec[j] = rand.Float32()
	}
	return vec
}

func multiShardTestData() []*models.Object {
	size := 20
	dim := 10
	out := make([]*models.Object, size)
	for i := range out {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}

		out[i] = &models.Object{
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  "TestClass",
			Vector: vec,
			Properties: map[string]interface{}{
				"boolProp": i%2 == 0,
			},
		}
	}

	return out
}

func bruteForceObjectsByQuery(objs []*models.Object,
	query []float32) []*models.Object {
	type distanceAndObj struct {
		distance float32
		obj      *models.Object
	}

	distances := make([]distanceAndObj, len(objs))

	for i := range objs {
		dist := 1 - asm.Dot(normalize(query), normalize(objs[i].Vector))
		distances[i] = distanceAndObj{
			distance: dist,
			obj:      objs[i],
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]*models.Object, len(objs))
	for i := range out {
		out[i] = distances[i].obj
	}

	return out
}

func normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}
