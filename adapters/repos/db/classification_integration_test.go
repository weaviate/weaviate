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
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/classification"
)

func TestClassifications(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
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

	t.Run("importing classification schema", func(t *testing.T) {
		for _, class := range classificationTestSchema() {
			err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
			require.Nil(t, err)
		}
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: classificationTestSchema()}}

	t.Run("importing categories", func(t *testing.T) {
		for _, res := range classificationTestCategories() {
			thing := res.Object()
			err := repo.PutObject(context.Background(), thing, res.Vector, nil)
			require.Nil(t, err)
		}
	})

	t.Run("importing articles", func(t *testing.T) {
		for _, res := range classificationTestArticles() {
			thing := res.Object()
			err := repo.PutObject(context.Background(), thing, res.Vector, nil)
			require.Nil(t, err)
		}
	})

	t.Run("finding all unclassified (no filters)", func(t *testing.T) {
		res, err := repo.GetUnclassified(context.Background(),
			"Article", []string{"exactCategory", "mainCategory"}, nil)
		require.Nil(t, err)
		require.Len(t, res, 6)
	})

	t.Run("finding all unclassified (with filters)", func(t *testing.T) {
		filter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Property: "description",
				},
				Value: &filters.Value{
					Value: "johnny",
					Type:  schema.DataTypeText,
				},
			},
		}

		res, err := repo.GetUnclassified(context.Background(),
			"Article", []string{"exactCategory", "mainCategory"}, filter)
		require.Nil(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, strfmt.UUID("a2bbcbdc-76e1-477d-9e72-a6d2cfb50109"), res[0].ID)
	})

	t.Run("aggregating over item neighbors", func(t *testing.T) {
		t.Run("close to politics (no filters)", func(t *testing.T) {
			res, err := repo.AggregateNeighbors(context.Background(),
				[]float32{0.7, 0.01, 0.01}, "Article",
				[]string{"exactCategory", "mainCategory"}, 1, nil)

			expectedRes := []classification.NeighborRef{
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idCategoryPolitics)),
					Property:     "exactCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.00010201335,
						ClosestWinningDistance: 0.00010201335,
						ClosestOverallDistance: 0.00010201335,
					},
				},
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idMainCategoryPoliticsAndSociety)),
					Property:     "mainCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.00010201335,
						ClosestWinningDistance: 0.00010201335,
						ClosestOverallDistance: 0.00010201335,
					},
				},
			}

			require.Nil(t, err)
			assert.ElementsMatch(t, expectedRes, res)
		})

		t.Run("close to food and drink (no filters)", func(t *testing.T) {
			res, err := repo.AggregateNeighbors(context.Background(),
				[]float32{0.01, 0.01, 0.66}, "Article",
				[]string{"exactCategory", "mainCategory"}, 1, nil)

			expectedRes := []classification.NeighborRef{
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idCategoryFoodAndDrink)),
					Property:     "exactCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.00011473894,
						ClosestWinningDistance: 0.00011473894,
						ClosestOverallDistance: 0.00011473894,
					},
				},
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idMainCategoryFoodAndDrink)),
					Property:     "mainCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.00011473894,
						ClosestWinningDistance: 0.00011473894,
						ClosestOverallDistance: 0.00011473894,
					},
				},
			}

			require.Nil(t, err)
			assert.ElementsMatch(t, expectedRes, res)
		})

		t.Run("close to food and drink (but limiting to politics through filter)", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Property: "description",
					},
					Value: &filters.Value{
						Value: "politics",
						Type:  schema.DataTypeText,
					},
					Operator: filters.OperatorEqual,
				},
			}
			res, err := repo.AggregateNeighbors(context.Background(),
				[]float32{0.01, 0.01, 0.66}, "Article",
				[]string{"exactCategory", "mainCategory"}, 1, filter)

			expectedRes := []classification.NeighborRef{
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idCategoryPolitics)),
					Property:     "exactCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.49242598,
						ClosestWinningDistance: 0.49242598,
						ClosestOverallDistance: 0.49242598,
					},
				},
				{
					Beacon:       strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idMainCategoryPoliticsAndSociety)),
					Property:     "mainCategory",
					OverallCount: 1,
					WinningCount: 1,
					LosingCount:  0,
					Distances: classification.NeighborRefDistances{
						MeanWinningDistance:    0.49242598,
						ClosestWinningDistance: 0.49242598,
						ClosestOverallDistance: 0.49242598,
					},
				},
			}

			require.Nil(t, err)
			assert.ElementsMatch(t, expectedRes, res)
		})
	})
}

// test fixtures
func classificationTestSchema() []*models.Class {
	return []*models.Class{
		{
			Class:               "ExactCategory",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		},
		{
			Class:               "MainCategory",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		},
		{
			Class:               "Article",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "description",
					DataType:     []string{string(schema.DataTypeText)},
					Tokenization: "word",
				},
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "exactCategory",
					DataType: []string{"ExactCategory"},
				},
				{
					Name:     "mainCategory",
					DataType: []string{"MainCategory"},
				},
			},
		},
	}
}

const (
	idMainCategoryPoliticsAndSociety = "39c6abe3-4bbe-4c4e-9e60-ca5e99ec6b4e"
	idMainCategoryFoodAndDrink       = "5a3d909a-4f0d-4168-8f5c-cd3074d1e79a"
	idCategoryPolitics               = "1b204f16-7da6-44fd-bbd2-8cc4a7414bc3"
	idCategorySociety                = "ec500f39-1dc9-4580-9bd1-55a8ea8e37a2"
	idCategoryFoodAndDrink           = "027b708a-31ca-43ea-9001-88bec864c79c"
)

func beaconRef(target string) *models.SingleRef {
	beacon := fmt.Sprintf("weaviate://localhost/%s", target)
	return &models.SingleRef{Beacon: strfmt.URI(beacon)}
}

func classificationTestCategories() search.Results {
	// using search.Results, because it's the perfect grouping of object and
	// vector
	return search.Results{
		// exact categories
		search.Result{
			ID:        idCategoryPolitics,
			ClassName: "ExactCategory",
			Vector:    []float32{1, 0, 0},
			Schema: map[string]interface{}{
				"name": "Politics",
			},
		},
		search.Result{
			ID:        idCategorySociety,
			ClassName: "ExactCategory",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"name": "Society",
			},
		},
		search.Result{
			ID:        idCategoryFoodAndDrink,
			ClassName: "ExactCategory",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"name": "Food and Drink",
			},
		},

		// main categories
		search.Result{
			ID:        idMainCategoryPoliticsAndSociety,
			ClassName: "MainCategory",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"name": "Politics and Society",
			},
		},
		search.Result{
			ID:        idMainCategoryFoodAndDrink,
			ClassName: "MainCategory",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"name": "Food and Drink",
			},
		},
	}
}

func classificationTestArticles() search.Results {
	// using search.Results, because it's the perfect grouping of object and
	// vector
	return search.Results{
		// classified
		search.Result{
			ID:        "8aeecd06-55a0-462c-9853-81b31a284d80",
			ClassName: "Article",
			Vector:    []float32{1, 0, 0},
			Schema: map[string]interface{}{
				"description":   "This article talks about politics",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryPolitics)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "9f4c1847-2567-4de7-8861-34cf47a071ae",
			ClassName: "Article",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"description":   "This articles talks about society",
				"exactCategory": models.MultipleRef{beaconRef(idCategorySociety)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "926416ec-8fb1-4e40-ab8c-37b226b3d68e",
			ClassName: "Article",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"description":   "This article talks about food",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryFoodAndDrink)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryFoodAndDrink)},
			},
		},

		// unclassified
		search.Result{
			ID:        "75ba35af-6a08-40ae-b442-3bec69b355f9",
			ClassName: "Article",
			Vector:    []float32{0.78, 0, 0},
			Schema: map[string]interface{}{
				"description": "Barack Obama is a former US president",
			},
		},
		search.Result{
			ID:        "f850439a-d3cd-4f17-8fbf-5a64405645cd",
			ClassName: "Article",
			Vector:    []float32{0.90, 0, 0},
			Schema: map[string]interface{}{
				"description": "Michelle Obama is Barack Obamas wife",
			},
		},
		search.Result{
			ID:        "a2bbcbdc-76e1-477d-9e72-a6d2cfb50109",
			ClassName: "Article",
			Vector:    []float32{0, 0.78, 0},
			Schema: map[string]interface{}{
				"description": "Johnny Depp is an actor",
			},
		},
		search.Result{
			ID:        "069410c3-4b9e-4f68-8034-32a066cb7997",
			ClassName: "Article",
			Vector:    []float32{0, 0.90, 0},
			Schema: map[string]interface{}{
				"description": "Brad Pitt starred in a Quentin Tarantino movie",
			},
		},
		search.Result{
			ID:        "06a1e824-889c-4649-97f9-1ed3fa401d8e",
			ClassName: "Article",
			Vector:    []float32{0, 0, 0.78},
			Schema: map[string]interface{}{
				"description": "Ice Cream often contains a lot of sugar",
			},
		},
		search.Result{
			ID:        "6402e649-b1e0-40ea-b192-a64eab0d5e56",
			ClassName: "Article",
			Vector:    []float32{0, 0, 0.90},
			Schema: map[string]interface{}{
				"description": "French Fries are more common in Belgium and the US than in France",
			},
		},
	}
}
