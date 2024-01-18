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

package traverser

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_ExploreConcepts(t *testing.T) {
	t.Run("without any near searchers", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{}

		_, err := traverser.Explore(context.Background(), nil, params)
		assert.Contains(t, err.Error(), "received no search params")
	})

	t.Run("with two searchers set at the same time", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, nil, nil, -1)
		params := ExploreParams{
			NearVector: &searchparams.NearVector{},
			ModuleParams: map[string]interface{}{
				"nearCustomText": nil,
			},
		}

		_, err := traverser.Explore(context.Background(), nil, params)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})
	t.Run("nearCustomText with no movements set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"a search term", "another"},
				}),
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearCustomText", 128)

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/BestClass/123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/AnAction/987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		assert.Equal(t, []float32{1, 2, 3}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearCustomText without optional params", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, nil, nil, -1)
		params := ExploreParams{
			NearVector: &searchparams.NearVector{
				Vector: []float32{7.8, 9},
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearVector", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/BestClass/123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/AnAction/987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		assert.Equal(t, []float32{7.8, 9}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearObject with id param", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, nil, nil, -1)
		params := ExploreParams{
			NearObject: &searchparams.NearObject{
				ID: "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
		}
		searchRes := search.Result{
			ClassName: "BestClass",
			ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
		}
		vectorSearcher.
			On("ObjectsByID", strfmt.UUID("bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a")).
			Return(search.Results{searchRes}, nil)
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearObject", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Beacon:    "weaviate://localhost/BestClass/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Beacon:    "weaviate://localhost/AnAction/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearObject with beacon param", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, nil, nil, -1)
		params := ExploreParams{
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
		}
		searchRes := search.Result{
			ClassName: "BestClass",
			ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
		}
		vectorSearcher.
			On("ObjectsByID", strfmt.UUID("bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a")).
			Return(search.Results{searchRes}, nil)
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearObject", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Beacon:    "weaviate://localhost/BestClass/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Beacon:    "weaviate://localhost/AnAction/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearCustomText with limit and distance set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			Limit: 100,
			NearVector: &searchparams.NearVector{
				Vector:       []float32{7.8, 9},
				Distance:     0.2,
				WithDistance: true,
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Dist:      0.4,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Dist:      0.4,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearVector", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{}, res) // certainty not matched

		assert.Equal(t, []float32{7.8, 9}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearCustomText with limit and certainty set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			Limit: 100,
			NearVector: &searchparams.NearVector{
				Vector:    []float32{7.8, 9},
				Certainty: 0.8,
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearVector", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{}, res) // certainty not matched

		assert.Equal(t, []float32{7.8, 9}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearCustomText with minimum distance set to 0.4", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"a search term", "another"},
					"distance": float64(0.4),
				}),
			},
		}
		vectorSearcher.results = []search.Result{}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{}, res, "empty result because distance is not met")
		assert.Equal(t, []float32{1, 2, 3}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearCustomText with minimum certainty set to 0.6", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts":  []interface{}{"a search term", "another"},
					"certainty": float64(0.6),
				}),
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearCustomText", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{}, res, "empty result because certainty is not met")
		assert.Equal(t, []float32{1, 2, 3}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("near text with movements set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)
		params := ExploreParams{
			Limit: 100,
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"a search term", "another"},
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"foo"},
						"force":    float64(0.7),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"bar"},
						"force":    float64(0.7),
					},
				}),
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearCustomText", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/BestClass/123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/AnAction/987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		// see dummy implementation of MoveTo and MoveAway for why the vector should
		// be the way it is
		assert.Equal(t, []float32{1.5, 2.5, 3.5}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"limit explicitly set")
	})

	t.Run("near text with movements and objects set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(vectorSearcher, log, getFakeModulesProvider(), metrics, defaultConfig)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorSearcher, explorer, schemaGetter, getFakeModulesProvider(), nil, -1)

		params := ExploreParams{
			Limit: 100,
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"a search term", "another"},
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"foo"},
						"force":    float64(0.7),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"bar"},
						"force":    float64(0.7),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "e9c12c22-766f-4bde-b140-d4cf8fd6e042",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e043",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e044",
							},
						},
					},
				}),
			},
		}
		vectorSearcher.results = []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}
		searchRes1 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			Dims:      128,
		}
		searchRes2 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e042",
			Dims:      128,
		}
		searchRes3 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e043",
			Dims:      128,
		}
		searchRes4 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e044",
			Dims:      128,
		}

		vectorSearcher.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
			Return(&searchRes1, nil)
		vectorSearcher.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e042")).
			Return(&searchRes2, nil)
		vectorSearcher.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e043")).
			Return(&searchRes3, nil)
		vectorSearcher.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e044")).
			Return(&searchRes4, nil)

		metrics.On("AddUsageDimensions", "n/a", "explore_graphql", "nearCustomText", 128)
		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/BestClass/123-456-789",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
			{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/AnAction/987-654-321",
				Certainty: 0.5,
				Dist:      0.5,
				Dims:      128,
			},
		}, res)

		// see dummy implementation of MoveTo and MoveAway for why the vector should
		// be the way it is
		assert.Equal(t, []float32{1.5, 2.5, 3.5}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"limit explicitly set")
	})
}
