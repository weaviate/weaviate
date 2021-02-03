//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ExploreConcepts(t *testing.T) {
	t.Run("without any near searchers", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{}

		_, err := traverser.Explore(context.Background(), nil, params)
		assert.Contains(t, err.Error(), "received no search params")
	})

	t.Run("with two searchers set at the same time", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearText:   &NearTextParams{},
			NearVector: &NearVectorParams{},
		}

		_, err := traverser.Explore(context.Background(), nil, params)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})
	t.Run("nearText with no movements set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearText: &NearTextParams{
				Values: []string{"a search term", "another"},
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/123-456-789",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/987-654-321",
				Certainty: 0.5,
			},
		}, res)

		assert.Equal(t, []float32{1, 2, 3}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearText without optional params", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearVector: &NearVectorParams{
				Vector: []float32{7.8, 9},
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/123-456-789",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/987-654-321",
				Certainty: 0.5,
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
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearObject: &NearObjectParams{
				ID: "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
		}
		searchRes := search.Result{
			ClassName: "BestClass",
			ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
		}
		vectorSearcher.
			On("ObjectByID", strfmt.UUID("bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a")).
			Return(&searchRes, nil)
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Beacon:    "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Beacon:    "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
			},
		}, res)

		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearObject with beacon param", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearObject: &NearObjectParams{
				Beacon: "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
		}
		searchRes := search.Result{
			ClassName: "BestClass",
			ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
		}
		vectorSearcher.
			On("ObjectByID", strfmt.UUID("bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a")).
			Return(&searchRes, nil)
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Beacon:    "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23a",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Beacon:    "weaviate://localhost/bd3d1560-3f0e-4b39-9d62-38b4a3c4f23b",
				Certainty: 0.5,
			},
		}, res)

		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearText with limit and certainty set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			Limit: 100,
			NearVector: &NearVectorParams{
				Vector:    []float32{7.8, 9},
				Certainty: 0.8,
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{}, res) // certainty not matched

		assert.Equal(t, []float32{7.8, 9}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("nearText with minimum certainty set to 0.6", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			NearText: &NearTextParams{
				Values:    []string{"a search term", "another"},
				Certainty: 0.6,
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}

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
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)
		params := ExploreParams{
			Limit: 100,
			NearText: &NearTextParams{
				Values: []string{"a search term", "another"},
				MoveTo: ExploreMove{
					Values: []string{"foo"},
					Force:  0.7,
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"bar"},
					Force:  0.7,
				},
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/123-456-789",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/987-654-321",
				Certainty: 0.5,
			},
		}, res)

		// see dummy implemenation of MoveTo and MoveAway for why the vector should
		// be the way it is
		assert.Equal(t, []float32{1.5, 2.5, 3.5}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"limit explicitly set")
	})

	t.Run("near text with movements and objects set", func(t *testing.T) {
		authorizer := &fakeAuthorizer{}
		locks := &fakeLocks{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(vectorSearcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		schemaGetter := &fakeSchemaGetter{}
		traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
			vectorizer, vectorSearcher, explorer, schemaGetter)

		params := ExploreParams{
			Limit: 100,
			NearText: &NearTextParams{
				Values: []string{"a search term", "another"},
				MoveTo: ExploreMove{
					Values: []string{"foo"},
					Force:  0.7,
					Objects: []ObjectMove{
						{ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e041"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"bar"},
					Force:  0.7,
					Objects: []ObjectMove{
						{ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e042"},
						{Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e043"},
						{Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e044"},
					},
				},
			},
		}
		vectorSearcher.results = []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
			},
		}
		searchRes1 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
		}
		searchRes2 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e042",
		}
		searchRes3 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e043",
		}
		searchRes4 := search.Result{
			ClassName: "BestClass",
			ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e044",
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

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []search.Result{
			search.Result{
				ClassName: "BestClass",
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/123-456-789",
				Certainty: 0.5,
			},
			search.Result{
				ClassName: "AnAction",
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/987-654-321",
				Certainty: 0.5,
			},
		}, res)

		// see dummy implemenation of MoveTo and MoveAway for why the vector should
		// be the way it is
		assert.Equal(t, []float32{1.5, 2.5, 3.5}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"limit explicitly set")
	})
}
