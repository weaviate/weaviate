/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package traverser

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ExploreConcepts(t *testing.T) {
	t.Run("with no movements set", func(t *testing.T) {

		authorizer := &fakeAuthorizer{}
		repo := &fakeRepo{}
		locks := &fakeLocks{}
		c11y := &fakeC11y{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		traverser := NewTraverser(locks, repo, c11y, logger, authorizer,
			vectorizer, vectorSearcher)
		params := ExploreParams{
			Values: []string{"a search term", "another"},
		}
		vectorSearcher.results = []VectorSearchResult{
			VectorSearchResult{
				ClassName: "BestClass",
				Kind:      kind.Thing,
				ID:        "123-456-789",
			},
			VectorSearchResult{
				ClassName: "AnAction",
				Kind:      kind.Action,
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []VectorSearchResult{
			VectorSearchResult{
				ClassName: "BestClass",
				Kind:      kind.Thing,
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/things/123-456-789",
			},
			VectorSearchResult{
				ClassName: "AnAction",
				Kind:      kind.Action,
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/actions/987-654-321",
			},
		}, res)

		assert.Equal(t, []float32{1, 2, 3}, vectorSearcher.calledWithVector)
		assert.Equal(t, 20, vectorSearcher.calledWithLimit,
			"uses the default limit if not explicitly set")
	})

	t.Run("with movements set", func(t *testing.T) {

		authorizer := &fakeAuthorizer{}
		repo := &fakeRepo{}
		locks := &fakeLocks{}
		c11y := &fakeC11y{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorSearcher := &fakeVectorSearcher{}
		traverser := NewTraverser(locks, repo, c11y, logger, authorizer,
			vectorizer, vectorSearcher)
		params := ExploreParams{
			Limit:  100,
			Values: []string{"a search term", "another"},
			MoveTo: ExploreMove{
				Values: []string{"foo"},
				Force:  0.7,
			},
			MoveAwayFrom: ExploreMove{
				Values: []string{"bar"},
				Force:  0.7,
			},
		}
		vectorSearcher.results = []VectorSearchResult{
			VectorSearchResult{
				ClassName: "BestClass",
				Kind:      kind.Thing,
				ID:        "123-456-789",
			},
			VectorSearchResult{
				ClassName: "AnAction",
				Kind:      kind.Action,
				ID:        "987-654-321",
			},
		}

		res, err := traverser.Explore(context.Background(), nil, params)
		require.Nil(t, err)
		assert.Equal(t, []VectorSearchResult{
			VectorSearchResult{
				ClassName: "BestClass",
				Kind:      kind.Thing,
				ID:        "123-456-789",
				Beacon:    "weaviate://localhost/things/123-456-789",
			},
			VectorSearchResult{
				ClassName: "AnAction",
				Kind:      kind.Action,
				ID:        "987-654-321",
				Beacon:    "weaviate://localhost/actions/987-654-321",
			},
		}, res)

		// see dummy implemenation of MoveTo and MoveAway for why the vector should
		// be the way it is
		assert.Equal(t, []float32{1.5, 2.5, 3.5}, vectorSearcher.calledWithVector)
		assert.Equal(t, 100, vectorSearcher.calledWithLimit,
			"limit explicitly set")
	})
}
