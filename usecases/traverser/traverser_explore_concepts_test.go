package traverser

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ExploreConcepts(t *testing.T) {
	authorizer := &fakeAuthorizer{}
	repo := &fakeRepo{}
	locks := &fakeLocks{}
	c11y := &fakeC11y{}
	logger, _ := test.NewNullLogger()
	vectorizer := &fakeVectorizer{}
	vectorSearcher := &fakeVectorSearcher{}
	traverser := NewTraverser(locks, repo, c11y, logger, authorizer,
		vectorizer, vectorSearcher)
	params := ExploreConceptsParams{
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

	res, err := traverser.ExploreConcepts(context.Background(), nil, params)
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
}
