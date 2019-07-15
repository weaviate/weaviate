package traverser

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_Explorer_GetClass(t *testing.T) {
	t.Run("when a where filter is set", func(t *testing.T) {
		// TODO: gh-911, replace this with actual functionality
		explorer := NewExplorer(nil, nil, nil)

		params := &LocalGetParams{
			Explore: &ExploreParams{
				Values: []string{"foo"},
			},
			Filters: &filters.LocalFilter{},
		}

		_, err := explorer.GetClass(context.Background(), params)
		msg := "combining 'explore' and 'where' parameters not possible yet - coming soon!"
		assert.Equal(t, errors.New(msg), err)
	})

	t.Run("when an explore param is set", func(t *testing.T) {
		params := &LocalGetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values: []string{"foo"},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []VectorSearchResult{
			{
				Kind: kind.Thing,
				ID:   "id1",
			},
			{
				Kind: kind.Action,
				ID:   "id2",
			},
		}

		thing := models.Thing{
			Schema: map[string]interface{}{
				"name": "Foo",
			},
		}
		action := models.Action{
			Schema: map[string]interface{}{
				"age": 200,
			},
		}

		search := &fakeVectorClassSearch{}
		vectorizer := &fakeVectorizer{}
		repo := &fakeExplorerRepo{}
		explorer := NewExplorer(search, vectorizer, repo)
		search.
			On("VectorClassSearch", kind.Thing, "BestClass", []float32{1, 2, 3},
				100, (*filters.LocalFilter)(nil)).
			Return(searchResults, nil)
		repo.On("GetThing", strfmt.UUID("id1")).Return(thing, nil)
		repo.On("GetAction", strfmt.UUID("id2")).Return(action, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("connected repo must be called once for each result", func(t *testing.T) {
			// TODO gh-912 improve interface between connector and UC
			repo.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
				}, res[1])
		})
	})

	t.Run("when an explore param is set and the required certainty not met", func(t *testing.T) {
		params := &LocalGetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values:    []string{"foo"},
				Certainty: 0.8,
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []VectorSearchResult{
			{
				Kind: kind.Thing,
				ID:   "id1",
			},
			{
				Kind: kind.Action,
				ID:   "id2",
			},
		}

		search := &fakeVectorClassSearch{}
		vectorizer := &fakeVectorizer{}
		repo := &fakeExplorerRepo{}
		explorer := NewExplorer(search, vectorizer, repo)
		search.
			On("VectorClassSearch", kind.Thing, "BestClass", []float32{1, 2, 3},
				100, (*filters.LocalFilter)(nil)).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("connected repo must never be called", func(t *testing.T) {
			// TODO gh-912 improve interface between connector and UC
			// note that we have not set up any expected calls!
			repo.AssertExpectations(t)
		})

		t.Run("no concept met the required certainty", func(t *testing.T) {
			assert.Len(t, res, 0)
		})
	})
}

type fakeVectorClassSearch struct {
	mock.Mock
}

func (f *fakeVectorClassSearch) VectorClassSearch(ctx context.Context,
	kind kind.Kind, className string, vector []float32, limit int,
	filters *filters.LocalFilter) ([]VectorSearchResult, error) {
	args := f.Called(kind, className, vector, limit, filters)
	return args.Get(0).([]VectorSearchResult), args.Error(1)
}

type fakeExplorerRepo struct {
	mock.Mock
}

func (f *fakeExplorerRepo) GetThing(ctx context.Context, uuid strfmt.UUID,
	res *models.Thing) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Thing)
	return args.Error(1)
}

func (f *fakeExplorerRepo) GetAction(ctx context.Context, uuid strfmt.UUID,
	res *models.Action) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Action)
	return args.Error(1)
}
