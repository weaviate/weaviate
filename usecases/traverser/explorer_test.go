//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Explorer_GetClass(t *testing.T) {
	t.Run("when an explore param is set", func(t *testing.T) {
		params := GetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values: []string{"foo"},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log)
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
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
		params := GetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values:    []string{"foo"},
				Certainty: 0.8,
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
			},
			{
				Kind: kind.Action,
				ID:   "id2",
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()

		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log)
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("no concept met the required certainty", func(t *testing.T) {
			assert.Len(t, res, 0)
		})
	})

	t.Run("when no explore param is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log)
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("ClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
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
}

func newFakeDistancer() func(a, b []float32) (float32, error) {
	return func(source, target []float32) (float32, error) {
		return 0.5, nil
	}
}
