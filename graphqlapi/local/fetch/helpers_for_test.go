/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package fetch

import (
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	testhelper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/stretchr/testify/mock"
)

type mockResolver struct {
	testhelper.MockResolver
}

func newMockResolver(c11y Contextionary) *mockResolver {
	field := Build()
	mocker := &mockResolver{}
	mocker.RootFieldName = "Fetch"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":      Resolver(mocker),
		"Contextionary": c11y,
	}
	return mocker
}

func (m *mockResolver) LocalFetchKindClass(params *Params) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}

func newMockContextionary() *mockContextionary {
	return &mockContextionary{}
}

type mockContextionary struct {
	mock.Mock
}

func (m *mockContextionary) SchemaSearch(p contextionary.SearchParams) (contextionary.SearchResults, error) {
	m.Called(p)
	return contextionary.SearchResults{
		Type: p.SearchType,
		Results: []contextionary.SearchResult{
			{
				Name:      p.Name,
				Certainty: 0.95,
				Kind:      p.Kind,
			},
			{
				Name:      p.Name + "alternative",
				Certainty: 0.85,
				Kind:      p.Kind,
			},
		},
	}, nil
}

func newEmptyContextionary() *emptyContextionary {
	return &emptyContextionary{}
}

type emptyContextionary struct {
	mock.Mock
}

func (m *emptyContextionary) SchemaSearch(p contextionary.SearchParams) (contextionary.SearchResults, error) {
	m.Called(p)
	return contextionary.SearchResults{
		Type:    p.SearchType,
		Results: []contextionary.SearchResult{},
	}, nil
}
