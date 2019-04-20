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
 */package kinds

import (
	contextionary "github.com/creativesoftwarefdn/weaviate/contextionary/schema"
)

// Traverser can be used to dynamically traverse the knowledge graph
type Traverser struct {
	locks                 locks
	repo                  TraverserRepo
	contextionaryProvider c11yProvider
}

// NewTraverser to traverse the knowledge graph
func NewTraverser(locks locks, repo TraverserRepo, c11y c11yProvider) *Traverser {
	return &Traverser{
		locks:                 locks,
		contextionaryProvider: c11y,
		repo:                  repo,
	}
}

// TraverserRepo describes the dependencies of the Traverser UC to the
// connected database
type TraverserRepo interface {
	LocalGetClass(*LocalGetParams) (interface{}, error)
	LocalGetMeta(*GetMetaParams) (interface{}, error)
	LocalAggregate(*AggregateParams) (interface{}, error)
	LocalFetchKindClass(*FetchParams) (interface{}, error)
	LocalFetchFuzzy([]string) (interface{}, error)
}

type c11yProvider interface {
	GetSchemaContextionary() *contextionary.Contextionary
}

// c11y is a local abstraction on the contextionary that needs to be
// provided to the graphQL API in order to resolve Local.Fetch queries.
type c11y interface {
	SchemaSearch(p contextionary.SearchParams) (contextionary.SearchResults, error)
	SafeGetSimilarWordsWithCertainty(word string, certainty float32) []string
}
