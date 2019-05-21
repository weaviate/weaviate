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
 */package kinds

import (
	"context"

	contextionary "github.com/semi-technologies/weaviate/contextionary/schema"
	"github.com/sirupsen/logrus"
)

// Traverser can be used to dynamically traverse the knowledge graph
type Traverser struct {
	locks                 locks
	repo                  TraverserRepo
	contextionaryProvider c11yProvider
	logger                logrus.FieldLogger
	authorizer            authorizer
}

// NewTraverser to traverse the knowledge graph
func NewTraverser(locks locks, repo TraverserRepo, c11y c11yProvider,
	logger logrus.FieldLogger, authorizer authorizer) *Traverser {
	return &Traverser{
		locks:                 locks,
		contextionaryProvider: c11y,
		repo:                  repo,
		logger:                logger,
		authorizer:            authorizer,
	}
}

// TraverserRepo describes the dependencies of the Traverser UC to the
// connected database
type TraverserRepo interface {
	LocalGetClass(context.Context, *LocalGetParams) (interface{}, error)
	LocalGetMeta(context.Context, *GetMetaParams) (interface{}, error)
	LocalAggregate(context.Context, *AggregateParams) (interface{}, error)
	LocalFetchKindClass(context.Context, *FetchParams) (interface{}, error)
	LocalFetchFuzzy(context.Context, []string) (interface{}, error)
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
