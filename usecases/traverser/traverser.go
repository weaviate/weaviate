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
 */

package traverser

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/sirupsen/logrus"
)

type locks interface {
	LockConnector() (func() error, error)
	LockSchema() (func() error, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

// Traverser can be used to dynamically traverse the knowledge graph
type Traverser struct {
	locks          locks
	repo           TraverserRepo
	c11y           c11y
	logger         logrus.FieldLogger
	authorizer     authorizer
	vectorizer     corpiVectorizer
	vectorSearcher vectorSearcher
}

type corpiVectorizer interface {
	Corpi(ctx context.Context, corpi []string) ([]float32, error)
}

type vectorSearcher interface {
	VectorSearch(ctx context.Context, index string,
		vector []float32) ([]VectorSearchResult, error)
}

// NewTraverser to traverse the knowledge graph
func NewTraverser(locks locks, repo TraverserRepo, c11y c11y,
	logger logrus.FieldLogger, authorizer authorizer,
	vectorizer corpiVectorizer, vectorSearcher vectorSearcher) *Traverser {
	return &Traverser{
		locks:          locks,
		c11y:           c11y,
		repo:           repo,
		logger:         logger,
		authorizer:     authorizer,
		vectorizer:     vectorizer,
		vectorSearcher: vectorSearcher,
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

// c11y is a local abstraction on the contextionary that needs to be
// provided to the graphQL API in order to resolve Local.Fetch queries.
type c11y interface {
	SchemaSearch(ctx context.Context, p SearchParams) (SearchResults, error)
	SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error)
}
