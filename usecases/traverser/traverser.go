//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/schema"
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
	config         *config.WeaviateConfig
	locks          locks
	logger         logrus.FieldLogger
	authorizer     authorizer
	vectorizer     CorpiVectorizer
	vectorSearcher VectorSearcher
	explorer       explorer
	schemaGetter   schema.SchemaGetter
}

type CorpiVectorizer interface {
	Corpi(ctx context.Context, corpi []string) ([]float32, error)
	MoveTo(source []float32, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source []float32, target []float32, weight float32) ([]float32, error)
	NormalizedDistance(a, b []float32) (float32, error)
}

type VectorSearcher interface {
	VectorSearch(ctx context.Context, vector []float32,
		limit int, filters *filters.LocalFilter) ([]search.Result, error)
	Aggregate(ctx context.Context, params AggregateParams) (*aggregation.Result, error)
}

type explorer interface {
	GetClass(ctx context.Context, params GetParams) ([]interface{}, error)
	Concepts(ctx context.Context, params ExploreParams) ([]search.Result, error)
}

// NewTraverser to traverse the knowledge graph
func NewTraverser(config *config.WeaviateConfig, locks locks,
	logger logrus.FieldLogger, authorizer authorizer,
	vectorizer CorpiVectorizer, vectorSearcher VectorSearcher,
	explorer explorer, schemaGetter schema.SchemaGetter) *Traverser {
	return &Traverser{
		config:         config,
		locks:          locks,
		logger:         logger,
		authorizer:     authorizer,
		vectorizer:     vectorizer,
		vectorSearcher: vectorSearcher,
		explorer:       explorer,
		schemaGetter:   schemaGetter,
	}
}

// TraverserRepo describes the dependencies of the Traverser UC to the
// connected database
type TraverserRepo interface {
	GetClass(context.Context, *GetParams) (interface{}, error)
	Aggregate(context.Context, *AggregateParams) (interface{}, error)
}

// c11y is a local abstraction on the contextionary that needs to be
// provided to the graphQL API in order to resolve Local.Fetch queries.
type c11y interface {
	SchemaSearch(ctx context.Context, p SearchParams) (SearchResults, error)
	SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error)
}

// SearchResult is a single search result. See wrapping Search Results for the Type
type SearchResult struct {
	Name      string
	Kind      kind.Kind
	Certainty float32
}

// SearchResults is grouping of SearchResults for a SchemaSearch
type SearchResults struct {
	Type    SearchType
	Results []SearchResult
}

// Len of the result set
func (r SearchResults) Len() int {
	return len(r.Results)
}
