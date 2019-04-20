package kinds

import (
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
)

// Traverser can be used to dynamically traverse the knowledge graph
type Traverser struct {
	locks         locks
	repo          TraverserRepo
	contextionary c11y
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

// c11y is a local abstraction on the contextionary that needs to be
// provided to the graphQL API in order to resolve Local.Fetch queries.
type c11y interface {
	SchemaSearch(p contextionary.SearchParams) (contextionary.SearchResults, error)
	SafeGetSimilarWordsWithCertainty(word string, certainty float32) []string
}
