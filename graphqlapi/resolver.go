package graphqlapi

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
)

// This type can resolve any GraphQL query.
// It is the aggregation of all resolver interfaces.
type Resolver interface {
	local.Resolver
}


// In practise, we need a read lock on the database to run GraphQL queries.
type ClosingResolver interface {
  Resolver

  // Close the handle of this resolver
  Close()
}

type ResolverProvider interface {
  GetResolver() ClosingResolver
}
