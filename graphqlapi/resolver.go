package graphqlapi

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
)

// This type can resolve any GraphQL query.
// It is the aggregation of all resolver interfaces.
type Resolver interface {
	local.Resolver
}
