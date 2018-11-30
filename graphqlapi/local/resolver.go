package local

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
)

// Resolving local GraphQL queries
type Resolver interface {
	local_get.Resolver
}
