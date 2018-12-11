package local

import (
	local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	local_get_meta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get_meta"
)

// Resolving local GraphQL queries
type Resolver interface {
	local_get.Resolver
	local_get_meta.Resolver
}