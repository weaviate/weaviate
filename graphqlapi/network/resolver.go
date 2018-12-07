package network

import (
	networkget "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
)

// Resolver can resolve any network query
type Resolver interface {
	networkget.Resolver
}
