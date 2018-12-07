package graphqlapi

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
)

// This type can resolve any GraphQL query.
// It is the aggregation of all resolver interfaces.
type Resolver interface {
	local.Resolver
	network.Resolver
}

// In practise, we need a read lock on the database to run GraphQL queries.
type ClosingResolver interface {
	local.Resolver

	// Close the handle of this resolver
	Close()
}

type ResolverProvider interface {
	DatabaseResolverProvider
	NetworkResolverProvider
}

type DatabaseResolverProvider interface {
	GetResolver() ClosingResolver
}

type NetworkResolverProvider interface {
	GetNetworkResolver() network.Resolver
}
