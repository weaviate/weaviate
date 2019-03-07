/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package graphqlapi

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
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
	ContextionaryProvider
	RequestsLogProvider
}

type DatabaseResolverProvider interface {
	GetResolver() (ClosingResolver, error)
}

type NetworkResolverProvider interface {
	GetNetworkResolver() network.Resolver
}

type ContextionaryProvider interface {
	GetContextionary() fetch.Contextionary
}

type RequestsLogProvider interface {
	GetRequestsLog() *telemetry.RequestsLog
}
