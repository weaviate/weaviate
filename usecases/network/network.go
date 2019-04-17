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
package network

import (
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/network/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/network/fetch"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
)

// PeerUpdateCallback should be called by UpdatePeers after a successful peer update
type PeerUpdateCallback func(peers peers.Peers)

// SchemaGetter must be provided to the Network, so the network
// has a means to retrieve the schema on each polling cycle.
// It does not care about whether a lock is necessary to retrieve the
// schema. If it is, then the provided implemtation must make
// sure that Schema() locks and unlocks.
type SchemaGetter interface {
	Schema() (schema.Schema, error)
}

// Network is a Minimal abstraction over the network. This is the only API exposed to the rest of Weaviate.
type Network interface {
	IsReady() bool
	GetStatus() string

	ListPeers() (peers.Peers, error)

	ProxyGetInstance(common.Params) (*models.GraphQLResponse, error)
	ProxyGetMetaInstance(common.Params) (*models.GraphQLResponse, error)
	ProxyAggregateInstance(common.Params) (*models.GraphQLResponse, error)
	ProxyFetch(query common.SubQuery) ([]fetch.Response, error)

	// UpdatePeers is Invoked by the Genesis server via an HTTP endpoint.
	UpdatePeers(newPeers peers.Peers) error

	// RegisterUpdatePeerCallback to be called after successful peer updates
	RegisterUpdatePeerCallback(callbackFn PeerUpdateCallback)

	// Register a SchemaGetter which can be called in each pinging
	// cycle to retrieve the current schema (hash)
	RegisterSchemaGetter(getter SchemaGetter)
}
