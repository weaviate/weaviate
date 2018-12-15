/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package network

import (
	"fmt"
	"net/url"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	network_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

// Peer represents a known peer, given to us by the Genesis service.
type Peer struct {
	Id   strfmt.UUID
	Name string
	URI  strfmt.URI
}

// CreateClient to access the full API of the peer. Pre-configured to the
// peer's URI and scheme. Currently assumes the default BasePath
func (p Peer) CreateClient() (*client.WeaviateDecentralisedKnowledgeGraph, error) {
	url, err := url.Parse(p.URI.String())
	if err != nil {
		return nil, fmt.Errorf("could not parse peer URL: %s", err)
	}

	config := &client.TransportConfig{
		Host:     url.Host,
		BasePath: client.DefaultBasePath,
		Schemes:  []string{url.Scheme},
	}
	peerClient := client.NewHTTPClientWithConfig(nil, config)

	return peerClient, nil
}

// Peers is a list of peers, can be used to retrieve all names
type Peers []Peer

// Names returns a list of all names of all peers
// This is to not expose internals in cases where
// only the names matter, such as in the GraphQL API
func (p Peers) Names() []string {
	names := make([]string, len(p), len(p))
	for i, peer := range p {
		names[i] = peer.Name
	}
	return names
}

// PeerUpdateCallback should be called by UpdatePeers after a successful peer update
type PeerUpdateCallback func(peers Peers)

// SchemaGetter must be provided to the Network, so the network
// has a means to retrieve the schema on each polling cycle.
// It does not care about whether a lock is necessary to retrieve the
// schema. If it is, then the provided implemtation must make
// sure that Schema() locks and unlocks.
type SchemaGetter interface {
	Schema() schema.Schema
}

// Network is a Minimal abstraction over the network. This is the only API exposed to the rest of Weaviate.
type Network interface {
	IsReady() bool
	GetStatus() string

	ListPeers() (Peers, error)
	// GetNetworkResolver() Network

	ProxyGetInstance(network_get.ProxyGetInstanceParams) (*models.GraphQLResponse, error)

	// UpdatePeers is Invoked by the Genesis server via an HTTP endpoint.
	UpdatePeers(newPeers Peers) error

	// RegisterUpdatePeerCallback to be called after successful peer updates
	RegisterUpdatePeerCallback(callbackFn PeerUpdateCallback)

	// Register a SchemaGetter which can be called in each pinging
	// cycle to retrieve the current schema (hash)
	RegisterSchemaGetter(getter SchemaGetter)

	// TODO: We'll add functions like
	// - QueryNetwork(q NetworkQuery, timeout int) (chan NetworkResponse, error)
}
