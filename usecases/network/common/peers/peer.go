//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package peers

import (
	"fmt"
	"net/url"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// Peer represents a known peer, given to us by the Genesis service.
type Peer struct {
	ID          strfmt.UUID
	Name        string
	URI         strfmt.URI
	LastChange  PeerChange
	SchemaHash  string
	Schema      schema.Schema
	SchemaError error
}

// CreateClient to access the full API of the peer. Pre-configured to the
// peer's URI and scheme. Currently assumes the default BasePath
func (p Peer) CreateClient() (*client.Weaviate, error) {
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

// ByName retrieves the peer which matches the specified
// name or errors
func (p Peers) ByName(name string) (Peer, error) {
	for _, peer := range p {
		if peer.Name == name {
			return peer, nil
		}
	}

	return Peer{}, fmt.Errorf("no peer '%s' in the network", name)
}
