package network

import (
	"fmt"
	"net/url"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
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

// Network is a Minimal abstraction over the network. This is the only API exposed to the rest of Weaviate.
type Network interface {
	IsReady() bool
	GetStatus() string

	ListPeers() ([]Peer, error)
	GetNetworkResolver() network.Resolver

	// UpdatePeers is Invoked by the Genesis server via an HTTP endpoint.
	UpdatePeers(newPeers []Peer) error

	// TODO: We'll add functions like
	// - QueryNetwork(q NetworkQuery, timeout int) (chan NetworkResponse, error)
}
