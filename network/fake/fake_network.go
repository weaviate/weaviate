package fake

import (
	"fmt"

	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	graphqlnetworkGet "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network"
)

type FakeNetwork struct {
	// nothing here :)
}

func (fn FakeNetwork) IsReady() bool {
	return false
}

func (fn FakeNetwork) GetStatus() string {
	return "not configured"
}

func (fn FakeNetwork) ListPeers() ([]network.Peer, error) {
	return nil, fmt.Errorf("Cannot list peers, because there is no network configured")
}

func (fn FakeNetwork) UpdatePeers(new_peers []network.Peer) error {
	return fmt.Errorf("Cannot update peers, because there is no network configured")
}

func (fn FakeNetwork) ProxyGetInstance(graphqlnetworkGet.ProxyGetInstanceParams) (*models.GraphQLResponse, error) {
	return nil, fmt.Errorf("Cannot proxy get instance, because there is no network configured")
}

// GetNetworkResolver for now simply returns itself
// because the network is not fully plugable yet.
// Once we have made the network pluggable, then this would
// be a method on the connector which returns the actual
// plugged in Network
func (fn FakeNetwork) GetNetworkResolver() graphqlnetwork.Resolver {
	return fn
}
