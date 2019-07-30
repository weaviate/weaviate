//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package fake

import (
	"fmt"

	graphqlnetwork "github.com/semi-technologies/weaviate/adapters/handlers/graphql/network"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/fetch"
	"github.com/semi-technologies/weaviate/entities/models"
	network "github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
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

func (fn FakeNetwork) ListPeers() (peers.Peers, error) {
	// there are no peers, but don't error
	return peers.Peers{}, nil
}

func (fn FakeNetwork) UpdatePeers(new_peers peers.Peers) error {
	return fmt.Errorf("Cannot update peers, because there is no network configured")
}

func (fn FakeNetwork) ProxyGetInstance(common.Params) (*models.GraphQLResponse, error) {
	return nil, fmt.Errorf("Cannot proxy get instance, because there is no network configured")
}

func (fn FakeNetwork) ProxyGetMetaInstance(common.Params) (*models.GraphQLResponse, error) {
	return nil, fmt.Errorf("Cannot proxy get meta, because there is no network configured")
}

func (fn FakeNetwork) ProxyAggregateInstance(common.Params) (*models.GraphQLResponse, error) {
	return nil, fmt.Errorf("Cannot proxy aggregate, because there is no network configured")
}

func (fn FakeNetwork) ProxyFetch(common.SubQuery) ([]fetch.Response, error) {
	return nil, fmt.Errorf("Cannot proxy fetch, because there is no network configured")
}

func (fn FakeNetwork) RegisterUpdatePeerCallback(callbackFn network.PeerUpdateCallback) {
	return
}

// GetNetworkResolver for now simply returns itself
// because the network is not fully plugable yet.
// Once we have made the network pluggable, then this would
// be a method on the connector which returns the actual
// plugged in Network
func (fn FakeNetwork) GetNetworkResolver() graphqlnetwork.Resolver {
	return fn
}

// RegisterSchemaGetter does nothing, since it's a fake network
// but also doesn't error
func (fn FakeNetwork) RegisterSchemaGetter(schemaGetter network.SchemaGetter) {
}
