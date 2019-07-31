//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package fake

import (
	"fmt"

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

func (fn FakeNetwork) RegisterUpdatePeerCallback(callbackFn network.PeerUpdateCallback) {
	return
}

// RegisterSchemaGetter does nothing, since it's a fake network
// but also doesn't error
func (fn FakeNetwork) RegisterSchemaGetter(schemaGetter network.SchemaGetter) {
}
