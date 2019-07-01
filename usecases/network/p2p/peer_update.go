/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package p2p

import (
	"reflect"

	libnetwork "github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
)

func (n *network) UpdatePeers(newPeers peers.Peers) error {
	n.Lock()
	defer n.Unlock()

	n.logger.
		WithField("action", "network_peer_update").
		WithField("peers", newPeers).
		Debug("received updated peer list")

	if !havePeersChanged(n.peers, newPeers) {
		n.logger.
			WithField("action", "network_peer_update").
			WithField("peers", newPeers).
			Debug("peers have not changed, doing nothing")
		return nil
	}

	n.logger.
		WithField("action", "network_peer_update").
		WithField("peers", newPeers).
		Debug("peers have changed, updating schema")

	// download schema updates if peers are new or their hash changed
	// in this iteration.
	newPeers = n.downloadChanged(newPeers)

	n.peers = newPeers
	for _, callbackFn := range n.callbacks {
		callbackFn(newPeers)
	}

	return nil
}

func (n *network) RegisterUpdatePeerCallback(callbackFn libnetwork.PeerUpdateCallback) {
	n.callbacks = append(n.callbacks, callbackFn)
}

func havePeersChanged(oldPeers peers.Peers, newPeers peers.Peers) bool {
	return !reflect.DeepEqual(oldPeers, newPeers)
}
