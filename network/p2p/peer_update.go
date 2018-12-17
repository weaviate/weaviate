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
package p2p

import (
	"fmt"
	"reflect"

	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
)

func (n *network) UpdatePeers(newPeers libnetwork.Peers) error {
	n.Lock()
	defer n.Unlock()

	n.messaging.InfoMessage(fmt.Sprintf("Received updated peer list with %v peers", len(newPeers)))

	if !havePeersChanged(n.peers, newPeers) {
		return nil
	}

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

func havePeersChanged(oldPeers libnetwork.Peers, newPeers libnetwork.Peers) bool {
	return !reflect.DeepEqual(oldPeers, newPeers)
}
