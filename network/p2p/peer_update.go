package p2p

import (
	"fmt"
	"reflect"

	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
)

func (n *network) UpdatePeers(newPeers []libnetwork.Peer) error {
	n.Lock()
	defer n.Unlock()

	n.messaging.InfoMessage(fmt.Sprintf("Received updated peer list with %v peers", len(newPeers)))

	if !havePeersChanged(n.peers, newPeers) {
		return nil
	}

	n.peers = newPeers
	for _, callbackFn := range n.callbacks {
		callbackFn(newPeers)
	}

	return nil
}

func (n *network) RegisterUpdatePeerCallback(callbackFn libnetwork.PeerUpdateCallback) {
	n.callbacks = append(n.callbacks, callbackFn)
}

func havePeersChanged(oldPeers []libnetwork.Peer, newPeers []libnetwork.Peer) bool {
	return !reflect.DeepEqual(oldPeers, newPeers)
}
