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

package p2p

import (
	"reflect"
	"testing"

	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestPeerUpdateWithNewPeers(t *testing.T) {
	oldPeers := []peers.Peer{}
	newPeers := []peers.Peer{{
		Name: "best-weaviate",
		ID:   "uuid",
		URI:  "does-not-matter",
	}}

	logger, _ := test.NewNullLogger()
	subject := network{
		peers:           oldPeers,
		downloadChanged: downloadChangedFake(newPeers),
		logger:          logger,
	}

	callbackCalled := false
	callbackCalledWith := []peers.Peer{}
	callbackSpy := func(peers peers.Peers) {
		callbackCalled = true
		callbackCalledWith = peers
	}

	subject.RegisterUpdatePeerCallback(callbackSpy)
	subject.UpdatePeers(newPeers)

	if callbackCalled != true {
		t.Error("expect PeerUpdateCallback to be called, but was never called")
	}

	if !reflect.DeepEqual(callbackCalledWith, newPeers) {
		t.Errorf("expect PeerUpdateCallback to be called with new peers, but was called with %#v",
			callbackCalledWith)
	}
}

func TestPeerUpdateWithoutAnyChange(t *testing.T) {
	unchangedPeers := []peers.Peer{{
		Name: "best-weaviate",
		ID:   "uuid",
		URI:  "does-not-matter",
	}}

	logger, _ := test.NewNullLogger()
	subject := network{
		peers:           unchangedPeers,
		downloadChanged: downloadChangedFake(unchangedPeers),
		logger:          logger,
	}

	callbackCalled := false
	callbackSpy := func(peers peers.Peers) {
		callbackCalled = true
	}

	subject.RegisterUpdatePeerCallback(callbackSpy)
	subject.UpdatePeers(unchangedPeers)

	if callbackCalled != false {
		t.Error("expect PeerUpdateCallback not to be called, but it was called")
	}
}

func downloadChangedFake(peerList peers.Peers) func(peers.Peers) peers.Peers {
	return func(peers.Peers) peers.Peers {
		return peerList
	}
}
