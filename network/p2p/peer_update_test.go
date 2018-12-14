package p2p

import (
	"reflect"
	"testing"

	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
)

func TestPeerUpdateWithNewPeers(t *testing.T) {
	oldPeers := []libnetwork.Peer{}
	newPeers := []libnetwork.Peer{{
		Name: "best-weaviate",
		Id:   "uuid",
		URI:  "does-not-matter",
	}}

	subject := network{
		peers: oldPeers,
	}

	callbackCalled := false
	callbackCalledWith := []libnetwork.Peer{}
	callbackSpy := func(peers libnetwork.Peers) {
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
	peers := []libnetwork.Peer{{
		Name: "best-weaviate",
		Id:   "uuid",
		URI:  "does-not-matter",
	}}

	subject := network{
		peers: peers,
	}

	callbackCalled := false
	callbackSpy := func(peers libnetwork.Peers) {
		callbackCalled = true
	}

	subject.RegisterUpdatePeerCallback(callbackSpy)
	subject.UpdatePeers(peers)

	if callbackCalled != false {
		t.Error("expect PeerUpdateCallback not to be called, but it was called")
	}
}
