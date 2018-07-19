package state

import (
	"fmt"
	"github.com/go-openapi/strfmt"
	log "github.com/sirupsen/logrus"
	"sync"
)

type inMemoryState struct {
	sync.Mutex
	peers map[string]Peer
}

func NewInMemoryState() State {
	return State(inMemoryState{
		peers: make(map[string]Peer),
	})
}

func (im inMemoryState) RegisterPeer(name string, host strfmt.Hostname) error {
	im.Lock()
	defer im.Unlock()
	log.Debugf("Registering peer '%v'", name)
	_, ok := im.peers[name]

	if ok {
		log.Debugf("Already a peer named '%v'", name)

		return fmt.Errorf("Such a peer already exists")
	} else {
		log.Debugf("Added the peer '%v'", name)

		im.peers[name] = Peer{
			name: name,
			host: host,
		}

		return nil
	}
}

func (im inMemoryState) ListPeers() ([]Peer, error) {
	im.Lock()
	defer im.Unlock()

	peers := make([]Peer, 0)

	for _, v := range im.peers {
		peers = append(peers, v)
	}

	return peers, nil
}

func (im inMemoryState) RemovePeer(name string) error {
	im.Lock()
	defer im.Unlock()

	_, ok := im.peers[name]

	if ok {
		delete(im.peers, name)
	}

	return nil
}

func (im inMemoryState) UpdatePeer(name string, update PeerInfo) error {
	im.Lock()
	defer im.Unlock()

	peer, ok := im.peers[name]

	if ok {
		peer.LastContactAt = update.LastContactAt
		return nil
	} else {
		return fmt.Errorf("No such peer exists")
	}
}
