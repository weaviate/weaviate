package state

import (
	"fmt"
	"github.com/go-openapi/strfmt"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"sync"
)

type inMemoryState struct {
	sync.Mutex
	peers map[strfmt.UUID]Peer
}

func NewInMemoryState() State {
	return State(inMemoryState{
		peers: make(map[strfmt.UUID]Peer),
	})
}

func (im inMemoryState) RegisterPeer(name string, host strfmt.Hostname) (*Peer, error) {
	im.Lock()
	defer im.Unlock()

	id := strfmt.UUID(uuid.NewV4().String())

	log.Debugf("Registering peer '%v' with id '%v'", name, id)
	peer := Peer{
		PeerInfo: PeerInfo{Id: id},
		name:     name,
		host:     host,
	}

	im.peers[id] = peer
	return &peer, nil
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

func (im inMemoryState) RemovePeer(id strfmt.UUID) error {
	im.Lock()
	defer im.Unlock()

	_, ok := im.peers[id]

	if ok {
		delete(im.peers, id)
	}

	return nil
}

func (im inMemoryState) UpdatePeer(id strfmt.UUID, update PeerInfo) error {
	im.Lock()
	defer im.Unlock()

	peer, ok := im.peers[id]

	if ok {
		peer.LastContactAt = update.LastContactAt
		return nil
	} else {
		return fmt.Errorf("No such peer exists")
	}
}
