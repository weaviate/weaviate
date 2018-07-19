package state

import (
  "fmt"
   log "github.com/sirupsen/logrus"
  "github.com/go-openapi/strfmt"
)

type inMemoryState struct {
  peers map[string]Peer
}

func NewInMemoryState() *State {
  s := State(inMemoryState {
    peers: make(map[string]Peer),
  })

  log.Debug("Created in memory state")

  return  &s
}

func (im inMemoryState) RegisterPeer(name string, host strfmt.Hostname) error {
  _, ok := im.peers[name]

  if ok {
    log.Debugf("Already a peer named %v", name)

    return fmt.Errorf("Such a peer already exists")
  } else {
    log.Debugf("Added the peer %v", name)

    im.peers[name] = Peer {
      name: name,
      host: host,
    }

    return nil
  }
}

func (im inMemoryState) ListPeers() ([]Peer, error) {
  peers := make([]Peer, 0)

  for _, v := range(im.peers) {
    peers = append(peers, v)
  }

  return peers, nil
}

func (im inMemoryState) RemovePeer(name string) error {
  _, ok := im.peers[name]

  if ok {
    delete(im.peers, name)
  }

  return nil
}

func (im inMemoryState) UpdatePeer(name string, update PeerInfo) error {
  peer, ok := im.peers[name]

  if ok {
    peer.LastContactAt = update.LastContactAt
    return nil
  } else {
    return fmt.Errorf("No such peer exists")
  }
}
