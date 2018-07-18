package state

import (
  "time"
)


type PeerInfo  struct {
  LastContactAt time.Time
}

type Peer struct {
  PeerInfo
  name string
  host string
}

// Abstract interface over how the Genesis server should store state.
type State interface {
  RegisterPeer(name string, host string) error
  ListPeers() ([]Peer, error)

  // Idempotent remove; removing a non-existing peer should not fail.
  RemovePeer(name string) error

  UpdatePeer(name string, peer PeerInfo) error
}
