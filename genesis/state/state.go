package state

import (
	"github.com/go-openapi/strfmt"
	"time"
)

type PeerInfo struct {
	LastContactAt time.Time
}

type Peer struct {
	PeerInfo
	name string
	host strfmt.Hostname
}

func (p Peer) Name() string {
	return p.name
}

func (p Peer) Host() strfmt.Hostname {
	return p.host
}

// Abstract interface over how the Genesis server should store state.
type State interface {
	RegisterPeer(name string, host strfmt.Hostname) error
	ListPeers() ([]Peer, error)

	// Idempotent remove; removing a non-existing peer should not fail.
	RemovePeer(name string) error

	UpdatePeer(name string, peer PeerInfo) error
}
