package state

import (
	"github.com/go-openapi/strfmt"
	"time"
)

type PeerInfo struct {
	Id            strfmt.UUID
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
	RegisterPeer(name string, host strfmt.Hostname) (*Peer, error)
	ListPeers() ([]Peer, error)

	// Idempotent remove; removing a non-existing peer should not fail.
	RemovePeer(id strfmt.UUID) error

	UpdatePeer(id strfmt.UUID, peer PeerInfo) error
}
