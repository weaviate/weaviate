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
	uri  strfmt.URI
}

func (p Peer) Name() string {
	return p.name
}

func (p Peer) URI() strfmt.URI {
	return p.uri
}

// Abstract interface over how the Genesis server should store state.
type State interface {
	RegisterPeer(name string, uri strfmt.URI) (*Peer, error)
	ListPeers() ([]Peer, error)

	// Idempotent remove; removing a non-existing peer should not fail.
	RemovePeer(id strfmt.UUID) error

	UpdateLastContact(id strfmt.UUID, contact_time time.Time) error
}
