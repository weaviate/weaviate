package network

import (
	"github.com/go-openapi/strfmt"
)

// A peer represents a known peer, given to us by the Genesis service.
type Peer struct {
	Id   strfmt.UUID
	Name string
	Host strfmt.Hostname
}

// Minimal abstraction over the network. This is the only API exposed to the rest of Weaviate.
type Network interface {
	IsReady() bool
	GetStatus() string

	ListPeers() ([]Peer, error)

	// Invoked by the Genesis server via an HTTP endpoint.
	UpdatePeers(new_peers []Peer) error

	// TODO: We'll add functions like
	// - QueryNetwork(q NetworkQuery, timeout int) (chan NetworkResponse, error)
}
