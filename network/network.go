package network

import (
	"github.com/go-openapi/strfmt"
)

// A peer represents a known peer, given to us by the Genesis service.
type Peer struct {
	name string
	host strfmt.Hostname
}

// Minimal abstraction over the network. This is the only API exposed to the rest of Weaviate.
type Network interface {
	IsReady() bool
	GetStatus() string

	ListPeers() ([]Peer, error)

	// TODO: We'll add functions like
	// - QueryNetwork(q NetworkQuery, timeout int) (chan NetworkResponse, error)
}
