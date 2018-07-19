package network

import (
	"fmt"
)

type FakeNetwork struct {
	// nothing here :)
}

func (fn FakeNetwork) IsReady() bool {
	return false
}

func (fn FakeNetwork) GetStatus() string {
	return "not configured"
}

func (fn FakeNetwork) ListPeers() ([]Peer, error) {
	return nil, fmt.Errorf("Cannot list peers, because there is no network configured")
}
