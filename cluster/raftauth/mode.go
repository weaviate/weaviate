//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package raftauth authenticates the Raft plane (the raft transport and the
// cluster gRPC service) with the cluster basic auth credentials.
//
// The raft transport uses a mutual HMAC-SHA256 challenge-response handshake at
// the start of every TCP connection, so the password never goes on the wire.
// The mode follows the same switches as the rest of the cluster API:
// credentials without SendOnly verify incoming peers, credentials with
// SendOnly only send them, and no credentials leave the plane open.
package raftauth

import (
	"net"

	"github.com/weaviate/weaviate/usecases/cluster"
)

// Mode is the effective Raft plane authentication mode of a node.
type Mode int

const (
	// ModeOff sends no credentials and accepts every peer.
	ModeOff Mode = iota
	// ModeSend sends credentials but accepts every peer, warning on a credential mismatch.
	ModeSend
	// ModeVerify sends credentials and rejects peers that cannot prove them.
	ModeVerify
)

// ModeFrom derives the mode from the cluster basic auth config, so the raft
// transport and the cluster gRPC service can never disagree.
func ModeFrom(ba cluster.BasicAuth) Mode {
	switch {
	case ba.VerifyEnabled():
		return ModeVerify
	case ba.Enabled():
		return ModeSend
	default:
		return ModeOff
	}
}

// Sends reports whether outgoing connections carry credentials.
func (m Mode) Sends() bool {
	return m != ModeOff
}

// Verifies reports whether incoming connections must prove the credentials.
func (m Mode) Verifies() bool {
	return m == ModeVerify
}

func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "off"
	case ModeSend:
		return "send"
	case ModeVerify:
		return "verify"
	default:
		return "unknown"
	}
}

// IsLoopbackBind reports whether a bind host only accepts local connections.
func IsLoopbackBind(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
