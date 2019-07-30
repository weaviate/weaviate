//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package state

import (
	"time"

	"github.com/go-openapi/strfmt"
)

type PeerInfo struct {
	Id            strfmt.UUID
	LastContactAt time.Time
	SchemaHash    string
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

	UpdateLastContact(id strfmt.UUID, contact_time time.Time, schemaHash string) error
}
