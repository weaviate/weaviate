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

package peers

// PeerChange indicates how a peer has changed compared to a previous
// update cycle
type PeerChange string

const (
	// NoChange indicates the peer was already present
	// and had the same schema hash previous round
	NoChange PeerChange = "NoChange"

	// SchemaChange indicates that the peer was already
	// present, but with a different schema
	SchemaChange PeerChange = "SchemaChange"

	// NewlyAdded indicates the peer was not present before
	NewlyAdded PeerChange = "NewlyAdded"
)

func PeersDiff(before Peers, after Peers) Peers {
	for i, peer := range after {
		after[i].LastChange = comparePeer(peer, before)
	}

	return after
}

func contained(list Peers, single Peer) (Peer, bool) {
	for _, current := range list {
		if current.ID == single.ID {
			return current, true
		}
	}

	return Peer{}, false
}

func comparePeer(peer Peer, before Peers) PeerChange {
	match, ok := contained(before, peer)
	if !ok {
		return NewlyAdded
	}

	if peer.SchemaHash != match.SchemaHash {
		return SchemaChange
	}

	return NoChange
}
