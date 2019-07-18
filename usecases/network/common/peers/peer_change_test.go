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

import (
	"reflect"
	"testing"
)

func TestEmptyPeersToSinglePeer(t *testing.T) {
	before := Peers{}
	after := Peers{{ID: "foo"}}

	actual := PeersDiff(before, after)
	expected := Peers{{
		ID:         "foo",
		LastChange: NewlyAdded,
	}}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected new peer list to be %#v, but was %#v", expected, actual)
	}
}

func TestSamePeersAsBefore(t *testing.T) {
	before := Peers{{ID: "foo"}}
	after := Peers{{ID: "foo"}}

	actual := PeersDiff(before, after)
	expected := Peers{{
		ID:         "foo",
		LastChange: NoChange,
	}}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected new peer list to be %#v, but was %#v", expected, actual)
	}
}

func TestPeerUpdatedSchema(t *testing.T) {
	before := Peers{{ID: "foo", SchemaHash: "oldhash"}}
	after := Peers{{ID: "foo", SchemaHash: "newhash"}}

	actual := PeersDiff(before, after)
	expected := Peers{{
		ID:         "foo",
		LastChange: SchemaChange,
		SchemaHash: "newhash",
	}}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected new peer list to be %#v, but was %#v", expected, actual)
	}
}
