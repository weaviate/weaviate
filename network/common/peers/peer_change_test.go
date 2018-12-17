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
