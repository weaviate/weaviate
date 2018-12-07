package p2p

import (
	"testing"

	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/go-openapi/strfmt"
)

func TestGetExistingPeer(t *testing.T) {
	peer := libnetwork.Peer{
		Id:   strfmt.UUID("some-id"),
		Name: "best-peer",
		URI:  "http://best-peer.com",
	}

	subject := network{
		peers: []libnetwork.Peer{peer},
	}

	actual, err := subject.GetPeerByName("best-peer")

	t.Run("should not error", func(t *testing.T) {
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("should return correct peer", func(t *testing.T) {
		if actual.Id != peer.Id {
			t.Errorf("%s does not match, wanted %s, gut got %s", "Id", peer.Id, actual.Id)
		}

		if actual.Name != peer.Name {
			t.Errorf("%s does not match, wanted %s, gut got %s", "Name", peer.Name, actual.Name)
		}

		if actual.URI != peer.URI {
			t.Errorf("%s does not match, wanted %s, gut got %s", "URI", peer.URI, actual.URI)
		}
	})

}

func TestGetWrongPeer(t *testing.T) {
	peer := libnetwork.Peer{
		Id:   strfmt.UUID("some-id"),
		Name: "best-peer",
		URI:  "http://best-peer.com",
	}

	subject := network{
		peers: []libnetwork.Peer{peer},
	}

	_, err := subject.GetPeerByName("worst-peer")

	t.Run("should error with ErrPeerNotFound", func(t *testing.T) {
		if err != ErrPeerNotFound {
			t.Errorf("expected peer not found error, but got %s", err)
		}
	})
}
