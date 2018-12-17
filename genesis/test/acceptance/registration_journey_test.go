package test

import (
	"context"
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/genesis/client"
	"github.com/creativesoftwarefdn/weaviate/genesis/client/operations"
	"github.com/creativesoftwarefdn/weaviate/genesis/models"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestPeerRegistrationJourney(t *testing.T) {
	var (
		newPeerID strfmt.UUID
	)
	genesisClient := client.NewHTTPClient(nil)
	transport := httptransport.New("localhost:8111", "/", []string{"http"})
	genesisClient.SetTransport(transport)

	t.Run("peers should be empty in the beginning", func(t *testing.T) {
		peers, err := genesisClient.Operations.GenesisPeersList(nil)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		if len(peers.Payload) != 0 {
			t.Fatalf("expected get an empty list of peers at startup, but got \n%#v", peers.Payload)
		}
	})

	t.Run("it should be possible to register a peer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		params := &operations.GenesisPeersRegisterParams{
			Body: &models.PeerUpdate{
				PeerName: "myFavoritePeer",
				PeerURI:  strfmt.URI("http://does-not-matter-for-now"),
			},
			Context: ctx,
		}
		ok, err := genesisClient.Operations.GenesisPeersRegister(params)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		// Store ID for later
		newPeerID = ok.Payload.Peer.ID
	})

	t.Run("peers should now contain added peer", func(t *testing.T) {
		peers, err := genesisClient.Operations.GenesisPeersList(nil)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		assert.Equal(t, 1, len(peers.Payload), "list of peers should have exactly one entry")
		assert.Equal(t, "myFavoritePeer", peers.Payload[0].PeerName, "peer should match what we registered before")
	})

	t.Run("peers can be deregistered", func(t *testing.T) {
		params := operations.NewGenesisPeersLeaveParams().
			WithTimeout(1 * time.Second).
			WithPeerID(newPeerID)
		_, err := genesisClient.Operations.GenesisPeersLeave(params)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}
	})
}
