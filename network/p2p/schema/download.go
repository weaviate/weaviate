package schema

import (
	"fmt"
	"time"

	schemaclient "github.com/creativesoftwarefdn/weaviate/client/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
)

func download(peer peers.Peer) (schema.Schema, error) {
	peerClient, err := peer.CreateClient()
	if err != nil {
		return schema.Schema{}, fmt.Errorf(
			"could not create client for %s: %s", peer.Name, err)
	}

	params := &schemaclient.WeaviateSchemaDumpParams{}
	params.WithTimeout(2 * time.Second)
	ok, err := peerClient.Schema.WeaviateSchemaDump(params, nil)
	if err != nil {
		return schema.Schema{}, fmt.Errorf(
			"could not download schema from %s: %s", peer.Name, err)
	}

	return schema.Schema{
		Things:  ok.Payload.Things,
		Actions: ok.Payload.Actions,
	}, nil
}
