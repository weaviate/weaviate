package peers

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

// RemoteKind tries to retrieve a kind (i.e. thing or action) from the
// specified remote peer. It fails if the peer is not in the network, does not
// have the requested resource or if the resource can be queried but it doesn't
// fit into our cached copy of the peers schema.
func (p Peers) RemoteKind(kind crossrefs.NetworkKind) (models.Thing, error) {
	result := models.Thing{}
	peer, err := p.ByName(kind.PeerName)
	if err != nil {
		return result, fmt.Errorf("kind '%s' with id '%s' does not exist: %s",
			kind.Kind, kind.ID, err)
	}

	client, err := peer.CreateClient()
	if err != nil {
		return result, fmt.Errorf(
			"could not get remote kind, because could not create client: %s", err)
	}

	params := things.NewWeaviateThingsGetParams().
		WithTimeout(1 * time.Second).
		WithThingID(kind.ID)
	ok, err := client.Things.WeaviateThingsGet(params, nil)
	if err != nil {
		return result, fmt.Errorf(
			"could not get remote kind: could not GET things from peer: %s", err)
	}

	return ok.Payload.Thing, nil
}
