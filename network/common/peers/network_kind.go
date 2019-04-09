/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package peers

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	libkind "github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

// RemoteKind tries to retrieve a kind (i.e. thing or action) from the
// specified remote peer. It fails if the peer is not in the network, does not
// have the requested resource or if the resource can be queried but it doesn't
// fit into our cached copy of the peers schema.
func (p Peers) RemoteKind(kind crossrefs.NetworkKind) (interface{}, error) {
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

	return p.getRemoteThingOrAction(kind, client)
}

func (p Peers) getRemoteThingOrAction(kind crossrefs.NetworkKind,
	client *client.WeaviateDecentralisedKnowledgeGraph) (interface{}, error) {
	result := models.Thing{}
	switch kind.Kind {
	case libkind.THING_KIND:
		params := things.NewWeaviateThingsGetParams().
			WithTimeout(1 * time.Second).
			WithID(kind.ID)
		ok, err := client.Things.WeaviateThingsGet(params, nil)
		if err != nil {
			return result, fmt.Errorf(
				"could not get remote kind: could not GET things from peer: %s", err)
		}

		_, err = p.HasClass(crossrefs.NetworkClass{ClassName: ok.Payload.AtClass, PeerName: kind.PeerName})
		if err != nil {
			return result, fmt.Errorf(
				"schema mismatch: class of remote kind (%s) is not in the cached remote schema", ok.Payload.AtClass)
		}

		return ok.Payload, nil
	case libkind.ACTION_KIND:
		params := actions.NewWeaviateActionsGetParams().
			WithTimeout(1 * time.Second).
			WithActionID(kind.ID)
		ok, err := client.Actions.WeaviateActionsGet(params, nil)
		if err != nil {
			return result, fmt.Errorf(
				"could not get remote kind: could not GET things from peer: %s", err)
		}

		_, err = p.HasClass(crossrefs.NetworkClass{ClassName: ok.Payload.Action.AtClass, PeerName: kind.PeerName})
		if err != nil {
			return result, fmt.Errorf(
				"schema mismatch: class of remote kind (%s) is not in the cached remote schema", ok.Payload.Action.AtClass)
		}

		return ok.Payload.Action, nil
	default:
		return result, fmt.Errorf("could not get remote kind: unknown kind '%s'", kind.Kind)
	}
}
