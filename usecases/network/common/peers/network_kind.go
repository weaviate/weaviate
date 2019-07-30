//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package peers

import (
	"fmt"
	"time"

	"github.com/semi-technologies/weaviate/client"
	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	libkind "github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
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
	case libkind.Thing:
		params := things.NewWeaviateThingsGetParams().
			WithTimeout(1 * time.Second).
			WithID(kind.ID)
		ok, err := client.Things.WeaviateThingsGet(params, nil)
		if err != nil {
			return result, fmt.Errorf(
				"could not get remote kind: could not GET things from peer: %s", err)
		}

		_, err = p.HasClass(crossrefs.NetworkClass{ClassName: ok.Payload.Class, PeerName: kind.PeerName})
		if err != nil {
			return result, fmt.Errorf(
				"schema mismatch: class of remote kind (%s) is not in the cached remote schema", ok.Payload.Class)
		}

		return ok.Payload, nil
	case libkind.Action:
		params := actions.NewWeaviateActionsGetParams().
			WithTimeout(1 * time.Second).
			WithID(kind.ID)
		ok, err := client.Actions.WeaviateActionsGet(params, nil)
		if err != nil {
			return result, fmt.Errorf(
				"could not get remote kind: could not GET things from peer: %s", err)
		}

		_, err = p.HasClass(crossrefs.NetworkClass{ClassName: ok.Payload.Class, PeerName: kind.PeerName})
		if err != nil {
			return result, fmt.Errorf(
				"schema mismatch: class of remote kind (%s) is not in the cached remote schema", ok.Payload.Class)
		}

		return ok.Payload, nil
	default:
		return result, fmt.Errorf("could not get remote kind: unknown kind '%s'", kind.Kind)
	}
}
