//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

type peersLister interface {
	ListPeers() (peers.Peers, error)
}

type referenceSchemaUpdater struct {
	schemaManager schemaManager
	network       peersLister
	fromClass     string
	kind          kind.Kind
	ctx           context.Context
	principal     *models.Principal
}

func newReferenceSchemaUpdater(ctx context.Context, principal *models.Principal, schemaManager schemaManager,
	network peersLister, fromClass string, kind kind.Kind) *referenceSchemaUpdater {
	return &referenceSchemaUpdater{schemaManager, network, fromClass, kind, ctx, principal}
}

// make sure this only ever called AFTER validtion as it skips
// validation checks itself
func (u *referenceSchemaUpdater) addNetworkDataTypes(schema interface{}) error {
	if schema == nil {
		return nil
	}

	for propName, prop := range schema.(map[string]interface{}) {
		switch propTyped := prop.(type) {
		case *models.SingleRef:
			err := u.singleRef(propTyped, propName)
			if err != nil {
				return err
			}

		case models.MultipleRef:
			for _, single := range propTyped {
				err := u.singleRef(single, propName)
				if err != nil {
					return err
				}
			}

		default:
			// primitive prop, skip
			continue
		}
	}
	return nil
}

func (u *referenceSchemaUpdater) singleRef(prop *models.SingleRef, propName string) error {
	parsed, err := crossref.ParseSingleRef(prop)
	if err != nil {
		return err
	}

	if parsed.Local {
		// local ref, nothign to do
		return nil
	}

	peers, err := u.network.ListPeers()
	if err != nil {
		return fmt.Errorf("could not list network peers: %s", err)
	}

	remoteKind, err := peers.RemoteKind(crossrefs.NetworkKind{
		Kind:     parsed.Kind,
		ID:       parsed.TargetID,
		PeerName: parsed.PeerName,
	})
	if err != nil {
		return fmt.Errorf("invalid network reference: %s", err)
	}

	return u.updateSchema(remoteKind, parsed.PeerName, prop, propName)
}

func (u *referenceSchemaUpdater) updateSchema(remoteKind interface{}, peerName string, prop *models.SingleRef,
	propName string) error {
	switch thingOrAction := remoteKind.(type) {
	case *models.Thing:
		remoteClass := fmt.Sprintf("%s/%s", peerName, thingOrAction.Class)
		err := u.schemaManager.UpdatePropertyAddDataType(u.ctx, u.principal, u.kind, u.fromClass, propName, remoteClass)
		if err != nil {
			return fmt.Errorf("could not add network thing class %s to %s.%s: %s",
				remoteClass, u.fromClass, propName, err)
		}
	case models.Action:
		remoteClass := fmt.Sprintf("%s/%s", peerName, thingOrAction.Class)
		err := u.schemaManager.UpdatePropertyAddDataType(u.ctx, u.principal, u.kind, u.fromClass, propName, remoteClass)
		if err != nil {
			return fmt.Errorf("could not add network action class %s to %s.%s: %s",
				remoteClass, u.fromClass, propName, err)
		}
	default:
		return fmt.Errorf("unrecognized kind from remote peer for %s from %s", prop.Beacon, peerName)
	}

	return nil
}
