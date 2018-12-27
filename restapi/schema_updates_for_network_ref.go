package restapi

import (
	"fmt"
	"net/url"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

type referenceSchemaUpdater struct {
	schemaManager database.SchemaManager
	network       libnetwork.Network
	fromClass     string
	kind          kind.Kind
}

func newReferenceSchemaUpdater(schemaManager database.SchemaManager, network libnetwork.Network,
	fromClass string, kind kind.Kind) *referenceSchemaUpdater {
	return &referenceSchemaUpdater{schemaManager, network, fromClass, kind}
}

// make sure this only ever called AFTER validtion as it skips
// validation checks itself
func (u *referenceSchemaUpdater) addNetworkDataTypes(schema interface{}) error {
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
	// single-ref
	parsed, err := url.Parse(*prop.LocationURL)
	if err != nil {
		return err
	}

	if parsed.Host == "localhost" {
		// local ref, nothign to do
		return nil
	}

	peers, err := u.network.ListPeers()
	if err != nil {
		return fmt.Errorf("could not list network peers: %s", err)
	}

	remoteKind, err := peers.RemoteKind(crossrefs.NetworkKind{
		Kind:     u.kind,
		ID:       prop.NrDollarCref,
		PeerName: parsed.Host,
	})
	if err != nil {
		return fmt.Errorf("invalid network reference: %s", err)
	}

	switch thingOrAction := remoteKind.(type) {
	case models.Thing:
		remoteClass := fmt.Sprintf("%s/%s", parsed.Host, thingOrAction.AtClass)
		err := u.schemaManager.UpdatePropertyAddDataType(u.kind, u.fromClass, propName, remoteClass)
		if err != nil {
			return fmt.Errorf("could not add network thing class %s to %s.%s", remoteClass, u.fromClass, propName)
		}
	case models.Action:
		remoteClass := fmt.Sprintf("%s/%s", parsed.Host, thingOrAction.AtClass)
		u.schemaManager.UpdatePropertyAddDataType(u.kind, u.fromClass, propName, remoteClass)
		if err != nil {
			return fmt.Errorf("could not add network action class %s to %s.%s", remoteClass, u.fromClass, propName)
		}
	default:
		return fmt.Errorf("unrecognized kind from remote peer for %s from %s", prop.NrDollarCref, parsed.Host)
	}

	return nil
}
