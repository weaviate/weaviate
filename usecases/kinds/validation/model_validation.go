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

package validation

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

type getRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error
}

type exists func(context.Context, kind.Kind, strfmt.UUID) (bool, error)

type peerLister interface {
	ListPeers() (peers.Peers, error)
}

const (
	// ErrorMissingActionThings message
	ErrorMissingActionThings string = "no things, object and subject, are added. Add 'things' by using the 'things' key in the root of the JSON"
	// ErrorMissingActionThingsObject message
	ErrorMissingActionThingsObject string = "no object-thing is added. Add the 'object' inside the 'things' part of the JSON"
	// ErrorMissingActionThingsSubject message
	ErrorMissingActionThingsSubject string = "no subject-thing is added. Add the 'subject' inside the 'things' part of the JSON"
	// ErrorMissingActionThingsObjectLocation message
	ErrorMissingActionThingsObjectLocation string = "no 'locationURL' is found in the object-thing. Add the 'locationURL' inside the 'object-thing' part of the JSON"
	// ErrorMissingActionThingsObjectType message
	ErrorMissingActionThingsObjectType string = "no 'type' is found in the object-thing. Add the 'type' inside the 'object-thing' part of the JSON"
	// ErrorInvalidActionThingsObjectType message
	ErrorInvalidActionThingsObjectType string = "object-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMissingActionThingsSubjectLocation message
	ErrorMissingActionThingsSubjectLocation string = "no 'locationURL' is found in the subject-thing. Add the 'locationURL' inside the 'subject-thing' part of the JSON"
	// ErrorMissingActionThingsSubjectType message
	ErrorMissingActionThingsSubjectType string = "no 'type' is found in the subject-thing. Add the 'type' inside the 'subject-thing' part of the JSON"
	// ErrorInvalidActionThingsSubjectType message
	ErrorInvalidActionThingsSubjectType string = "subject-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMissingClass message
	ErrorMissingClass string = "the given class is empty"
	// ErrorMissingContext message
	ErrorMissingContext string = "the given context is empty"
	// ErrorNoExternalCredentials message
	ErrorNoExternalCredentials string = "no credentials available for the Weaviate instance for %s given in the %s"
	// ErrorExternalNotFound message
	ErrorExternalNotFound string = "given statuscode of '%s' is '%d', but 200 was expected for LocationURL given in the %s"
	// ErrorInvalidCRefType message
	ErrorInvalidCRefType string = "'cref' type '%s' does not exists"
	// ErrorNotFoundInDatabase message
	ErrorNotFoundInDatabase string = "error finding the '%s' in the database: '%s' at %s"
)

// ValidateThingBody Validates a thing body using the 'Thing' object.
func ValidateThingBody(ctx context.Context, thing *models.Thing, databaseSchema schema.WeaviateSchema,
	exists exists, network peerLister, serverConfig *config.WeaviateConfig) error {
	// Validate the body
	bve := validateBody(thing.Class)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ThingSchema.Schema, thing, kind.Thing,
		exists, network, serverConfig)

	return sve
}

// ValidateActionBody Validates a action body using the 'Action' object.
func ValidateActionBody(ctx context.Context, action *models.Action, databaseSchema schema.WeaviateSchema,
	exists exists, network peerLister, serverConfig *config.WeaviateConfig,
) error {
	// Validate the body
	bve := validateBody(action.Class)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ActionSchema.Schema, action, kind.Action,
		exists, network, serverConfig)

	return sve
}

// validateBody Validates the overlapping body values
func validateBody(class string) error {
	// If the given class is empty, return an error
	if class == "" {
		return fmt.Errorf(ErrorMissingClass)
	}

	// No error
	return nil
}

// validateRefType validates the reference type with one of the existing reference types
func validateRefType(s string) bool {
	return (s == "things" || s == "actions")
}

// ValidateSingleRef validates a single ref based on location URL and existence of the object in the database
func ValidateSingleRef(ctx context.Context, serverConfig *config.WeaviateConfig, cref *models.SingleRef,
	exists exists, network peerLister, errorVal string) error {

	ref, err := crossref.ParseSingleRef(cref)
	if err != nil {
		return fmt.Errorf("invalid reference: %s", err)
	}

	if !ref.Local {
		return validateNetworkRef(network, ref)
	}

	return validateLocalRef(ctx, exists, ref, errorVal)
}

func validateLocalRef(ctx context.Context, exists exists, ref *crossref.Ref, errorVal string) error {
	// Check whether the given Object exists in the DB
	var err error

	ok, err := exists(ctx, ref.Kind, ref.TargetID)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf(ErrorNotFoundInDatabase, ref.Kind.Name(), err, errorVal)
	}

	return nil
}

func validateNetworkRef(network peerLister, ref *crossref.Ref) error {
	// Network ref
	peers, err := network.ListPeers()
	if err != nil {
		return fmt.Errorf("could not validate network reference: could not list network peers: %s", err)
	}

	_, err = peers.RemoteKind(crossrefs.NetworkKind{Kind: ref.Kind, ID: ref.TargetID, PeerName: ref.PeerName})
	if err != nil {
		return fmt.Errorf("invalid network reference: %s", err)
	}

	return nil
}

func ValidateMultipleRef(ctx context.Context, serverConfig *config.WeaviateConfig,
	refs *models.MultipleRef, exists exists, network peerLister,
	errorVal string) error {
	if refs == nil {
		return nil
	}

	for _, ref := range *refs {
		err := ValidateSingleRef(ctx, serverConfig, ref, exists, network, errorVal)
		if err != nil {
			return err
		}
	}
	return nil
}
