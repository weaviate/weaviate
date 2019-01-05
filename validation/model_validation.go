/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */

package validation

import (
	"context"
	"fmt"
	"net/url"

	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

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

// ValidateThingBody Validates a thing body using the 'ThingCreate' object.
func ValidateThingBody(ctx context.Context, thing *models.ThingCreate, databaseSchema schema.WeaviateSchema,
	dbConnector dbconnector.DatabaseConnector, network network.Network, serverConfig *config.WeaviateConfig,
	keyToken *models.KeyTokenGetResponse) error {
	// Validate the body
	bve := validateBody(thing.AtClass, thing.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ThingSchema.Schema, thing, connutils.RefTypeThing,
		dbConnector, network, serverConfig, keyToken)

	return sve
}

// ValidateActionBody Validates a action body using the 'ActionCreate' object.
func ValidateActionBody(ctx context.Context, action *models.ActionCreate, databaseSchema schema.WeaviateSchema,
	dbConnector dbconnector.DatabaseConnector, network network.Network, serverConfig *config.WeaviateConfig,
	keyToken *models.KeyTokenGetResponse) error {
	// Validate the body
	bve := validateBody(action.AtClass, action.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ActionSchema.Schema, action, connutils.RefTypeAction,
		dbConnector, network, serverConfig, keyToken)

	return sve
}

// validateBody Validates the overlapping body values
func validateBody(class string, context string) error {
	// If the given class is empty, return an error
	if class == "" {
		return fmt.Errorf(ErrorMissingClass)
	}

	// If the given context is empty, return an error
	if context == "" {
		return fmt.Errorf(ErrorMissingContext)
	}

	// No error
	return nil
}

// validateRefType validates the reference type with one of the existing reference types
func validateRefType(s connutils.RefType) bool {
	return (s == connutils.RefTypeAction ||
		s == connutils.RefTypeThing ||
		s == connutils.RefTypeKey ||
		s == connutils.RefTypeNetworkThing ||
		s == connutils.RefTypeNetworkAction)
}

// ValidateSingleRef validates a single ref based on location URL and existence of the object in the database
func ValidateSingleRef(ctx context.Context, serverConfig *config.WeaviateConfig, cref *models.SingleRef, dbConnector dbconnector.DatabaseConnector, network network.Network, errorVal string, keyToken *models.KeyTokenGetResponse) error {
	// Init reftype
	refType := connutils.RefType(cref.Type)
	url, err := url.Parse(*cref.LocationURL)
	if err != nil {
		return fmt.Errorf("could not parse location url (%s): %s", *cref.LocationURL, err)
	}

	// Per convention a local ref must point to host 'localhost', regardless of
	// how the local instance is identified via a host alias or FQDN.
	if url.Host != "localhost" {
		peers, err := network.ListPeers()
		if err != nil {
			return fmt.Errorf("could not validate network reference: could not list network peers: %s", err)
		}

		var k kind.Kind
		switch refType {
		case connutils.RefTypeNetworkThing:
			k = kind.THING_KIND
		case connutils.RefTypeNetworkAction:
			k = kind.ACTION_KIND
		default:
			return fmt.Errorf("unrecognized network reference type '%s' for cref %#v at %s, "+
				"must be one of: %s, %s", refType, cref, url.String(), connutils.RefTypeNetworkThing,
				connutils.RefTypeNetworkAction)
		}

		_, err = peers.RemoteKind(crossrefs.NetworkKind{Kind: k, ID: cref.NrDollarCref, PeerName: url.Host})
		if err != nil {
			return fmt.Errorf("invalid network reference: %s", err)
		}
	} else {
		// Check whether the given Object exists in the DB
		var err error
		if refType == connutils.RefTypeThing {
			obj := &models.ThingGetResponse{}
			err = dbConnector.GetThing(ctx, cref.NrDollarCref, obj)
		} else if refType == connutils.RefTypeAction {
			obj := &models.ActionGetResponse{}
			err = dbConnector.GetAction(ctx, cref.NrDollarCref, obj)
		} else if refType == connutils.RefTypeKey {
			obj := &models.KeyGetResponse{}
			err = dbConnector.GetKey(ctx, cref.NrDollarCref, obj)
		} else {
			return fmt.Errorf(ErrorInvalidCRefType, cref.Type)
		}

		if err != nil {
			return fmt.Errorf(ErrorNotFoundInDatabase, cref.Type, err, errorVal)
		}
	}

	return nil
}

func ValidateMultipleRef(ctx context.Context, serverConfig *config.WeaviateConfig,
	refs *models.MultipleRef, dbConnector dbconnector.DatabaseConnector, network network.Network,
	errorVal string, keyToken *models.KeyTokenGetResponse) error {
	if refs == nil {
		return nil
	}

	for _, ref := range *refs {
		err := ValidateSingleRef(ctx, serverConfig, ref, dbConnector, network, errorVal, keyToken)
		if err != nil {
			return err
		}
	}
	return nil
}
