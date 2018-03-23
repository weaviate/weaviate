/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package validation

import (
	"context"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
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
func ValidateThingBody(ctx context.Context, thing *models.ThingCreate, databaseSchema schema.WeaviateSchema, dbConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig, keyToken *models.KeyTokenGetResponse) error {
	// Validate the body
	bve := validateBody(thing.AtClass, thing.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ThingSchema.Schema, thing, connutils.RefTypeThing, dbConnector, serverConfig, keyToken)

	return sve
}

// ValidateActionBody Validates a action body using the 'ActionCreate' object.
func ValidateActionBody(ctx context.Context, action *models.ActionCreate, databaseSchema schema.WeaviateSchema, dbConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig, keyToken *models.KeyTokenGetResponse) error {
	// Validate the body
	bve := validateBody(action.AtClass, action.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Check whether the Things exist
	if action.Things == nil {
		return fmt.Errorf(ErrorMissingActionThings)
	}

	// Check whether the Object exist in the JSON
	if action.Things.Object == nil {
		return fmt.Errorf(ErrorMissingActionThingsObject)
	}

	// Check whether the Subject exist in the JSON
	if action.Things.Subject == nil {
		return fmt.Errorf(ErrorMissingActionThingsSubject)
	}

	// Check whether the Object has a location
	if action.Things.Object.LocationURL == nil {
		return fmt.Errorf(ErrorMissingActionThingsObjectLocation)
	}

	// Check whether the Object has a type
	if action.Things.Object.Type == "" {
		return fmt.Errorf(ErrorMissingActionThingsObjectType)
	}

	// Check whether the Object reference type exists
	if !validateRefType(connutils.RefType(action.Things.Object.Type)) {
		return fmt.Errorf(
			ErrorInvalidActionThingsObjectType,
			connutils.RefTypeAction,
			connutils.RefTypeThing,
			connutils.RefTypeKey,
		)
	}

	// Check whether the Subject has a location
	if action.Things.Subject.LocationURL == nil {
		return fmt.Errorf(ErrorMissingActionThingsSubjectLocation)
	}

	// Check whether the Subject has a type
	if action.Things.Subject.Type == "" {
		return fmt.Errorf(ErrorMissingActionThingsSubjectType)
	}

	// Check whether the Subject reference type exists
	if !validateRefType(connutils.RefType(action.Things.Subject.Type)) {
		return fmt.Errorf(
			ErrorInvalidActionThingsSubjectType,
			connutils.RefTypeAction,
			connutils.RefTypeThing,
			connutils.RefTypeKey,
		)
	}

	// Check existance of Thing, external or internal
	if err := ValidateSingleRef(ctx, serverConfig, action.Things.Object, dbConnector, "Object-Thing", keyToken); err != nil {
		return err
	}

	// Check existance of Thing, external or internal
	if err := ValidateSingleRef(ctx, serverConfig, action.Things.Subject, dbConnector, "Subject-Thing", keyToken); err != nil {
		return err
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(ctx, databaseSchema.ActionSchema.Schema, action, connutils.RefTypeAction, dbConnector, serverConfig, keyToken)

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
	return (s == connutils.RefTypeAction || s == connutils.RefTypeThing || s == connutils.RefTypeKey)
}

// ValidateSingleRef validates a single ref based on location URL and existance of the object in the database
func ValidateSingleRef(ctx context.Context, serverConfig *config.WeaviateConfig, cref *models.SingleRef, dbConnector dbconnector.DatabaseConnector, errorVal string, keyToken *models.KeyTokenGetResponse) error {
	// Init reftype
	refType := connutils.RefType(cref.Type)

	// Check existance of Object, external or internal
	if serverConfig.GetHostAddress() != *cref.LocationURL {
		// Search for key-information for resolving this part. Dont validate if not exists
		instance, err := serverConfig.GetInstance(*cref.LocationURL, keyToken)
		if err != nil {
			return fmt.Errorf(ErrorNoExternalCredentials, *cref.LocationURL, errorVal)
		}

		// Set endpoint
		endpoint := ""

		if refType == connutils.RefTypeThing {
			endpoint = "things"
		} else if refType == connutils.RefTypeAction {
			endpoint = "actions"
		} else if refType == connutils.RefTypeKey {
			endpoint = "keys"
		}

		// Check wheter the Object's location URL is pointing to a existing Weaviate instance
		response, err := connutils.DoExternalRequest(instance, endpoint, cref.NrDollarCref)
		if err != nil {
			return fmt.Errorf(ErrorExternalNotFound, *cref.LocationURL, response.StatusCode, errorVal)
		}
	} else {
		// Check whether the given Object exists in the DB
		var err error
		if refType == connutils.RefTypeThing {
			obj := &models.ThingGetResponse{}
			err = dbConnector.GetThing(ctx, cref.NrDollarCref, obj)
		} else if refType == connutils.RefTypeAction {
			obj := &models.ActionGetResponse{}
			err = dbConnector.GetAction(cref.NrDollarCref, obj)
		} else if refType == connutils.RefTypeKey {
			obj := &models.KeyGetResponse{}
			err = dbConnector.GetKey(cref.NrDollarCref, obj)
		} else {
			return fmt.Errorf(ErrorInvalidCRefType, cref.Type)
		}

		if err != nil {
			return fmt.Errorf(ErrorNotFoundInDatabase, cref.Type, err, errorVal)
		}
	}

	return nil
}
