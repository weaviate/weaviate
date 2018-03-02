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
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// ValidateThingBody Validates a thing body using the 'ThingCreate' object.
func ValidateThingBody(thing *models.ThingCreate, databaseSchema schema.WeaviateSchema, dbConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig) error {
	// Validate the body
	bve := validateBody(thing.AtClass, thing.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(databaseSchema.ThingSchema.Schema, &thing.Schema, thing.AtClass, dbConnector, serverConfig)

	return sve
}

// ValidateActionBody Validates a action body using the 'ActionCreate' object.
func ValidateActionBody(action *models.ActionCreate, databaseSchema schema.WeaviateSchema, dbConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig) error {
	// Validate the body
	bve := validateBody(action.AtClass, action.AtContext)

	// Return error if possible
	if bve != nil {
		return bve
	}

	// Check whether the Things exist
	if action.Things == nil {
		return fmt.Errorf("no things, object and subject, are added. Add 'things' by using the 'things' key in the root of the JSON")
	}

	// Check whether the Object exist in the JSON
	if action.Things.Object == nil {
		return fmt.Errorf("no object-thing is added. Add the 'object' inside the 'things' part of the JSON")
	}

	// Check whether the Subject exist in the JSON
	if action.Things.Subject == nil {
		return fmt.Errorf("no subject-thing is added. Add the 'subject' inside the 'things' part of the JSON")
	}

	// Check whether the Object has a location
	if action.Things.Object.LocationURL == nil {
		return fmt.Errorf("no 'locationURL' is found in the object-thing. Add the 'locationURL' inside the 'object-thing' part of the JSON")
	}

	// Check whether the Object has a type
	if action.Things.Object.Type == "" {
		return fmt.Errorf("no 'type' is found in the object-thing. Add the 'type' inside the 'object-thing' part of the JSON")
	}

	// Check whether the Object reference type exists
	if !validateRefType(action.Things.Object.Type) {
		return fmt.Errorf(
			"object-thing requires one of the following values in 'type': '%s', '%s' or '%s'",
			connutils.RefTypeAction,
			connutils.RefTypeThing,
			connutils.RefTypeKey,
		)
	}

	// Check whether the Subject has a location
	if action.Things.Subject.LocationURL == nil {
		return fmt.Errorf("no 'locationURL' is found in the subject-thing. Add the 'locationURL' inside the 'subject-thing' part of the JSON")
	}

	// Check whether the Subject has a type
	if action.Things.Subject.Type == "" {
		return fmt.Errorf("no 'type' is found in the subject-thing. Add the 'type' inside the 'subject-thing' part of the JSON")
	}

	// Check whether the Subject reference type exists
	if !validateRefType(action.Things.Subject.Type) {
		return fmt.Errorf(
			"subject-thing requires one of the following values in 'type': '%s', '%s' or '%s'",
			connutils.RefTypeAction,
			connutils.RefTypeThing,
			connutils.RefTypeKey,
		)
	}

	// Check existance of Thing, external or internal
	if serverConfig.GetHostAddress() != *action.Things.Object.LocationURL {
		// Search for key-information for resolving this part. Dont validate if not exists
		instance, err := serverConfig.GetInstance(*action.Things.Object.LocationURL)
		if err != nil {
			return fmt.Errorf("no credentials available for the Weaviate instance for %s given in the subject-thing", *action.Things.Object.LocationURL)
		}

		// Check wheter the Object's location URL is pointing to a existing Weaviate instance
		response, err := connutils.DoExternalRequest(instance, "things", action.Things.Object.NrDollarCref)
		if err != nil {
			return fmt.Errorf("given statuscode of '%s' is '%d', but 200 was expected for LocationURL given in the subject-thing", *action.Things.Object.LocationURL, response.StatusCode)
		}
	} else {
		// Check whether the given Object exists in the DB
		ot := &models.ThingGetResponse{}
		ote := dbConnector.GetThing(action.Things.Object.NrDollarCref, ot)
		if ote != nil {
			return fmt.Errorf("error finding the 'Object'-thing in the database: %s", ote)
		}
	}

	// Check existance of Thing, external or internal
	if serverConfig.GetHostAddress() != *action.Things.Subject.LocationURL {
		// Search for key-information for resolving this part. Dont validate if not exists
		instance, err := serverConfig.GetInstance(*action.Things.Subject.LocationURL)
		if err != nil {
			return fmt.Errorf("no credentials available for the Weaviate instance for %s given in the subject-thing", *action.Things.Subject.LocationURL)
		}

		// Check wheter the Subject's location URL is pointing to a existing Weaviate instance
		response, err := connutils.DoExternalRequest(instance, "things", action.Things.Subject.NrDollarCref)
		if err != nil {
			return fmt.Errorf("given statuscode of '%s' is '%d', but 200 was expected for LocationURL given in the subject-thing", *action.Things.Subject.LocationURL, response.StatusCode)
		}
	} else {
		// Check whether the given Subject exists in the DB
		ot := &models.ThingGetResponse{}
		ote := dbConnector.GetThing(action.Things.Subject.NrDollarCref, ot)
		if ote != nil {
			return fmt.Errorf("error finding the 'Subject'-thing in the database: %s", ote)
		}
	}

	// Return the schema validation error
	sve := ValidateSchemaInBody(databaseSchema.ActionSchema.Schema, &action.Schema, action.AtClass, dbConnector, serverConfig)

	return sve
}

// validateBody Validates the overlapping body values
func validateBody(class string, context string) error {
	// If the given class is empty, return an error
	if class == "" {
		return fmt.Errorf("the given class is empty")
	}

	// If the given context is empty, return an error
	if context == "" {
		return fmt.Errorf("the given context is empty")
	}

	// No error
	return nil
}

// validateRefType validates the reference type with one of the existing reference types
func validateRefType(s string) bool {
	return (s == connutils.RefTypeAction || s == connutils.RefTypeThing || s == connutils.RefTypeKey)
}
