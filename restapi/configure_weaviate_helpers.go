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

// Package restapi with all rest API functions.
package restapi

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-openapi/swag"

	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
	dblisting "github.com/creativesoftwarefdn/weaviate/connectors/listing"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
)

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(env *config.Environment) dbconnector.DatabaseConnector {
	// Get all connectors
	connectors := dblisting.GetAllConnectors()
	cacheConnectors := dblisting.GetAllCacheConnectors()

	// Init the db-connector variable
	var connector dbconnector.DatabaseConnector

	// Loop through all connectors and determine its name
	for _, c := range connectors {
		if c.GetName() == env.Database.Name {
			messaging.InfoMessage(fmt.Sprintf("Using database '%s'", env.Database.Name))
			connector = c
			break
		}
	}

	// Loop through all cache-connectors and determine its name
	for _, cc := range cacheConnectors {
		if cc.GetName() == env.Cache.Name {
			messaging.InfoMessage(fmt.Sprintf("Using cache layer '%s'", env.Cache.Name))
			cc.SetDatabaseConnector(connector)
			connector = cc
			break
		}
	}

	return connector
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(messages ...string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// appends all error messages to the error
	for _, message := range messages {
		er.Error = append(er.Error, &models.ErrorResponseErrorItems0{
			Message: message,
		})
	}

	return er
}

func addLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serverConfig.Environment.Debug {
			log.Printf("Received request: %+v %+v\n", r.Method, r.URL)
		}
		next.ServeHTTP(w, r)
	})
}
