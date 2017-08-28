/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dbconnector

import (
	"encoding/json"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/models"
	"io/ioutil"
	"log"

	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/connectors/config"
	"github.com/weaviate/weaviate/connectors/dgraph"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	GetName() string
	SetConfig(connectorConfig.Environment)

	AddThing(*models.Thing, strfmt.UUID) error // TODO: response: ThingGetResponse?
	GetThing(strfmt.UUID) (models.ThingGetResponse, error)
	ListThings(int, int) (models.ThingsListResponse, error)
	UpdateThing(*models.Thing, strfmt.UUID) error
	DeleteThing(strfmt.UUID) error

	AddAction(*models.Action, strfmt.UUID) error
	GetAction(strfmt.UUID) (models.ActionGetResponse, error)
	// ListActions(strfmt.UUID, int, int) (models.ActionsListResponse, error)
	UpdateAction(*models.Action, strfmt.UUID) error
}

// GetAllConnectors contains all available connectors
func GetAllConnectors() []DatabaseConnector {
	// Set all existing connectors
	connectors := []DatabaseConnector{
		&dgraph.Dgraph{},
	}

	return connectors
}

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(flags *swag.CommandLineOptionsGroup) DatabaseConnector {
	// Get command line flags
	configEnvironment := flags.Options.(*connectorConfig.ConfigFlags).ConfigSection
	configFileName := flags.Options.(*connectorConfig.ConfigFlags).ConfigFile

	databaseName := connectorConfig.DefaultDatabaseName

	// Set default if not given
	if configFileName == "" {
		configFileName = connectorConfig.DefaultConfigFile
		log.Println("INFO: Using default file location '" + connectorConfig.DefaultConfigFile + "'.")
	}

	// Read config file
	file, e := ioutil.ReadFile(configFileName)
	if e != nil {
		log.Println("INFO: File '" + configFileName + "' not found.")
	}

	// Set default env if not given
	if e != nil || configEnvironment == "" {
		configEnvironment = connectorConfig.DefaultEnvironment
		log.Println("INFO: Using default environment '" + connectorConfig.DefaultEnvironment + "'.")
	}

	// Read from the config file and add it to an object
	var configFile connectorConfig.ConfigFile
	json.Unmarshal(file, &configFile)

	// Loop through all values in object to see whether the given connection-name exists
	var databaseConfig connectorConfig.Environment
	foundName := false
	for _, env := range configFile.Environments {
		if env.Name == configEnvironment {
			databaseName = env.Database.Name
			foundName = true

			// Get config interface data
			databaseConfig = env
		}
	}

	// Return default database because no good config is found
	if !foundName {
		log.Println("INFO: Using default database '" + connectorConfig.DefaultDatabaseName + "'.")
		databaseName = connectorConfig.DefaultDatabaseName
	}

	// Make default database if name is not found
	var defaultDatabase DatabaseConnector = &dgraph.Dgraph{}

	// Get all connectors
	connectors := GetAllConnectors()

	// Loop through all connectors and determine its name
	for _, connector := range connectors {
		if connector.GetName() == databaseName {
			// Add config interface data to connector, so the connector could do it's own trick with it.
			connector.SetConfig(databaseConfig)
			return connector
		}
	}

	// Return default Database
	log.Println("INFO: Using default database '" + defaultDatabase.GetName() + "', because '" + databaseName + "' does not exist.")
	return defaultDatabase
}
