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
	"log"

	"github.com/weaviate/weaviate/connectors/datastore"
	"github.com/weaviate/weaviate/connectors/memory"
	"github.com/weaviate/weaviate/connectors/utils"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	Add(connector_utils.DatabaseObject) (string, error)
	Get(string) (connector_utils.DatabaseObject, error)
	List(string, int, int, *connector_utils.ObjectReferences) (connector_utils.DatabaseObjects, int64, error)
	ValidateKey(string) ([]connector_utils.DatabaseUsersObject, error)
	AddKey(string, connector_utils.DatabaseUsersObject) (connector_utils.DatabaseUsersObject, error)
	GetName() string
}

// CreateDatabaseConnector Creates a database connector with name given
func CreateDatabaseConnector(databaseConnectorName string) DatabaseConnector {
	// Make default database if name is not found
	var defaultDatabase DatabaseConnector = &memory.Memory{}

	// Set all existing connectors
	connectors := []DatabaseConnector{
		&datastore.Datastore{},
		defaultDatabase, // Also memory-database
	}

	// Loop through all connectors and determine its name
	for _, connector := range connectors {
		if connector.GetName() == databaseConnectorName {
			return connector
		}
	}

	// Return default Database
	log.Println("INFO: Using default database '" + defaultDatabase.GetName() + "', because '" + databaseConnectorName + "' does not exist.")
	return defaultDatabase
}
