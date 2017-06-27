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
	"io/ioutil"
	"log"

	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/connectors/memory"
)

const defaultConfigFile string = "~/weaviate.conf.json"
const defaultEnvironment string = "development"
const defaultDatabaseName string = "memory"

// ConfigFlags are input options
type ConfigFlags struct {
	ConfigSection string `long:"config" description:"the section inside the config file that has to be used"`
	ConfigFile    string `long:"config-file" description:"path to config file (default: ~/weaviate.conf.json)"`
}

// ConfigFile gives the outline of the config file
type ConfigFile struct {
	Environments []Environment `json:"environments"`
}

// Environment outline of the environment inside the config file
type Environment struct {
	Name     string   `json:"name"`
	Database Database `json:"database"`
}

// Database is the outline of the database
type Database struct {
	Name           string      `json:"name"`
	DatabaseConfig interface{} `json:"database_config"`
}

// GetConfigOptionGroup creates a option group for swagger
func GetConfigOptionGroup() *swag.CommandLineOptionsGroup {
	commandLineOptionsGroup := swag.CommandLineOptionsGroup{
		ShortDescription: "Connector config usage:",
		LongDescription:  "",
		Options:          &ConfigFlags{},
	}

	return &commandLineOptionsGroup
}

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(flags *swag.CommandLineOptionsGroup) DatabaseConnector {
	// Get command line flags
	configEnvironment := flags.Options.(*ConfigFlags).ConfigSection
	configFileName := flags.Options.(*ConfigFlags).ConfigFile

	databaseName := defaultDatabaseName

	// Set default if not given
	if configFileName == "" {
		configFileName = defaultConfigFile
		log.Println("INFO: Using default file location '" + defaultConfigFile + "'.")
	}

	// Read config file
	file, e := ioutil.ReadFile(configFileName)
	if e != nil {
		log.Println("INFO: File '" + configFileName + "' not found.")
	}

	// Set default env if not given
	if e != nil || configEnvironment == "" {
		configEnvironment = defaultEnvironment
		log.Println("INFO: Using default environment '" + defaultEnvironment + "'.")
	}

	// Read from the config file and add it to an object
	var configFile ConfigFile
	json.Unmarshal(file, &configFile)

	// Loop through all values in object to see whether the given connection-name exists
	var databaseConfig interface{}
	foundName := false
	for _, env := range configFile.Environments {
		if env.Name == configEnvironment {
			databaseName = env.Database.Name
			foundName = true

			// Get config interface data
			databaseConfig = env.Database.DatabaseConfig
		}
	}

	// Return default database because no good config is found
	if !foundName {
		log.Println("INFO: Using default database '" + defaultDatabaseName + "'.")
		databaseName = defaultDatabaseName
	}

	// Make default database if name is not found
	var defaultDatabase DatabaseConnector = &memory.Memory{}

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
