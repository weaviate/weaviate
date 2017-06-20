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
	"github.com/go-openapi/swag"
	"io/ioutil"
	"log"
)

const defaultConfigFile string = "~/weaviate.conf.json"
const defaultEnvironment string = "development"

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
	Name string `json:"name"`
	// DatabaseConfig interface{} `json:"database_config"`
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

// GetDatabaseConnector creates a database connector
func GetDatabaseConnector(flags *swag.CommandLineOptionsGroup) string {
	// Get command line flags
	configEnvironment := flags.Options.(*ConfigFlags).ConfigSection
	configFileName := flags.Options.(*ConfigFlags).ConfigFile

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

	var configFile ConfigFile
	json.Unmarshal(file, &configFile)

	for _, env := range configFile.Environments {
		if env.Name == configEnvironment {
			return env.Database.Name
		}
	}

	log.Println("INFO: Using default database 'memory'.")
	return "memory"
}
