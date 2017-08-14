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

package connectorConfig

import (
	"github.com/go-openapi/swag"
)

const DefaultConfigFile string = "~/weaviate.conf.json"
const DefaultEnvironment string = "development"
const DefaultDatabaseName string = "memory"

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
	Name        string   `json:"name"`
	Database    Database `json:"database"`
	Schemas     Schemas  `json:"schemas"`
	MQTTEnabled bool     `json:"mqttEnabled"`
}

// Database is the outline of the database
type Database struct {
	Name           string      `json:"name"`
	DatabaseConfig interface{} `json:"database_config"`
}

// Schemas contains the schema for 'things' and for 'actions'
type Schemas struct {
	Thing  string `json:"thing"`
	Action string `json:"action"`
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
