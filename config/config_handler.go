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

package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"

	"github.com/go-openapi/swag"
)

// DefaultConfigFile is the default file when no config file is provided
const DefaultConfigFile string = "./weaviate.conf.json"

// DefaultEnvironment is the default env when no env is provided
const DefaultEnvironment string = "development"

// Flags are input options
type Flags struct {
	ConfigSection string `long:"config" description:"the section inside the config file that has to be used"`
	ConfigFile    string `long:"config-file" description:"path to config file (default: ./weaviate.conf.json)"`
}

// File gives the outline of the config file
type File struct {
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
		ShortDescription: "Connector config usage",
		LongDescription:  "",
		Options:          &Flags{},
	}

	return &commandLineOptionsGroup
}

// WeaviateConfig represents the used schema's
type WeaviateConfig struct {
	Environment Environment
}

// LoadConfig from config locations
func (f *WeaviateConfig) LoadConfig(flags *swag.CommandLineOptionsGroup) error {
	// Get command line flags
	configEnvironment := flags.Options.(*Flags).ConfigSection
	configFileName := flags.Options.(*Flags).ConfigFile

	// Set default if not given
	if configFileName == "" {
		configFileName = DefaultConfigFile
		log.Println("INFO: Using default config file location '" + DefaultConfigFile + "'.")
	}

	// Read config file
	file, err := ioutil.ReadFile(configFileName)
	if err != nil {
		return errors.New("config file '" + configFileName + "' not found.")
	}

	// Set default env if not given
	if err != nil || configEnvironment == "" {
		configEnvironment = DefaultEnvironment
		log.Println("INFO: Using default environment '" + DefaultEnvironment + "'.")
	}

	// Read from the config file and add it to an object
	var configFile File
	err = json.Unmarshal(file, &configFile)

	// Return error if config file is incorrect
	if err != nil {
		return errors.New("error unmarshalling the config file. ")
	}

	// Loop through all values in object to see whether the given connection-name exists
	foundEnvironment := false
	for _, env := range configFile.Environments {
		if env.Name == configEnvironment {
			foundEnvironment = true

			// Get config interface data
			f.Environment = env
		}
	}

	// Return default database because no good config is found
	if !foundEnvironment {
		return errors.New("no environment found with name '" + configEnvironment + "'")
	}

	log.Println("INFO: Config file found, loading environment...")

	return nil
}
