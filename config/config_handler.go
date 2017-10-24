/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/messages"
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
	Cache       Cache    `json:"cache"`
	Debug       bool     `json:"debug"`
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

// Cache is the outline of the cache-system
type Cache struct {
	Name string `json:"name"`
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
	Hostname    string
	Scheme      string
}

// GetHostAddress from config locations
func (f *WeaviateConfig) GetHostAddress() string {
	return fmt.Sprintf("%s://%s", f.Scheme, f.Hostname)
}

// LoadConfig from config locations
func (f *WeaviateConfig) LoadConfig(flags *swag.CommandLineOptionsGroup, m *messages.Messaging) error {
	// Get command line flags
	configEnvironment := flags.Options.(*Flags).ConfigSection
	configFileName := flags.Options.(*Flags).ConfigFile

	// Set default if not given
	if configFileName == "" {
		configFileName = DefaultConfigFile
		m.InfoMessage("Using default config file location '" + DefaultConfigFile + "'.")
	}

	// Read config file
	file, err := ioutil.ReadFile(configFileName)
	if err != nil {
		return errors.New("config file '" + configFileName + "' not found.")
	}

	// Set default env if not given
	if err != nil || configEnvironment == "" {
		configEnvironment = DefaultEnvironment
		m.InfoMessage("Using default environment '" + DefaultEnvironment + "'.")
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

	m.InfoMessage("Config file found, loading environment..")

	// Check the debug mode
	m.Debug = f.Environment.Debug
	if f.Environment.Debug {
		m.InfoMessage("Running in DEBUG-mode")
	}

	return nil
}
