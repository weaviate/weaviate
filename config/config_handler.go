/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/go-openapi/swag"

	"github.com/creativesoftwarefdn/weaviate/messages"
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
	Name          string        `json:"name"`
	Database      Database      `json:"database"`
	Schemas       Schemas       `json:"schemas"`
	Broker        Broker        `json:"broker"`
	Network       *Network      `json:"network"`
	Limit         int64         `json:"limit"`
	Debug         bool          `json:"debug"`
	Development   Development   `json:"development"`
	Contextionary Contextionary `json:"contextionary"`
	ConfigStore   ConfigStore   `json:"configuration_storage"`
	Telemetry     Telemetry     `json:"telemetry"`
}

type Contextionary struct {
	KNNFile      string `json:"knn_file"`
	IDXFile      string `json:"idx_file"`
	failOnGerund bool   `json:"fail_ongerund"` // is false by default.
}

type Network struct {
	GenesisURL string `json:"genesis_url"`
	PublicURL  string `json:"public_url"`
	PeerName   string `json:"peer_name"`
}

type ConfigStore struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type Telemetry struct {
	Enabled  bool   `json:"enabled"`
	Interval int    `json:"interval"`
	URL      string `json:"url"`
}

// Broker checks if broker details are set
type Broker struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
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

// Development is the outline of (temporary) config variables
// Note: the purpose is that these variables will be moved somewhere else in time
type Development struct {
	ExternalInstances []Instance `json:"external_instances"`
}

// Instance is the outline for an external instance whereto crossreferences can be resolved
type Instance struct {
	URL      string `json:"url"`
	APIKey   string `json:"api_key"`
	APIToken string `json:"api_token"`
}

// GetConfigOptionGroup creates a option group for swagger
func GetConfigOptionGroup() *swag.CommandLineOptionsGroup {
	commandLineOptionsGroup := swag.CommandLineOptionsGroup{
		ShortDescription: "Connector config & MQTT config",
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
		return fmt.Errorf("error unmarshalling the config file: %s", err)
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

// GetInstance from config
func (f *WeaviateConfig) GetInstance(hostname string) (instance Instance, err error) {
	err = nil

	found := false

	// For each instance, check if hostname is the same
	for _, v := range f.Environment.Development.ExternalInstances {
		if hostname == v.URL {
			instance = v
			found = true
			break
		}
	}

	if !found {
		// Didn't find item in list
		err = errors.New("can't find key for given instance")
		return
	}

	return
}
