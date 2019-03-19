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
	"regexp"

	"github.com/go-openapi/swag"
	"gopkg.in/yaml.v2"

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
	Name                 string          `json:"name" yaml:"name"`
	AnalyticsEngine      AnalyticsEngine `json:"analytics_engine" yaml:"analytics_engine"`
	Database             Database        `json:"database" yaml:"database"`
	Broker               Broker          `json:"broker" yaml:"broker"`
	Network              *Network        `json:"network" yaml:"network"`
	Limit                int64           `json:"limit" yaml:"limit"`
	Debug                bool            `json:"debug" yaml:"debug"`
	Development          Development     `json:"development" yaml:"development"`
	Contextionary        Contextionary   `json:"contextionary" yaml:"contextionary"`
	ConfigurationStorage ConfigStore     `json:"configuration_storage" yaml:"configuration_storage"`
}

type Contextionary struct {
	KNNFile string `json:"knn_file" yaml:"knn_file"`
	IDXFile string `json:"idx_file" yaml:"idx_file"`
}

// AnalyticsEngine represents an external analytics engine, such as Spark for
// Janusgraph
type AnalyticsEngine struct {
	// Enabled configures whether an analytics engine should be used. Setting
	// this to true leads to the options "useAnalyticsEngine" and
	// "forceRecalculate" to become available in the GraphQL GetMeta->Kind->Class
	// and Aggregate->Kind->Class.
	//
	// Important: If enabled is set to true, you must also configure an analytics
	// engine in your database connector. If the chosen connector does not
	// support an external analytics engine, enabled must be set to false.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// DefaultUseAnalyticsEngine configures what the "useAnalyticsEngine" in the
	// GraphQL API will default to when not set.
	DefaultUseAnalyticsEngine bool `json:"default_use_analytics_engine" yaml:"default_use_analytics_engine"`
}

type Network struct {
	GenesisURL string `json:"genesis_url" yaml:"genesis_url"`
	PublicURL  string `json:"public_url" yaml:"public_url"`
	PeerName   string `json:"peer_name" yaml:"peer_name"`
}

type ConfigStore struct {
	Type string `json:"type" yaml:"type"`
	URL  string `json:"url" yaml:"url"`
}

// Broker checks if broker details are set
type Broker struct {
	Host string `json:"host" yaml:"host"`
	Port int32  `json:"port" yaml:"port"`
}

// Database is the outline of the database
type Database struct {
	Name           string      `json:"name" yaml:"name"`
	DatabaseConfig interface{} `json:"database_config" yaml:"database_config"`
}

// Development is the outline of (temporary) config variables
// Note: the purpose is that these variables will be moved somewhere else in time
type Development struct {
	ExternalInstances []Instance `json:"external_instances" yaml:"external_instances"`
}

// Instance is the outline for an external instance whereto crossreferences can be resolved
type Instance struct {
	URL      string `json:"url" yaml:"url"`
	APIKey   string `json:"api_key" yaml:"api_key"`
	APIToken string `json:"api_token" yaml:"api_token"`
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

	configFile, err := f.parseConfigFile(file, configFileName)
	if err != nil {
		return err
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

func (f *WeaviateConfig) parseConfigFile(file []byte, name string) (File, error) {
	var config File

	m := regexp.MustCompile(".*\\.(\\w+)$").FindStringSubmatch(name)
	if len(m) < 2 {
		return config, fmt.Errorf("config file does not have a file ending, got '%s'", name)
	}

	switch m[1] {
	case "json":
		err := json.Unmarshal(file, &config)
		if err != nil {
			return config, fmt.Errorf("error unmarshalling the json config file: %s", err)
		}
	case "yaml":
		err := yaml.Unmarshal(file, &config)
		if err != nil {
			return config, fmt.Errorf("error unmarshalling the yaml config file: %s", err)
		}
	default:
		return config, fmt.Errorf("unsupported config file extension '%s', use .yaml or .json", m[1])
	}

	return config, nil
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
