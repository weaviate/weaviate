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
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// DefaultConfigFile is the default file when no config file is provided
const DefaultConfigFile string = "./weaviate.conf.json"

// Flags are input options
type Flags struct {
	ConfigFile string `long:"config-file" description:"path to config file (default: ./weaviate.conf.json)"`
}

// Telemetry gives the outline of the telemetry parameters in the config file
type Telemetry struct {
	RemoteURL string `json:"remote_url" yaml:"remote_url"`
	Interval  int    `json:"interval" yaml:"interval"`
	Disabled  bool   `json:"disabled" yaml:"disabled"`
}

// Config outline of the config file
type Config struct {
	Name                 string          `json:"name" yaml:"name"`
	AnalyticsEngine      AnalyticsEngine `json:"analytics_engine" yaml:"analytics_engine"`
	Database             Database        `json:"database" yaml:"database"`
	Network              *Network        `json:"network" yaml:"network"`
	Debug                bool            `json:"debug" yaml:"debug"`
	QueryDefaults        QueryDefaults   `json:"query_defaults" yaml:"query_defaults"`
	Contextionary        Contextionary   `json:"contextionary" yaml:"contextionary"`
	ConfigurationStorage ConfigStore     `json:"configuration_storage" yaml:"configuration_storage"`
	Authentication       Authentication  `json:"authentication" yaml:"authentication"`
	Telemetry            Telemetry       `json:"telemetry" yaml:"telemetry"`
}

// QueryDefaults for optional parameters
type QueryDefaults struct {
	Limit int64 `json:"limit" yaml:"limit"`
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

// Database is the outline of the database
type Database struct {
	Name           string      `json:"name" yaml:"name"`
	DatabaseConfig interface{} `json:"database_config" yaml:"database_config"`
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
	Config   Config
	Hostname string
	Scheme   string
}

// GetHostAddress from config locations
func (f *WeaviateConfig) GetHostAddress() string {
	return fmt.Sprintf("%s://%s", f.Scheme, f.Hostname)
}

// LoadConfig from config locations
func (f *WeaviateConfig) LoadConfig(flags *swag.CommandLineOptionsGroup, logger logrus.FieldLogger) error {
	// Get command line flags
	configFileName := flags.Options.(*Flags).ConfigFile

	// Set default if not given
	if configFileName == "" {
		configFileName = DefaultConfigFile
		logger.WithField("action", "config_load").WithField("config_file_path", DefaultConfigFile).
			Info("no config file specified, using default")
	}

	// Read config file
	file, err := ioutil.ReadFile(configFileName)
	if err != nil {
		return errors.New("config file '" + configFileName + "' not found.")
	}

	config, err := f.parseConfigFile(file, configFileName)
	if err != nil {
		return err
	}

	f.Config = config

	if err := f.Config.Authentication.Validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	return nil
}

func (f *WeaviateConfig) parseConfigFile(file []byte, name string) (Config, error) {
	var config Config

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
