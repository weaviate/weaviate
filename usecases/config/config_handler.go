//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"

	"github.com/go-openapi/swag"
	"github.com/semi-technologies/weaviate/deprecations"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// DefaultConfigFile is the default file when no config file is provided
const DefaultConfigFile string = "./weaviate.conf.json"

// Flags are input options
type Flags struct {
	ConfigFile string `long:"config-file" description:"path to config file (default: ./weaviate.conf.json)"`
}

// Config outline of the config file
type Config struct {
	Name           string         `json:"name" yaml:"name"`
	Debug          bool           `json:"debug" yaml:"debug"`
	QueryDefaults  QueryDefaults  `json:"query_defaults" yaml:"query_defaults"`
	Contextionary  Contextionary  `json:"contextionary" yaml:"contextionary"`
	Authentication Authentication `json:"authentication" yaml:"authentication"`
	Authorization  Authorization  `json:"authorization" yaml:"authorization"`
	Origin         string         `json:"origin" yaml:"origin"`
	Persistence    Persistence    `json:"persistence" yaml:"persistence"`
}

// Validate the non-nested parameters. Nested objects must provide their own
// validation methods
func (c Config) Validate() error {
	return nil
}

// QueryDefaults for optional parameters
type QueryDefaults struct {
	Limit int64 `json:"limit" yaml:"limit"`
}

type Contextionary struct {
	URL string `json:"url" yaml:"url"`
}

type Persistence struct {
	DataPath string `json:"dataPath" yaml:"dataPath"`
}

func (p Persistence) Validate() error {
	if p.DataPath == "" {
		return fmt.Errorf("persistence.dataPath must be set")
	}

	return nil
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
	_ = err // explicitly ignore

	if len(file) > 0 {
		config, err := f.parseConfigFile(file, configFileName)
		if err != nil {
			return err
		}
		f.Config = config

		deprecations.Log(logger, "config-files")
	}

	if err := FromEnv(&f.Config); err != nil {
		return err
	}

	if err := f.Config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if err := f.Config.Authentication.Validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if err := f.Config.Authorization.Validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if err := f.Config.Persistence.Validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	return nil
}

func (f *WeaviateConfig) parseConfigFile(file []byte, name string) (Config, error) {
	var config Config

	m := regexp.MustCompile(`.*\.(\w+)$`).FindStringSubmatch(name)
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
