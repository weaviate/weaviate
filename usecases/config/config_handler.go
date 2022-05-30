//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
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
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/deprecations"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// DefaultConfigFile is the default file when no config file is provided
const DefaultConfigFile string = "./weaviate.conf.json"

// DefaultCleanupIntervalSeconds can be overwritten on a per-class basis
const DefaultCleanupIntervalSeconds = int64(60)

const (
	// These BM25 tuning params can be overwritten on a per-class basis
	DefaultBM25k1 = float32(1.2)
	DefaultBM25b  = float32(0.75)
)

const (
	DefaultDiskUseWarningPercentage  = uint64(80)
	DefaultDiskUseReadonlyPercentage = uint64(90)
)

// Flags are input options
type Flags struct {
	ConfigFile string `long:"config-file" description:"path to config file (default: ./weaviate.conf.json)"`
}

// Config outline of the config file
type Config struct {
	Name                    string         `json:"name" yaml:"name"`
	Debug                   bool           `json:"debug" yaml:"debug"`
	QueryDefaults           QueryDefaults  `json:"query_defaults" yaml:"query_defaults"`
	QueryMaximumResults     int64          `json:"query_maximum_results" yaml:"query_maximum_results"`
	Contextionary           Contextionary  `json:"contextionary" yaml:"contextionary"`
	Authentication          Authentication `json:"authentication" yaml:"authentication"`
	Authorization           Authorization  `json:"authorization" yaml:"authorization"`
	Origin                  string         `json:"origin" yaml:"origin"`
	Persistence             Persistence    `json:"persistence" yaml:"persistence"`
	DefaultVectorizerModule string         `json:"default_vectorizer_module" yaml:"default_vectorizer_module"`
	EnableModules           string         `json:"enable_modules" yaml:"enable_modules"`
	ModulesPath             string         `json:"modules_path" yaml:"modules_path"`
	AutoSchema              AutoSchema     `json:"auto_schema" yaml:"auto_schema"`
	Cluster                 cluster.Config `json:"cluster" yaml:"cluster"`
	Monitoring              Monitoring     `json:"monitoring" yaml:"monitoring"`
	DiskUse                 DiskUse        `json:"disk_use" yaml:"disk_use"`
}

type moduleProvider interface {
	ValidateVectorizer(moduleName string) error
}

// Validate the non-nested parameters. Nested objects must provide their own
// validation methods
func (c Config) Validate(modProv moduleProvider) error {
	if err := c.validateDefaultVectorizerModule(modProv); err != nil {
		return errors.Wrap(err, "default vectorizer module")
	}

	return nil
}

func (c Config) validateDefaultVectorizerModule(modProv moduleProvider) error {
	if c.DefaultVectorizerModule == VectorizerModuleNone {
		return nil
	}

	return modProv.ValidateVectorizer(c.DefaultVectorizerModule)
}

type AutoSchema struct {
	Enabled       bool   `json:"enabled" yaml:"enabled"`
	DefaultString string `json:"defaultString" yaml:"defaultString"`
	DefaultNumber string `json:"defaultNumber" yaml:"defaultNumber"`
	DefaultDate   string `json:"defaultDate" yaml:"defaultDate"`
}

func (a AutoSchema) Validate() error {
	if a.DefaultNumber != "int" && a.DefaultNumber != "number" {
		return fmt.Errorf("autoSchema.defaultNumber must be either 'int' or 'number")
	}
	if a.DefaultString != "string" && a.DefaultString != "text" {
		return fmt.Errorf("autoSchema.defaultString must be either 'string' or 'text")
	}
	if a.DefaultDate != "date" && a.DefaultDate != "string" && a.DefaultDate != "text" {
		return fmt.Errorf("autoSchema.defaultDate must be either 'date' or 'string' or 'text")
	}

	return nil
}

// QueryDefaults for optional parameters
type QueryDefaults struct {
	Limit int64 `json:"limit" yaml:"limit"`
}

type Contextionary struct {
	URL string `json:"url" yaml:"url"`
}

type Monitoring struct {
	Enabled bool   `json:"enabled"`
	Tool    string `json:"tool"`
	Port    int    `json:"port"`
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

type DiskUse struct {
	WarningPercentage  uint64 `json:"warning_percentage" yaml:"warning_percentage"`
	ReadOnlyPercentage uint64 `json:"readonly_percentage" yaml:"readonly_percentage"`
}

func (d DiskUse) Validate() error {
	if d.WarningPercentage > 100 {
		return fmt.Errorf("disk_use.read_only_percentage must be between 0 and 100")
	}

	if d.ReadOnlyPercentage > 100 {
		return fmt.Errorf("disk_use.read_only_percentage must be between 0 and 100")
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
			Info("no config file specified, using default or environment based")
	}

	// Read config file
	file, err := ioutil.ReadFile(configFileName)
	_ = err // explicitly ignore

	if len(file) > 0 {
		config, err := f.parseConfigFile(file, configFileName)
		if err != nil {
			return configErr(err)
		}
		f.Config = config

		deprecations.Log(logger, "config-files")
	}

	if err := FromEnv(&f.Config); err != nil {
		return configErr(err)
	}

	if err := f.Config.Authentication.Validate(); err != nil {
		return configErr(err)
	}

	if err := f.Config.Authorization.Validate(); err != nil {
		return configErr(err)
	}

	if err := f.Config.Persistence.Validate(); err != nil {
		return configErr(err)
	}

	if err := f.Config.AutoSchema.Validate(); err != nil {
		return configErr(err)
	}

	if err := f.Config.DiskUse.Validate(); err != nil {
		return configErr(err)
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

func configErr(err error) error {
	return fmt.Errorf("invalid config: %v", err)
}
