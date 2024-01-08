//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/go-openapi/swag"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/deprecations"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/usecases/cluster"
	"gopkg.in/yaml.v2"
)

// ServerVersion is set when the misc handlers are setup.
// When misc handlers are setup, the entire swagger spec
// is already being parsed for the server version. This is
// a good time for us to set ServerVersion, so that the
// spec only needs to be parsed once.
var ServerVersion string

// GitHash keeps the current git hash commit information
var GitHash = "unknown"

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
	DefaultMaxImportGoroutinesFactor = float64(1.5)

	DefaultDiskUseWarningPercentage  = uint64(80)
	DefaultDiskUseReadonlyPercentage = uint64(90)
	DefaultMemUseWarningPercentage   = uint64(80)
	// TODO: off by default for now, to make sure
	//       the measurement is reliable. once
	//       confirmed, we can set this to 90
	DefaultMemUseReadonlyPercentage = uint64(0)
)

// Flags are input options
type Flags struct {
	ConfigFile string `long:"config-file" description:"path to config file (default: ./weaviate.conf.json)"`
}

// Config outline of the config file
type Config struct {
	Name                                string                   `json:"name" yaml:"name"`
	Debug                               bool                     `json:"debug" yaml:"debug"`
	QueryDefaults                       QueryDefaults            `json:"query_defaults" yaml:"query_defaults"`
	QueryMaximumResults                 int64                    `json:"query_maximum_results" yaml:"query_maximum_results"`
	QueryNestedCrossReferenceLimit      int64                    `json:"query_nested_cross_reference_limit" yaml:"query_nested_cross_reference_limit"`
	Contextionary                       Contextionary            `json:"contextionary" yaml:"contextionary"`
	Authentication                      Authentication           `json:"authentication" yaml:"authentication"`
	Authorization                       Authorization            `json:"authorization" yaml:"authorization"`
	Origin                              string                   `json:"origin" yaml:"origin"`
	Persistence                         Persistence              `json:"persistence" yaml:"persistence"`
	DefaultVectorizerModule             string                   `json:"default_vectorizer_module" yaml:"default_vectorizer_module"`
	DefaultVectorDistanceMetric         string                   `json:"default_vector_distance_metric" yaml:"default_vector_distance_metric"`
	EnableModules                       string                   `json:"enable_modules" yaml:"enable_modules"`
	ModulesPath                         string                   `json:"modules_path" yaml:"modules_path"`
	ModuleHttpClientTimeout             time.Duration            `json:"modules_client_timeout" yaml:"modules_client_timeout"`
	AutoSchema                          AutoSchema               `json:"auto_schema" yaml:"auto_schema"`
	Cluster                             cluster.Config           `json:"cluster" yaml:"cluster"`
	Replication                         replication.GlobalConfig `json:"replication" yaml:"replication"`
	Monitoring                          Monitoring               `json:"monitoring" yaml:"monitoring"`
	GRPC                                GRPC                     `json:"grpc" yaml:"grpc"`
	Profiling                           Profiling                `json:"profiling" yaml:"profiling"`
	ResourceUsage                       ResourceUsage            `json:"resource_usage" yaml:"resource_usage"`
	MaxImportGoroutinesFactor           float64                  `json:"max_import_goroutine_factor" yaml:"max_import_goroutine_factor"`
	MaximumConcurrentGetRequests        int                      `json:"maximum_concurrent_get_requests" yaml:"maximum_concurrent_get_requests"`
	TrackVectorDimensions               bool                     `json:"track_vector_dimensions" yaml:"track_vector_dimensions"`
	ReindexVectorDimensionsAtStartup    bool                     `json:"reindex_vector_dimensions_at_startup" yaml:"reindex_vector_dimensions_at_startup"`
	DisableLazyLoadShards               bool                     `json:"disable_lazy_load_shards" yaml:"disable_lazy_load_shards"`
	RecountPropertiesAtStartup          bool                     `json:"recount_properties_at_startup" yaml:"recount_properties_at_startup"`
	ReindexSetToRoaringsetAtStartup     bool                     `json:"reindex_set_to_roaringset_at_startup" yaml:"reindex_set_to_roaringset_at_startup"`
	IndexMissingTextFilterableAtStartup bool                     `json:"index_missing_text_filterable_at_startup" yaml:"index_missing_text_filterable_at_startup"`
	DisableGraphQL                      bool                     `json:"disable_graphql" yaml:"disable_graphql"`
	AvoidMmap                           bool                     `json:"avoid_mmap" yaml:"avoid_mmap"`
	CORS                                CORS                     `json:"cors" yaml:"cors"`
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

	if err := c.validateDefaultVectorDistanceMetric(); err != nil {
		return errors.Wrap(err, "default vector distance metric")
	}

	return nil
}

func (c Config) validateDefaultVectorizerModule(modProv moduleProvider) error {
	if c.DefaultVectorizerModule == VectorizerModuleNone {
		return nil
	}

	return modProv.ValidateVectorizer(c.DefaultVectorizerModule)
}

func (c Config) validateDefaultVectorDistanceMetric() error {
	switch c.DefaultVectorDistanceMetric {
	case "", common.DistanceCosine, common.DistanceDot, common.DistanceL2Squared, common.DistanceManhattan, common.DistanceHamming:
		return nil
	default:
		return fmt.Errorf("must be one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]")
	}
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
	if a.DefaultString != schema.DataTypeText.String() &&
		a.DefaultString != schema.DataTypeString.String() {
		return fmt.Errorf("autoSchema.defaultString must be either 'string' or 'text")
	}
	if a.DefaultDate != "date" &&
		a.DefaultDate != schema.DataTypeText.String() &&
		a.DefaultDate != schema.DataTypeString.String() {
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
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Tool    string `json:"tool" yaml:"tool"`
	Port    int    `json:"port" yaml:"port"`
	Group   bool   `json:"group_classes" yaml:"group_classes"`
}

// Support independent TLS credentials for gRPC
type GRPC struct {
	Port     int    `json:"port" yaml:"port"`
	CertFile string `json:"certFile" yaml:"certFile"`
	KeyFile  string `json:"keyFile" yaml:"keyFile"`
}

type Profiling struct {
	BlockProfileRate     int `json:"blockProfileRate" yaml:"blockProfileRate"`
	MutexProfileFraction int `json:"mutexProfileFraction" yaml:"mutexProfileFraction"`
}

type Persistence struct {
	DataPath                          string `json:"dataPath" yaml:"dataPath"`
	FlushIdleMemtablesAfter           int    `json:"flushIdleMemtablesAfter" yaml:"flushIdleMemtablesAfter"`
	MemtablesMaxSizeMB                int    `json:"memtablesMaxSizeMB" yaml:"memtablesMaxSizeMB"`
	MemtablesMinActiveDurationSeconds int    `json:"memtablesMinActiveDurationSeconds" yaml:"memtablesMinActiveDurationSeconds"`
	MemtablesMaxActiveDurationSeconds int    `json:"memtablesMaxActiveDurationSeconds" yaml:"memtablesMaxActiveDurationSeconds"`
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

type MemUse struct {
	WarningPercentage  uint64 `json:"warning_percentage" yaml:"warning_percentage"`
	ReadOnlyPercentage uint64 `json:"readonly_percentage" yaml:"readonly_percentage"`
}

func (m MemUse) Validate() error {
	if m.WarningPercentage > 100 {
		return fmt.Errorf("mem_use.read_only_percentage must be between 0 and 100")
	}

	if m.ReadOnlyPercentage > 100 {
		return fmt.Errorf("mem_use.read_only_percentage must be between 0 and 100")
	}

	return nil
}

type ResourceUsage struct {
	DiskUse DiskUse
	MemUse  MemUse
}

type CORS struct {
	AllowOrigin  string `json:"allow_origin" yaml:"allow_origin"`
	AllowMethods string `json:"allow_methods" yaml:"allow_methods"`
	AllowHeaders string `json:"allow_headers" yaml:"allow_headers"`
}

const (
	DefaultCORSAllowOrigin  = "*"
	DefaultCORSAllowMethods = "*"
	DefaultCORSAllowHeaders = "Content-Type, Authorization, Batch, X-Openai-Api-Key, X-Openai-Organization, X-Openai-Baseurl, X-Anyscale-Baseurl, X-Anyscale-Api-Key, X-Cohere-Api-Key, X-Cohere-Baseurl, X-Huggingface-Api-Key, X-Azure-Api-Key, X-Palm-Api-Key, X-Jinaai-Api-Key, X-Aws-Access-Key, X-Aws-Secret-Key"
)

func (r ResourceUsage) Validate() error {
	if err := r.DiskUse.Validate(); err != nil {
		return err
	}

	if err := r.MemUse.Validate(); err != nil {
		return err
	}

	return nil
}

// GetConfigOptionGroup creates an option group for swagger
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
	}

	// Read config file
	file, err := os.ReadFile(configFileName)
	_ = err // explicitly ignore

	if len(file) > 0 {
		logger.WithField("action", "config_load").WithField("config_file_path", configFileName).
			Info("Usage of the weaviate.conf.json file is deprecated and will be removed in the future. Please use environment variables.")
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

	if err := f.Config.ResourceUsage.Validate(); err != nil {
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
