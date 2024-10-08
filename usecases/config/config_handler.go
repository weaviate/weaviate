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
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-openapi/swag"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/deprecations"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"gopkg.in/yaml.v2"
)

// ServerVersion is set when the misc handlers are setup.
// When misc handlers are setup, the entire swagger spec
// is already being parsed for the server version. This is
// a good time for us to set ServerVersion, so that the
// spec only needs to be parsed once.
var ServerVersion string

var (
	// GitHash keeps the current git hash commit information, value injected by the compiler using ldflags -X at build time.
	GitHash = "unknown"
	// ImageTag keeps the docker tag the weaviate binary was built in, value injected by the compiler using ldflags -X at build time.
	ImageTag = "localhost"
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

	RaftPort               int      `long:"raft-port" description:"the port used by Raft for inter-node communication"`
	RaftInternalRPCPort    int      `long:"raft-internal-rpc-port" description:"the port used for internal RPCs within the cluster"`
	RaftRPCMessageMaxSize  int      `long:"raft-rpc-message-max-size" description:"maximum internal raft grpc message size in bytes, defaults to 1073741824"`
	RaftJoin               []string `long:"raft-join" description:"a comma-separated list of server addresses to join on startup. Each element needs to be in the form NODE_NAME[:NODE_PORT]. If NODE_PORT is not present, raft-internal-rpc-port default value will be used instead"`
	RaftBootstrapTimeout   int      `long:"raft-bootstrap-timeout" description:"the duration for which the raft bootstrap procedure will wait for each node in raft-join to be reachable"`
	RaftBootstrapExpect    int      `long:"raft-bootstrap-expect" description:"specifies the number of server nodes to wait for before bootstrapping the cluster"`
	RaftHeartbeatTimeout   int      `long:"raft-heartbeat-timeout" description:"raft heartbeat timeout"`
	RaftElectionTimeout    int      `long:"raft-election-timeout" description:"raft election timeout"`
	RaftSnapshotThreshold  int      `long:"raft-snap-threshold" description:"number of outstanding log entries before performing a snapshot"`
	RaftSnapshotInterval   int      `long:"raft-snap-interval" description:"controls how often raft checks if it should perform a snapshot"`
	RaftMetadataOnlyVoters bool     `long:"raft-metadata-only-voters" description:"configures the voters to store metadata exclusively, without storing any other data"`
}

// Config outline of the config file
type Config struct {
	Name                                string                   `json:"name" yaml:"name"`
	Debug                               bool                     `json:"debug" yaml:"debug"`
	QueryDefaults                       QueryDefaults            `json:"query_defaults" yaml:"query_defaults"`
	QueryMaximumResults                 int64                    `json:"query_maximum_results" yaml:"query_maximum_results"`
	QueryNestedCrossReferenceLimit      int64                    `json:"query_nested_cross_reference_limit" yaml:"query_nested_cross_reference_limit"`
	QueryCrossReferenceDepthLimit       int                      `json:"query_cross_reference_depth_limit" yaml:"query_cross_reference_depth_limit"`
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
	Monitoring                          monitoring.Config        `json:"monitoring" yaml:"monitoring"`
	GRPC                                GRPC                     `json:"grpc" yaml:"grpc"`
	Profiling                           Profiling                `json:"profiling" yaml:"profiling"`
	ResourceUsage                       ResourceUsage            `json:"resource_usage" yaml:"resource_usage"`
	MaxImportGoroutinesFactor           float64                  `json:"max_import_goroutine_factor" yaml:"max_import_goroutine_factor"`
	MaximumConcurrentGetRequests        int                      `json:"maximum_concurrent_get_requests" yaml:"maximum_concurrent_get_requests"`
	TrackVectorDimensions               bool                     `json:"track_vector_dimensions" yaml:"track_vector_dimensions"`
	ReindexVectorDimensionsAtStartup    bool                     `json:"reindex_vector_dimensions_at_startup" yaml:"reindex_vector_dimensions_at_startup"`
	DisableLazyLoadShards               bool                     `json:"disable_lazy_load_shards" yaml:"disable_lazy_load_shards"`
	ForceFullReplicasSearch             bool                     `json:"force_full_replicas_search" yaml:"force_full_replicas_search"`
	RecountPropertiesAtStartup          bool                     `json:"recount_properties_at_startup" yaml:"recount_properties_at_startup"`
	ReindexSetToRoaringsetAtStartup     bool                     `json:"reindex_set_to_roaringset_at_startup" yaml:"reindex_set_to_roaringset_at_startup"`
	IndexMissingTextFilterableAtStartup bool                     `json:"index_missing_text_filterable_at_startup" yaml:"index_missing_text_filterable_at_startup"`
	DisableGraphQL                      bool                     `json:"disable_graphql" yaml:"disable_graphql"`
	AvoidMmap                           bool                     `json:"avoid_mmap" yaml:"avoid_mmap"`
	CORS                                CORS                     `json:"cors" yaml:"cors"`
	DisableTelemetry                    bool                     `json:"disable_telemetry" yaml:"disable_telemetry"`
	HNSWStartupWaitForVectorCache       bool                     `json:"hnsw_startup_wait_for_vector_cache" yaml:"hnsw_startup_wait_for_vector_cache"`
	Sentry                              *entsentry.ConfigOpts    `json:"sentry" yaml:"sentry"`

	// Raft Specific configuration
	// TODO-RAFT: Do we want to be able to specify these with config file as well ?
	Raft Raft
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

// DefaultQueryDefaultsLimit is the default query limit when no limit is provided
const DefaultQueryDefaultsLimit int64 = 10

type Contextionary struct {
	URL string `json:"url" yaml:"url"`
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
	Port                 int `json:"port" yaml:"port"`
}

type Persistence struct {
	DataPath                          string `json:"dataPath" yaml:"dataPath"`
	MemtablesFlushDirtyAfter          int    `json:"flushDirtyMemtablesAfter" yaml:"flushDirtyMemtablesAfter"`
	MemtablesMaxSizeMB                int    `json:"memtablesMaxSizeMB" yaml:"memtablesMaxSizeMB"`
	MemtablesMinActiveDurationSeconds int    `json:"memtablesMinActiveDurationSeconds" yaml:"memtablesMinActiveDurationSeconds"`
	MemtablesMaxActiveDurationSeconds int    `json:"memtablesMaxActiveDurationSeconds" yaml:"memtablesMaxActiveDurationSeconds"`
	LSMMaxSegmentSize                 int64  `json:"lsmMaxSegmentSize" yaml:"lsmMaxSegmentSize"`
	LSMSegmentsCleanupIntervalHours   int64  `json:"lsmSegmentsCleanupIntervalHours" yaml:"lsmSegmentsCleanupIntervalHours"`
	HNSWMaxLogSize                    int64  `json:"hnswMaxLogSize" yaml:"hnswMaxLogSize"`
}

// DefaultPersistenceDataPath is the default location for data directory when no location is provided
const DefaultPersistenceDataPath string = "./data"

// DefaultPersistenceLSMMaxSegmentSize is effectively unlimited for backward
// compatibility. TODO: consider changing this in a future release and make
// some noise about it. This is technically a breaking change.
const DefaultPersistenceLSMMaxSegmentSize = math.MaxInt64

// DefaultPersistenceLSMSegmentsCleanupIntervalHours = 0 for backward compatibility.
// value = 0 means cleanup is turned off.
const DefaultPersistenceLSMSegmentsCleanupIntervalHours = 0

const DefaultPersistenceHNSWMaxLogSize = 500 * 1024 * 1024 // 500MB for backward compatibility

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
	DefaultCORSAllowHeaders = "Content-Type, Authorization, Batch, X-Openai-Api-Key, X-Openai-Organization, X-Openai-Baseurl, X-Anyscale-Baseurl, X-Anyscale-Api-Key, X-Cohere-Api-Key, X-Cohere-Baseurl, X-Huggingface-Api-Key, X-Azure-Api-Key, X-Google-Api-Key, X-Google-Vertex-Api-Key, X-Google-Studio-Api-Key, X-Palm-Api-Key, X-Jinaai-Api-Key, X-Aws-Access-Key, X-Aws-Secret-Key, X-Voyageai-Baseurl, X-Voyageai-Api-Key, X-Mistral-Baseurl, X-Mistral-Api-Key, X-OctoAI-Api-Key"
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

type Raft struct {
	Port                   int
	InternalRPCPort        int
	RPCMessageMaxSize      int
	Join                   []string
	SnapshotThreshold      uint64
	HeartbeatTimeout       time.Duration
	ElectionTimeout        time.Duration
	SnapshotInterval       time.Duration
	ConsistencyWaitTimeout time.Duration

	BootstrapTimeout   time.Duration
	BootstrapExpect    int
	MetadataOnlyVoters bool

	EnableOneNodeRecovery bool
	ForceOneNodeRecovery  bool

	EnableFQDNResolver bool
	FQDNResolverTLD    string
}

func (r *Raft) Validate() error {
	if r.Port == 0 {
		return fmt.Errorf("raft.port must be greater than 0")
	}

	if r.InternalRPCPort == 0 {
		return fmt.Errorf("raft.intra_rpc_port must be greater than 0")
	}

	uniqueMap := make(map[string]struct{}, len(r.Join))
	updatedJoinList := make([]string, len(r.Join))
	for i, nodeNameAndPort := range r.Join {
		// Check that the format is correct. In case only node name is present we append the default raft port
		nodeNameAndPortSplitted := strings.Split(nodeNameAndPort, ":")
		if len(nodeNameAndPortSplitted) == 0 {
			return fmt.Errorf("raft.join element %s has no node name", nodeNameAndPort)
		} else if len(nodeNameAndPortSplitted) < 2 {
			// If user only specify a node name and no port, use the default raft port
			nodeNameAndPortSplitted = append(nodeNameAndPortSplitted, fmt.Sprintf("%d", DefaultRaftPort))
		} else if len(nodeNameAndPortSplitted) > 2 {
			return fmt.Errorf("raft.join element %s has unexpected amount of element", nodeNameAndPort)
		}

		// Check that the node name is unique
		nodeName := nodeNameAndPortSplitted[0]
		if _, ok := uniqueMap[nodeName]; ok {
			return fmt.Errorf("raft.join contains the value %s multiple times. Joined nodes must have a unique id", nodeName)
		} else {
			uniqueMap[nodeName] = struct{}{}
		}

		// TODO-RAFT START
		// Validate host and port

		updatedJoinList[i] = strings.Join(nodeNameAndPortSplitted, ":")
	}
	r.Join = updatedJoinList

	if r.BootstrapExpect == 0 {
		return fmt.Errorf("raft.bootstrap_expect must be greater than 0")
	}

	if r.BootstrapExpect > len(r.Join) {
		return fmt.Errorf("raft.bootstrap.expect must be less than or equal to the length of raft.join")
	}

	if r.SnapshotInterval <= 0 {
		return fmt.Errorf("raft.bootstrap.snapshot_interval must be more than 0")
	}

	if r.SnapshotThreshold <= 0 {
		return fmt.Errorf("raft.bootstrap.snapshot_threshold must be more than 0")
	}

	if r.ConsistencyWaitTimeout <= 0 {
		return fmt.Errorf("raft.bootstrap.consistency_wait_timeout must be more than 0")
	}

	return nil
}

// GetConfigOptionGroup creates an option group for swagger
func GetConfigOptionGroup() *swag.CommandLineOptionsGroup {
	commandLineOptionsGroup := swag.CommandLineOptionsGroup{
		ShortDescription: "Connector, raft & MQTT config",
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

// LoadConfig from config locations. The load order for configuration values if the following
// 1. Config file
// 2. Environment variables
// 3. Command line flags
// If a config option is specified multiple times in different locations, the latest one will be used in this order.
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

	// Load config from config file if present
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

	// Load config from env
	if err := FromEnv(&f.Config); err != nil {
		return configErr(err)
	}

	// Load config from flags
	f.fromFlags(flags.Options.(*Flags))

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

	if err := f.Config.Raft.Validate(); err != nil {
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

// fromFlags parses values from flags given as parameter and overrides values in the config
func (f *WeaviateConfig) fromFlags(flags *Flags) {
	if flags.RaftPort > 0 {
		f.Config.Raft.Port = flags.RaftPort
	}
	if flags.RaftInternalRPCPort > 0 {
		f.Config.Raft.InternalRPCPort = flags.RaftInternalRPCPort
	}
	if flags.RaftRPCMessageMaxSize > 0 {
		f.Config.Raft.RPCMessageMaxSize = flags.RaftRPCMessageMaxSize
	}
	if flags.RaftJoin != nil {
		f.Config.Raft.Join = flags.RaftJoin
	}
	if flags.RaftBootstrapTimeout > 0 {
		f.Config.Raft.BootstrapTimeout = time.Second * time.Duration(flags.RaftBootstrapTimeout)
	}
	if flags.RaftBootstrapExpect > 0 {
		f.Config.Raft.BootstrapExpect = flags.RaftBootstrapExpect
	}
	if flags.RaftSnapshotInterval > 0 {
		f.Config.Raft.SnapshotInterval = time.Second * time.Duration(flags.RaftSnapshotInterval)
	}
	if flags.RaftSnapshotThreshold > 0 {
		f.Config.Raft.SnapshotThreshold = uint64(flags.RaftSnapshotThreshold)
	}
	if flags.RaftMetadataOnlyVoters {
		f.Config.Raft.MetadataOnlyVoters = true
	}
}

func configErr(err error) error {
	return fmt.Errorf("invalid config: %v", err)
}
