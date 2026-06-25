//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"slices"
	"strings"
	"time"

	"github.com/go-openapi/swag"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"github.com/weaviate/weaviate/deprecations"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	usagetypes "github.com/weaviate/weaviate/usecases/modulecomponents/usage/types"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// ServerVersion is deprecated. Use `build.Version`. It's there for backward compatiblility.
// ServerVersion is set when the misc handlers are setup.
// When misc handlers are setup, the entire swagger spec
// is already being parsed for the server version. This is
// a good time for us to set ServerVersion, so that the
// spec only needs to be parsed once.
var ServerVersion string

// DefaultConfigFile is the default file when no config file is provided
const DefaultConfigFile string = "./weaviate.conf.json"

// DefaultCleanupIntervalSeconds can be overwritten on a per-class basis
const DefaultCleanupIntervalSeconds = int64(60)

const (
	// These BM25 tuning params can be overwritten on a per-class basis
	DefaultBM25k1 = float32(1.2)
	DefaultBM25b  = float32(0.75)
)

var DefaultUsingBlockMaxWAND = os.Getenv("USE_INVERTED_SEARCHABLE") == "" || entcfg.Enabled(os.Getenv("USE_INVERTED_SEARCHABLE"))

const (
	// Lazy load shard auto-detection thresholds
	DefaultLazyLoadShardCountThreshold  = 1000
	DefaultLazyLoadShardSizeThresholdGB = 100.0 // 100GB
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

const (
	DefaultObjectsTTLDeleteSchedule      = "" // disabled
	DefaultObjectsTTLBatchSize           = 10_000
	DefaultObjectsTTLConcurrencyFactor   = 1
	DefaultObjectsTTLPauseEveryNoBatches = 10
	DefaultObjectsTTLPauseDuration       = time.Minute

	// DefaultExportParallelism is the number of concurrent scan workers per
	// export. Defaults to 0 which means GOMAXPROCS at runtime. The value is
	// dynamically configurable via runtime overrides.
	DefaultExportParallelism = 0
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

	RuntimeOverridesEnabled      bool          `long:"runtime-overrides.enabled" description:"enable runtime overrides config"`
	RuntimeOverridesPath         string        `long:"runtime-overrides.path" description:"path to runtime overrides config"`
	RuntimeOverridesLoadInterval time.Duration `long:"runtime-overrides.load-interval" description:"load interval for runtime overrides config"`
}

type SchemaHandlerConfig struct {
	MaximumAllowedCollectionsCount *runtime.DynamicValue[int] `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`
}

// RestrictionsConfig gates *what kind* of class operators allow to be
// created (vs UsageLimitsConfig which caps how much). nil or empty list
// = no restriction; see validateRestrictions for cross-field rules.
type RestrictionsConfig struct {
	AllowedVectorIndexTypes *runtime.DynamicValue[[]string] `json:"allowed_vector_index_types" yaml:"allowed_vector_index_types"`
	AllowedCompressionTypes *runtime.DynamicValue[[]string] `json:"allowed_compression_types" yaml:"allowed_compression_types"`
	// ErrorMessage is the operator-overridable template rendered into
	// the CONFIG_NOT_ALLOWED response `message`. Placeholders:
	// {restriction}, {value}, {allowed}.
	ErrorMessage *runtime.DynamicValue[string] `json:"error_message" yaml:"error_message"`
}

// UsageLimitsConfig holds the env-var and runtime-overrideable usage-limit
// knobs introduced by the Free-Tier guardrails RFC. The collection-count
// limit lives separately on SchemaHandlerConfig for backward compatibility
// with the pre-existing MAXIMUM_ALLOWED_COLLECTIONS_COUNT env var.
//
// All fields are *runtime.DynamicValue[*]; nil means "unset" (treated as
// unlimited / default). Operators set values via env vars at startup and
// can also override at runtime via the YAML runtime-overrides file.
type UsageLimitsConfig struct {
	// ErrorMessage is the operator-overridable template used to render the
	// `message` field of the structured limit-exceeded response. Recognized
	// placeholders are {limit} and {value}; see usagelimits.RenderTemplate.
	ErrorMessage *runtime.DynamicValue[string] `json:"error_message" yaml:"error_message"`
	// MaxObjectsCount caps the total live object count. Negative (incl.
	// the default -1) means unlimited.
	MaxObjectsCount *runtime.DynamicValue[int] `json:"max_objects_count" yaml:"max_objects_count"`
	// MaxTenantsPerCollection caps the number of tenants on a multi-tenant
	// class. Checked at tenant create time only.
	MaxTenantsPerCollection *runtime.DynamicValue[int] `json:"max_tenants_per_collection" yaml:"max_tenants_per_collection"`
	// MaxShardsPerCollection caps the requested shard count of a class
	// create request. Config-time only.
	MaxShardsPerCollection *runtime.DynamicValue[int] `json:"max_shards_per_collection" yaml:"max_shards_per_collection"`
}

type RuntimeOverrides struct {
	Enabled      bool          `json:"enabled"`
	Path         string        `json:"path" yaml:"path"`
	LoadInterval time.Duration `json:"load_interval" yaml:"load_interval"`
}

// Config outline of the config file
type Config struct {
	Backup                           Backup                   `json:"backup" yaml:"backup"`
	Name                             string                   `json:"name" yaml:"name"`
	Debug                            bool                     `json:"debug" yaml:"debug"`
	QueryDefaults                    QueryDefaults            `json:"query_defaults" yaml:"query_defaults"`
	QueryMaximumResults              int64                    `json:"query_maximum_results" yaml:"query_maximum_results"`
	QueryHybridMaximumResults        int64                    `json:"query_hybrid_maximum_results" yaml:"query_hybrid_maximum_results"`
	QueryBoostDefaultDepth           int                      `json:"query_boost_default_depth" yaml:"query_boost_default_depth"`
	QueryNestedCrossReferenceLimit   int64                    `json:"query_nested_cross_reference_limit" yaml:"query_nested_cross_reference_limit"`
	QueryCrossReferenceDepthLimit    int                      `json:"query_cross_reference_depth_limit" yaml:"query_cross_reference_depth_limit"`
	Contextionary                    Contextionary            `json:"contextionary" yaml:"contextionary"`
	Authentication                   Authentication           `json:"authentication" yaml:"authentication"`
	Authorization                    Authorization            `json:"authorization" yaml:"authorization"`
	Origin                           string                   `json:"origin" yaml:"origin"`
	Persistence                      Persistence              `json:"persistence" yaml:"persistence"`
	DefaultVectorizerModule          string                   `json:"default_vectorizer_module" yaml:"default_vectorizer_module"`
	DefaultVectorDistanceMetric      string                   `json:"default_vector_distance_metric" yaml:"default_vector_distance_metric"`
	EnableModules                    string                   `json:"enable_modules" yaml:"enable_modules"`
	EnableApiBasedModules            bool                     `json:"api_based_modules_disabled" yaml:"api_based_modules_disabled"`
	ModulesPath                      string                   `json:"modules_path" yaml:"modules_path"`
	ModuleHttpClientTimeout          time.Duration            `json:"modules_client_timeout" yaml:"modules_client_timeout"`
	AutoSchema                       AutoSchema               `json:"auto_schema" yaml:"auto_schema"`
	Cluster                          cluster.Config           `json:"cluster" yaml:"cluster"`
	Replication                      replication.GlobalConfig `json:"replication" yaml:"replication"`
	Monitoring                       monitoring.Config        `json:"monitoring" yaml:"monitoring"`
	GRPC                             GRPC                     `json:"grpc" yaml:"grpc"`
	MCP                              MCP                      `json:"mcp" yaml:"mcp"`
	Profiling                        Profiling                `json:"profiling" yaml:"profiling"`
	ResourceUsage                    ResourceUsage            `json:"resource_usage" yaml:"resource_usage"`
	MaxImportGoroutinesFactor        float64                  `json:"max_import_goroutine_factor" yaml:"max_import_goroutine_factor"`
	MaximumConcurrentGetRequests     int                      `json:"maximum_concurrent_get_requests" yaml:"maximum_concurrent_get_requests"`
	MaximumConcurrentShardLoads      int                      `json:"maximum_concurrent_shard_loads" yaml:"maximum_concurrent_shard_loads"`
	MaximumConcurrentBucketLoads     int                      `json:"maximum_concurrent_bucket_loads" yaml:"maximum_concurrent_bucket_loads"`
	TrackVectorDimensions            bool                     `json:"track_vector_dimensions" yaml:"track_vector_dimensions"`
	TrackVectorDimensionsInterval    time.Duration            `json:"track_vector_dimensions_interval" yaml:"track_vector_dimensions_interval"`
	ReindexVectorDimensionsAtStartup bool                     `json:"reindex_vector_dimensions_at_startup" yaml:"reindex_vector_dimensions_at_startup"`
	// EnableLazyLoadShards controls lazy shard loading.
	// nil = auto-detect based on thresholds, true = always lazy-load, false = always eager-load.
	// DISABLE_LAZY_LOAD_SHARDS=true sets this to false for backward compatibility.
	EnableLazyLoadShards                *bool                  `json:"enable_lazy_load_shards" yaml:"enable_lazy_load_shards"`
	LazyLoadShardCountThreshold         int                    `json:"lazy_load_shard_count_threshold" yaml:"lazy_load_shard_count_threshold"`
	LazyLoadShardSizeThresholdGB        float64                `json:"lazy_load_shard_size_threshold_gb" yaml:"lazy_load_shard_size_threshold_gb"`
	ForceFullReplicasSearch             bool                   `json:"force_full_replicas_search" yaml:"force_full_replicas_search"`
	TransferInactivityTimeout           time.Duration          `json:"transfer_inactivity_timeout" yaml:"transfer_inactivity_timeout"`
	RecountPropertiesAtStartup          bool                   `json:"recount_properties_at_startup" yaml:"recount_properties_at_startup"`
	ReindexSetToRoaringsetAtStartup     bool                   `json:"reindex_set_to_roaringset_at_startup" yaml:"reindex_set_to_roaringset_at_startup"`
	ReindexerGoroutinesFactor           float64                `json:"reindexer_goroutines_factor" yaml:"reindexer_goroutines_factor"`
	ReindexMapToBlockmaxAtStartup       bool                   `json:"reindex_map_to_blockmax_at_startup" yaml:"reindex_map_to_blockmax_at_startup"`
	ReindexMapToBlockmaxConfig          MapToBlockamaxConfig   `json:"reindex_map_to_blockmax_config" yaml:"reindex_map_to_blockmax_config"`
	IndexMissingTextFilterableAtStartup bool                   `json:"index_missing_text_filterable_at_startup" yaml:"index_missing_text_filterable_at_startup"`
	DisableGraphQL                      bool                   `json:"disable_graphql" yaml:"disable_graphql"`
	AvoidMmap                           bool                   `json:"avoid_mmap" yaml:"avoid_mmap"`
	CORS                                CORS                   `json:"cors" yaml:"cors"`
	DisableTelemetry                    bool                   `json:"disable_telemetry" yaml:"disable_telemetry"`
	TelemetryURL                        string                 `json:"telemetry_url" yaml:"telemetry_url"`
	TelemetryPushInterval               time.Duration          `json:"telemetry_push_interval" yaml:"telemetry_push_interval"`
	HNSWStartupWaitForVectorCache       bool                   `json:"hnsw_startup_wait_for_vector_cache" yaml:"hnsw_startup_wait_for_vector_cache"`
	HNSWVisitedListPoolMaxSize          int                    `json:"hnsw_visited_list_pool_max_size" yaml:"hnsw_visited_list_pool_max_size"`
	HNSWFlatSearchConcurrency           int                    `json:"hnsw_flat_search_concurrency" yaml:"hnsw_flat_search_concurrency"`
	HNSWAcornFilterRatio                float64                `json:"hnsw_acorn_filter_ratio" yaml:"hnsw_acorn_filter_ratio"`
	HNSWGeoIndexEF                      int                    `json:"hnsw_geo_index_ef" yaml:"hnsw_geo_index_ef"`
	AsyncIndexingEnabled                bool                   `json:"async_indexing_enabled" yaml:"async_indexing_enabled"`
	Sentry                              *entsentry.ConfigOpts  `json:"sentry" yaml:"sentry"`
	MetadataServer                      MetadataServer         `json:"metadata_server" yaml:"metadata_server"`
	SchemaHandlerConfig                 SchemaHandlerConfig    `json:"schema" yaml:"schema"`
	UsageLimits                         UsageLimitsConfig      `json:"usage_limits" yaml:"usage_limits"`
	Restrictions                        RestrictionsConfig     `json:"restrictions" yaml:"restrictions"`
	DistributedTasks                    DistributedTasksConfig `json:"distributed_tasks" yaml:"distributed_tasks"`
	ReplicationEngineMaxWorkers         int                    `json:"replication_engine_max_workers" yaml:"replication_engine_max_workers"`
	ReplicationEngineFileCopyWorkers    int                    `json:"replication_engine_file_copy_workers" yaml:"replication_engine_file_copy_workers"`
	HFreshEnabled                       bool                   `json:"hfresh_enabled" yaml:"hfresh_enabled"`
	ReplicationEngineFileCopyChunkSize  int                    `json:"replication_engine_file_copy_chunk_size" yaml:"replication_engine_file_copy_chunk_size"`
	// Raft Specific configuration
	// TODO-RAFT: Do we want to be able to specify these with config file as well ?
	Raft Raft

	// map[className][]propertyName
	ReindexIndexesAtStartup map[string][]string `json:"reindex_indexes_at_startup" yaml:"reindex_indexes_at_startup"`

	RuntimeOverrides RuntimeOverrides `json:"runtime_overrides" yaml:"runtime_overrides"`

	ReplicaMovementEnabled bool `json:"replica_movement_enabled" yaml:"replica_movement_enabled"`

	// TenantActivityReadLogLevel is 'debug' by default as every single READ
	// interaction with a tenant leads to a log line. However, this may
	// temporarily be desired, e.g. for analysis or debugging purposes. In this
	// case the log level can be elevated, e.g. to 'info'. This is overall less
	// noisy than changing the global log level, but still allows to see all
	// tenant read activity.
	TenantActivityReadLogLevel *runtime.DynamicValue[string] `json:"tenant_activity_read_log_level" yaml:"tenant_activity_read_log_level"`
	// TenantActivityWriteLogLevel is 'debug' by default as every single WRITE
	// interaction with a tenant leads to a log line. However, this may
	// temporarily be desired, e.g. for analysis or debugging purposes. In this
	// case the log level can be elevated, e.g. to 'info'. This is overall less
	// noisy than changing the global log level, but still allows to see all
	// tenant write activity.
	TenantActivityWriteLogLevel *runtime.DynamicValue[string] `json:"tenant_activity_write_log_level" yaml:"tenant_activity_write_log_level"`

	// RevectorizeCheck is an optimization where Weaviate checks if a vector can
	// be reused from a previous version of the object, for example because the
	// only change was an update of a property that is excluded from
	// vectorization. This check is on by default (backward-compatibility).
	//
	// However, this check comes at a cost, it means that every single insert
	// will turn into a read-before-write pattern, even if the inserted object is
	// new. That is because the logic first needs to check if the object even
	// exists. In cases where write throughput matters and the overwhelming
	// majority of inserts are new, unique objects, it might be advisable to turn
	// this feature off using the provided flag.
	RevectorizeCheckDisabled *runtime.DynamicValue[bool] `json:"revectorize_check_disabled" yaml:"revectorize_check_disabled"`

	QuerySlowLogEnabled   *runtime.DynamicValue[bool]          `json:"query_slow_log_enabled" yaml:"query_slow_log_enabled"`
	QuerySlowLogThreshold *runtime.DynamicValue[time.Duration] `json:"query_slow_log_threshold" yaml:"query_slow_log_threshold"`

	// New classes will be created with the default quantization
	DefaultQuantization *runtime.DynamicValue[string] `json:"default_quantization" yaml:"default_quantization"`

	// New classes will be created with this vector index type instead of HNSW.
	// Valid values: "hnsw", "flat", "dynamic", "hfresh".
	DefaultVectorIndexType *runtime.DynamicValue[string] `json:"default_vector_index" yaml:"default_vector_index"`

	// New classes will be created with this shard count instead of the cluster node count.
	// A value of 0 means use the cluster node count (default behavior).
	DefaultShardingCount *runtime.DynamicValue[int] `json:"default_sharding_count" yaml:"default_sharding_count"`

	QueryBitmapBufsMaxMemory  int `json:"query_bitmap_bufs_max_memory" yaml:"query_bitmap_bufs_max_memory"`
	QueryBitmapBufsMaxBufSize int `json:"query_bitmap_bufs_max_buf_size" yaml:"query_bitmap_bufs_max_buf_size"`

	// InvertedSorterDisabled forces the "objects bucket" strategy and doesn't
	// not consider inverted sorting, even when the query planner thinks this is
	// the better option.
	//
	// Most users should never set this flag, it exists for two reasons:
	//  - For benchmarking reasons, this flag can be used to evaluate the
	//		(positive) impact of the inverted sorter.
	//  - As a safety net to revert to the old behavior in case there is a bug
	//		in the inverted indexer despite the very extensive testing.
	//
	// This flat may be removed in the future.
	InvertedSorterDisabled *runtime.DynamicValue[bool] `json:"inverted_sorter_disabled" yaml:"inverted_sorter_disabled"`

	// LazyPropertyLengthsEnabled defers loading an inverted segment's property
	// length map until first use and frees it after a compaction drops the
	// segment, trading a one-time load on the first cold BM25 query for memory.
	LazyPropertyLengthsEnabled *runtime.DynamicValue[bool] `json:"lazy_property_lengths_enabled" yaml:"lazy_property_lengths_enabled"`

	// Export configures the data export feature and its storage destination.
	Export Export `json:"export" yaml:"export"`

	// Namespaces configures cluster-level namespace support. Namespaces can
	// only be enabled on newly bootstrapped clusters (enforced at startup).
	Namespaces Namespaces `json:"namespaces" yaml:"namespaces"`

	// Usage configuration for the usage module
	Usage usagetypes.UsageConfig `json:"usage" yaml:"usage"`

	// The minimum timeout for the server to wait before it returns an error
	MinimumInternalTimeout time.Duration `json:"minimum_internal_timeout" yaml:"minimum_internal_timeout"`

	// Time expired objects should be deleted at by background routine
	// accepts format: https://github.com/netresearch/go-cron?tab=readme-ov-file#cron-expression-format
	ObjectsTTLDeleteSchedule      *runtime.DynamicValue[string]        `json:"objects_ttl_delete_schedule" yaml:"objects_ttl_delete_schedule"`
	ObjectsTTLBatchSize           *runtime.DynamicValue[int]           `json:"objects_ttl_batch_size" yaml:"objects_ttl_batch_size"`
	ObjectsTTLPauseEveryNoBatches *runtime.DynamicValue[int]           `json:"objects_ttl_pause_every_no_batches" yaml:"objects_ttl_pause_every_no_batches"`
	ObjectsTTLPauseDuration       *runtime.DynamicValue[time.Duration] `json:"objects_ttl_pause_duration" yaml:"objects_ttl_pause_duration"`
	ObjectsTTLConcurrencyFactor   *runtime.DynamicValue[float64]       `json:"objects_ttl_concurrency_factor" yaml:"objects_ttl_concurrency_factor"`

	// ExportParallelism controls the number of concurrent scan workers per
	// export. 0 (default) means GOMAXPROCS at runtime.
	ExportParallelism *runtime.DynamicValue[int] `json:"export_parallelism" yaml:"export_parallelism"`

	// The specific mode of operation for the instance itself. Is an enum of Full, WriteOnly, ReadOnly, ScaleOut
	OperationalMode *runtime.DynamicValue[string] `json:"operational_mode" yaml:"operational_mode"`

	// Disable vector dimension tracking that are used for billing. These metrics are being deprecated in favor of more accurate metrics
	DisableDimensionMetrics *runtime.DynamicValue[bool] `json:"disable_dimension_metrics" yaml:"disable_dimension_metrics"`
}

type MapToBlockamaxConfig struct {
	SwapBuckets                bool                     `json:"swap_buckets" yaml:"swap_buckets"`
	UnswapBuckets              bool                     `json:"unswap_buckets" yaml:"unswap_buckets"`
	TidyBuckets                bool                     `json:"tidy_buckets" yaml:"tidy_buckets"`
	ReloadShards               bool                     `json:"reload_shards" yaml:"reload_shards"`
	Rollback                   bool                     `json:"rollback" yaml:"rollback"`
	ConditionalStart           bool                     `json:"conditional_start" yaml:"conditional_start"`
	ProcessingDurationSeconds  int                      `json:"processing_duration_seconds" yaml:"processing_duration_seconds"`
	PauseDurationSeconds       int                      `json:"pause_duration_seconds" yaml:"pause_duration_seconds"`
	PerObjectDelayMilliseconds int                      `json:"per_object_delay_milliseconds" yaml:"per_object_delay_milliseconds"`
	Selected                   []CollectionPropsTenants `json:"selected" yaml:"selected"`
}

type CollectionPropsTenants struct {
	Collection string   `json:"collection" yaml:"collection"`
	Props      []string `json:"props" yaml:"props"`
	Tenants    []string `json:"tenants" yaml:"tenants"`
}

// Validate the configuration
func (c *Config) Validate() error {
	if err := c.Authentication.Validate(); err != nil {
		return configErr(err)
	}

	if err := c.Authorization.Validate(); err != nil {
		return configErr(err)
	}

	if c.Authentication.AnonymousAccess.Enabled && c.Authorization.Rbac.Enabled {
		return fmt.Errorf("cannot enable anonymous access and rbac authorization")
	}

	// Namespaces are incompatible with GraphQL: the GraphQL schema does not
	// model namespace-qualified class names. On namespace-enabled clusters the
	// operator must disable GraphQL explicitly via DISABLE_GRAPHQL=true.
	if c.Namespaces.Enabled && !c.DisableGraphQL {
		return fmt.Errorf("NAMESPACES_ENABLED=true requires DISABLE_GRAPHQL=true: GraphQL is not supported on namespace-enabled clusters")
	}

	// Without RBAC the role narrowing that keeps namespaced principals off
	// cluster-wide operator surfaces never runs. Also rejects AdminList-only,
	// which sets Rbac.Enabled=false.
	if c.Namespaces.Enabled && !c.Authorization.Rbac.Enabled {
		return fmt.Errorf("NAMESPACES_ENABLED=true requires RBAC to be enabled")
	}

	if err := c.validateOIDCNamespaceClaims(); err != nil {
		return configErr(err)
	}

	if err := c.Persistence.Validate(); err != nil {
		return configErr(err)
	}

	if err := c.AutoSchema.Validate(); err != nil {
		return configErr(err)
	}

	if err := c.ResourceUsage.Validate(); err != nil {
		return configErr(err)
	}

	if err := c.Raft.Validate(); err != nil {
		return configErr(err)
	}

	if err := c.validateUsageLimitsReplicationLinkage(); err != nil {
		return configErr(err)
	}

	if err := c.validateReplicationFactorBounds(); err != nil {
		return configErr(err)
	}

	if err := c.validateRestrictions(); err != nil {
		return configErr(err)
	}

	return nil
}

// validateOIDCNamespaceClaims requires the namespace + global-principal
// claim env vars when NAMESPACES_ENABLED and AUTHENTICATION_OIDC_ENABLED
// are both on, and forbids them when NAMESPACES_ENABLED is off. No-op
// when OIDC is disabled.
func (c *Config) validateOIDCNamespaceClaims() error {
	if !c.Authentication.OIDC.Enabled {
		return nil
	}

	nsClaim := dynamicString(c.Authentication.OIDC.NamespaceClaim)
	globalClaim := dynamicString(c.Authentication.OIDC.GlobalPrincipalClaim)

	if c.Namespaces.Enabled {
		if nsClaim == "" || globalClaim == "" {
			return fmt.Errorf("AUTHENTICATION_OIDC_NAMESPACE_CLAIM and AUTHENTICATION_OIDC_GLOBAL_PRINCIPAL_CLAIM are required when NAMESPACES_ENABLED=true and AUTHENTICATION_OIDC_ENABLED=true")
		}
		return nil
	}

	if nsClaim != "" || globalClaim != "" {
		return fmt.Errorf("AUTHENTICATION_OIDC_NAMESPACE_CLAIM and AUTHENTICATION_OIDC_GLOBAL_PRINCIPAL_CLAIM must not be set when NAMESPACES_ENABLED=false")
	}
	return nil
}

// validateUsageLimitsReplicationLinkage enforces the RF=1 precondition for the
// object/tenant/shard usage limits: only the RF=1 deployment shape is
// supported, so when any of those caps is set we require
// REPLICATION_MAXIMUM_FACTOR=1 so per-class RF cannot exceed 1. The collection
// cap is excluded because it predates this PR and tying it would be a breaking
// change for existing operators.
func (c *Config) validateUsageLimitsReplicationLinkage() error {
	hasLimit := dynamicIntSet(c.UsageLimits.MaxObjectsCount) ||
		dynamicIntSet(c.UsageLimits.MaxTenantsPerCollection) ||
		dynamicIntSet(c.UsageLimits.MaxShardsPerCollection)
	if !hasLimit {
		return nil
	}
	if c.Replication.MaximumFactor != 1 {
		return fmt.Errorf("usage limits require REPLICATION_MAXIMUM_FACTOR=1; got %d", c.Replication.MaximumFactor)
	}
	return nil
}

// dynamicString returns the value carried by a *DynamicValue[string], or ""
// when the pointer itself is nil (uninitialized config).
func dynamicString(v *runtime.DynamicValue[string]) string {
	if v == nil {
		return ""
	}
	return v.Get()
}

// dynamicIntSet reports whether dv carries a configured (>=0) value. A nil
// DynamicValue or a negative value means "unset / unlimited".
func dynamicIntSet(dv *runtime.DynamicValue[int]) bool {
	return dv != nil && dv.Get() >= 0
}

// validateReplicationFactorBounds rejects configurations where the floor
// exceeds the ceiling, which would make every class creation unsatisfiable.
// A MaximumFactor <= 0 means "no cap" by convention (see GlobalConfig), so
// the comparison is skipped in that case. MinimumFactor < 1 is also rejected
// since a factor of zero is meaningless and the in-code default is 1.
func (c *Config) validateReplicationFactorBounds() error {
	if c.Replication.MinimumFactor < 1 {
		return fmt.Errorf(
			"REPLICATION_MINIMUM_FACTOR must be >= 1; got %d",
			c.Replication.MinimumFactor,
		)
	}
	if c.Replication.MaximumFactor > 0 &&
		c.Replication.MinimumFactor > c.Replication.MaximumFactor {
		return fmt.Errorf(
			"REPLICATION_MINIMUM_FACTOR (%d) cannot exceed REPLICATION_MAXIMUM_FACTOR (%d)",
			c.Replication.MinimumFactor, c.Replication.MaximumFactor,
		)
	}
	return nil
}

// Mirrors entities/vectorindex/config.go; duplicated as plain strings to
// avoid an import cycle. VectorIndexTypeNone is an internal sentinel.
var validRestrictionVectorIndexTypes = []string{"hnsw", "flat", "dynamic", "hfresh"}

// Matches DEFAULT_QUANTIZATION values so operators can copy them across.
// "none" means "uncompressed"; omitting it from the allow-list makes
// every non-hfresh class require a compression.
var validRestrictionCompressionTypes = []string{"none", "pq", "sq", "rq-1", "rq-8", "bq"}

func IsValidRestrictionVectorIndexType(v string) bool {
	return slices.Contains(validRestrictionVectorIndexTypes, strings.ToLower(strings.TrimSpace(v)))
}

func IsValidRestrictionCompressionType(v string) bool {
	return slices.Contains(validRestrictionCompressionTypes, strings.ToLower(strings.TrimSpace(v)))
}

// makeRestrictionListValidator rejects unknown entries at SetValue time.
// Cross-field rules (default-matching, hfresh+compression) stay in
// validateRestrictions/ValidateRestrictionsRuntime — SetValue sees one
// field at a time.
func makeRestrictionListValidator(valid []string, envName string) func([]string) error {
	return func(val []string) error {
		for _, entry := range val {
			entry = strings.ToLower(strings.TrimSpace(entry))
			if entry == "" {
				continue
			}
			if !slices.Contains(valid, entry) {
				return fmt.Errorf("%s contains invalid entry %q; valid values are: %s",
					envName, entry, strings.Join(valid, ", "))
			}
		}
		return nil
	}
}

func NewRestrictionVectorIndexTypeListValidator() func([]string) error {
	return makeRestrictionListValidator(validRestrictionVectorIndexTypes, "ALLOWED_VECTOR_INDEX_TYPES")
}

func NewRestrictionCompressionTypeListValidator() func([]string) error {
	return makeRestrictionListValidator(validRestrictionCompressionTypes, "ALLOWED_COMPRESSION_TYPES")
}

// NewDefaultVectorIndexValidator returns the per-value validator attached to
// DefaultVectorIndexType. Empty means "unset", which the defaults path falls
// back from. The "none" sentinel is reserved for indexes dropped via
// DeleteClassVectorIndex and must never appear as a class-creation default —
// both env-time and runtime-override paths share this rule.
//
// The check is strict (exact match, no lowercase/trim) because
// DynamicValue.SetValue stores the value verbatim and downstream parsers
// (e.g. usecases/schema/parser.parseGivenVectorIndexConfig) compare
// case-sensitively. Callers that need to accept mixed-case input — like the
// env path — normalize before calling SetValue.
func NewDefaultVectorIndexValidator() func(string) error {
	return func(val string) error {
		if val == "" {
			return nil
		}
		if !slices.Contains(validRestrictionVectorIndexTypes, val) {
			return fmt.Errorf("invalid DEFAULT_VECTOR_INDEX %q, must be one of: %v",
				val, validRestrictionVectorIndexTypes)
		}
		return nil
	}
}

// ValidateRestrictionsRuntime is the cross-field layer of validation
// (per-value runs at SetValue time). On failure it logs and clears both
// allow-lists — reverting to the prior value would need a snapshot
// outside the DynamicValue, which buys little over fail-safe-and-warn.
func (c *Config) ValidateRestrictionsRuntime(log logrus.FieldLogger) error {
	allowVector, vErr := normalizeRestrictionList(c.Restrictions.AllowedVectorIndexTypes, validRestrictionVectorIndexTypes, "ALLOWED_VECTOR_INDEX_TYPES")
	allowCompression, cErr := normalizeRestrictionList(c.Restrictions.AllowedCompressionTypes, validRestrictionCompressionTypes, "ALLOWED_COMPRESSION_TYPES")

	var problems []string
	if vErr != nil {
		problems = append(problems, vErr.Error())
	}
	if cErr != nil {
		problems = append(problems, cErr.Error())
	}
	if len(allowVector) == 1 && allowVector[0] == "hfresh" && len(allowCompression) > 0 {
		problems = append(problems, "ALLOWED_COMPRESSION_TYPES cannot be set when ALLOWED_VECTOR_INDEX_TYPES is exclusively 'hfresh': hfresh has no compression configuration")
	}

	problems = append(problems, runtimeMismatchProblems(
		allowVector, c.DefaultVectorIndexType,
		"ALLOWED_VECTOR_INDEX_TYPES", "DEFAULT_VECTOR_INDEX",
	)...)
	problems = append(problems, runtimeMismatchProblems(
		allowCompression, c.DefaultQuantization,
		"ALLOWED_COMPRESSION_TYPES", "DEFAULT_QUANTIZATION",
	)...)

	if len(problems) == 0 {
		return nil
	}

	log.WithFields(logrus.Fields{
		"action":   "runtime_overrides_restrictions_invalid",
		"problems": problems,
	}).Errorf("runtime override violates restriction invariants — resetting allow-lists to empty (no restriction) until fixed: %s", strings.Join(problems, "; "))

	// Fail-safe: drop both allow-lists.
	if c.Restrictions.AllowedVectorIndexTypes != nil {
		_ = c.Restrictions.AllowedVectorIndexTypes.SetValue(nil)
	}
	if c.Restrictions.AllowedCompressionTypes != nil {
		_ = c.Restrictions.AllowedCompressionTypes.SetValue(nil)
	}
	return nil
}

// validateRestrictions enforces the boot-time cross-field rules between
// the allow-lists and DEFAULT_VECTOR_INDEX / DEFAULT_QUANTIZATION. May
// seed the matching default when a single-entry allow-list leaves it
// unset, so downstream readers don't need to special-case.
func (c *Config) validateRestrictions() error {
	allowVector, err := normalizeRestrictionList(c.Restrictions.AllowedVectorIndexTypes, validRestrictionVectorIndexTypes, "ALLOWED_VECTOR_INDEX_TYPES")
	if err != nil {
		return err
	}
	allowCompression, err := normalizeRestrictionList(c.Restrictions.AllowedCompressionTypes, validRestrictionCompressionTypes, "ALLOWED_COMPRESSION_TYPES")
	if err != nil {
		return err
	}

	if len(allowVector) == 1 && allowVector[0] == "hfresh" && len(allowCompression) > 0 {
		return fmt.Errorf("ALLOWED_COMPRESSION_TYPES cannot be set when ALLOWED_VECTOR_INDEX_TYPES is exclusively 'hfresh': hfresh has no compression configuration")
	}

	if err := reconcileAllowListWithDefault(
		allowVector, c.DefaultVectorIndexType,
		"ALLOWED_VECTOR_INDEX_TYPES", "DEFAULT_VECTOR_INDEX",
	); err != nil {
		return err
	}
	if err := reconcileAllowListWithDefault(
		allowCompression, c.DefaultQuantization,
		"ALLOWED_COMPRESSION_TYPES", "DEFAULT_QUANTIZATION",
	); err != nil {
		return err
	}

	// Persist normalized form: a whitespace-only input ("X=,") collapses
	// to an empty list — leaving the original would force the schema
	// layer to enforce an allow-list of blank strings.
	if c.Restrictions.AllowedVectorIndexTypes != nil {
		_ = c.Restrictions.AllowedVectorIndexTypes.SetValue(allowVector)
	}
	if c.Restrictions.AllowedCompressionTypes != nil {
		_ = c.Restrictions.AllowedCompressionTypes.SetValue(allowCompression)
	}

	return nil
}

// normalizeRestrictionList trims/lowercases/dedupes entries and rejects
// unknowns. Returns nil for the "no restriction" case (nil or empty dv).
func normalizeRestrictionList(dv *runtime.DynamicValue[[]string], valid []string, envName string) ([]string, error) {
	if dv == nil {
		return nil, nil
	}
	raw := dv.Get()
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, v := range raw {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		validEntry := false
		for _, w := range valid {
			if v == w {
				validEntry = true
				break
			}
		}
		if !validEntry {
			return nil, fmt.Errorf("%s contains invalid entry %q; valid values are: %s",
				envName, v, strings.Join(valid, ", "))
		}
		out = append(out, v)
	}
	return out, nil
}

// reconcileAllowListWithDefault enforces "default must be in the list",
// seeding the default in the single-entry / unset-default case.
func reconcileAllowListWithDefault(allowList []string, defaultDV *runtime.DynamicValue[string], allowEnv, defaultEnv string) error {
	if len(allowList) == 0 {
		return nil
	}
	currentDefault := ""
	if defaultDV != nil {
		currentDefault = strings.ToLower(strings.TrimSpace(defaultDV.Get()))
	}
	if len(allowList) > 1 {
		if currentDefault == "" {
			return fmt.Errorf("%s lists multiple values (%s); %s must also be set to one of them",
				allowEnv, strings.Join(allowList, ", "), defaultEnv)
		}
		if !slices.Contains(allowList, currentDefault) {
			return fmt.Errorf("%s=%q is not in %s (%s)",
				defaultEnv, currentDefault, allowEnv, strings.Join(allowList, ", "))
		}
		if defaultDV != nil && defaultDV.Get() != currentDefault {
			_ = defaultDV.SetValue(currentDefault)
		}
		return nil
	}
	only := allowList[0]
	if currentDefault == "" {
		if defaultDV != nil {
			_ = defaultDV.SetValue(only)
		}
		return nil
	}
	if currentDefault != only {
		return fmt.Errorf("%s=%q does not match the only entry in %s (%q)",
			defaultEnv, currentDefault, allowEnv, only)
	}
	if defaultDV != nil && defaultDV.Get() != currentDefault {
		_ = defaultDV.SetValue(currentDefault)
	}
	return nil
}

// runtimeMismatchProblems is the runtime-hook equivalent of
// reconcileAllowListWithDefault: collect every mismatch (one log line
// per operator change) and never seed defaults at runtime — the boot
// path is the only place we may rewrite an unset default.
func runtimeMismatchProblems(allowList []string, defaultDV *runtime.DynamicValue[string], allowEnv, defaultEnv string) []string {
	if len(allowList) == 0 {
		return nil
	}
	currentDefault := ""
	if defaultDV != nil {
		currentDefault = strings.ToLower(strings.TrimSpace(defaultDV.Get()))
	}
	var out []string
	if len(allowList) > 1 {
		if currentDefault == "" {
			out = append(out, fmt.Sprintf("%s lists multiple values (%s); %s must also be set to one of them",
				allowEnv, strings.Join(allowList, ", "), defaultEnv))
		} else if !slices.Contains(allowList, currentDefault) {
			out = append(out, fmt.Sprintf("%s=%q is not in %s (%s)",
				defaultEnv, currentDefault, allowEnv, strings.Join(allowList, ", ")))
		}
		return out
	}
	// Single-entry: unset default is tolerated (seeding is boot-only).
	if currentDefault != "" && currentDefault != allowList[0] {
		out = append(out, fmt.Sprintf("%s=%q does not match the only entry in %s (%q)",
			defaultEnv, currentDefault, allowEnv, allowList[0]))
	}
	return out
}

// ValidateModules validates the non-nested parameters. Nested objects must provide their own
// validation methods
func (c *Config) ValidateModules(modProv moduleProvider) error {
	if err := c.validateDefaultVectorizerModule(modProv); err != nil {
		return errors.Wrap(err, "default vectorizer module")
	}

	if err := c.validateDefaultVectorDistanceMetric(); err != nil {
		return errors.Wrap(err, "default vector distance metric")
	}

	return nil
}

func (c *Config) validateDefaultVectorizerModule(modProv moduleProvider) error {
	if c.DefaultVectorizerModule == VectorizerModuleNone {
		return nil
	}

	return modProv.ValidateVectorizer(c.DefaultVectorizerModule)
}

type moduleProvider interface {
	ValidateVectorizer(moduleName string) error
}

func (c *Config) validateDefaultVectorDistanceMetric() error {
	switch c.DefaultVectorDistanceMetric {
	case "", common.DistanceCosine, common.DistanceDot, common.DistanceL2Squared, common.DistanceManhattan, common.DistanceHamming:
		return nil
	default:
		return fmt.Errorf("must be one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]")
	}
}

type AutoSchema struct {
	Enabled       *runtime.DynamicValue[bool] `json:"enabled" yaml:"enabled"`
	DefaultString string                      `json:"defaultString" yaml:"defaultString"`
	DefaultNumber string                      `json:"defaultNumber" yaml:"defaultNumber"`
	DefaultDate   string                      `json:"defaultDate" yaml:"defaultDate"`
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
	Limit        int64 `json:"limit" yaml:"limit"`
	LimitGraphQL int64 `json:"limitGraphQL" yaml:"limitGraphQL"`
}

// DefaultBackupMinChunkSize is the default minimum size for backup chunks
const DefaultBackupMinChunkSize = 1024 * 1024 // 1MB

// DefaultBackupChunkTargetSize is the default target size for packing small files into chunks
const DefaultBackupChunkTargetSize = 10 * 1024 * 1024 // 10MB

// DefaultBackupSplitFileSize is the default size for splitting large files during backup
const DefaultBackupSplitFileSize = 50 * 1024 * 1024 * 1024 // 50GB

// Backup contains backup-related configuration
type Backup struct {
	MinChunkSize    int64 `json:"min_chunk_size" yaml:"min_chunk_size"`
	ChunkTargetSize int64 `json:"chunk_target_size" yaml:"chunk_target_size"`
	SplitFileSize   int64 `json:"split_file_size" yaml:"split_file_size"`

	// SkipAccessCheck disables the write+delete probe the backup client runs on
	// initialize, deferring write/permission errors to backup time. Use it for
	// least-privilege credentials that can write but lack DeleteObject.
	// Env: BACKUP_SKIP_ACCESS_CHECK.
	SkipAccessCheck bool `json:"skip_access_check" yaml:"skip_access_check"`
}

// DefaultQueryDefaultsLimit is the default query limit when no limit is provided
const (
	DefaultQueryDefaultsLimit        int64 = 10
	DefaultQueryDefaultsLimitGraphQL int64 = 100
)

type Contextionary struct {
	URL string `json:"url" yaml:"url"`
}

// Support independent TLS credentials for gRPC
type GRPC struct {
	Port            int           `json:"port" yaml:"port"`
	CertFile        string        `json:"certFile" yaml:"certFile"`
	KeyFile         string        `json:"keyFile" yaml:"keyFile"`
	MaxMsgSize      int           `json:"maxMsgSize" yaml:"maxMsgSize"`
	MaxOpenConns    int           `json:"maxOpenConns" yaml:"maxOpenConns"`
	IdleConnTimeout time.Duration `json:"idleConnTimeout" yaml:"idleConnTimeout"`
	WebEnabled      bool          `json:"webEnabled" yaml:"webEnabled"`
}

type MCP struct {
	Enabled            *runtime.DynamicValue[bool] `json:"enabled" yaml:"enabled"`
	WriteAccessEnabled *runtime.DynamicValue[bool] `json:"writeAccessEnabled" yaml:"writeAccessEnabled"`
	ConfigPath         string                      `json:"configPath" yaml:"configPath"`
}

type Profiling struct {
	BlockProfileRate     int  `json:"blockProfileRate" yaml:"blockProfileRate"`
	MutexProfileFraction int  `json:"mutexProfileFraction" yaml:"mutexProfileFraction"`
	Disabled             bool `json:"disabled" yaml:"disabled"`
	Port                 int  `json:"port" yaml:"port"`
	// DebugEndpointsEnabled gates the debug HTTP listener (pprof, fgprof,
	// /debug/*). The listener always binds: while this is false the port is
	// open but every request returns 404, checked per-request so runtime
	// flips need no restart. GO_PROFILING_DISABLE (Disabled) is a separate
	// switch that stops the listener binding at all.
	DebugEndpointsEnabled *runtime.DynamicValue[bool] `json:"debugEndpointsEnabled" yaml:"debugEndpointsEnabled"`
}

type DistributedTasksConfig struct {
	CompletedTaskTTL      time.Duration              `json:"completedTaskTTL" yaml:"completedTaskTTL"`
	SchedulerTickInterval time.Duration              `json:"schedulerTickInterval" yaml:"schedulerTickInterval"`
	ReindexConcurrency    *runtime.DynamicValue[int] `json:"reindexConcurrency" yaml:"reindexConcurrency"`
}

type Persistence struct {
	DataPath                            string `json:"dataPath" yaml:"dataPath"`
	MemtablesFlushDirtyAfter            int    `json:"flushDirtyMemtablesAfter" yaml:"flushDirtyMemtablesAfter"`
	MemtablesMaxSizeMB                  int    `json:"memtablesMaxSizeMB" yaml:"memtablesMaxSizeMB"`
	MemtablesMinActiveDurationSeconds   int    `json:"memtablesMinActiveDurationSeconds" yaml:"memtablesMinActiveDurationSeconds"`
	MemtablesMaxActiveDurationSeconds   int    `json:"memtablesMaxActiveDurationSeconds" yaml:"memtablesMaxActiveDurationSeconds"`
	LSMMaxSegmentSize                   int64  `json:"lsmMaxSegmentSize" yaml:"lsmMaxSegmentSize"`
	LSMSegmentsCleanupIntervalSeconds   int    `json:"lsmSegmentsCleanupIntervalSeconds" yaml:"lsmSegmentsCleanupIntervalSeconds"`
	LSMSeparateObjectsCompactions       bool   `json:"lsmSeparateObjectsCompactions" yaml:"lsmSeparateObjectsCompactions"`
	LSMEnableSegmentsChecksumValidation bool   `json:"lsmEnableSegmentsChecksumValidation" yaml:"lsmEnableSegmentsChecksumValidation"`
	LSMSkipWriteClassNameEnabled        bool   `json:"lsmSkipClassNameEnabled" yaml:"lsmSkipClassNameEnabled"`
	LSMCycleManagerRoutinesFactor       int    `json:"lsmCycleManagerRoutinesFactor" yaml:"lsmCycleManagerRoutinesFactor"`
	IndexRangeableInMemory              bool   `json:"indexRangeableInMemory" yaml:"indexRangeableInMemory"`
	MinMMapSize                         int64  `json:"minMMapSize" yaml:"minMMapSize"`
	LazySegmentsDisabled                bool   `json:"lazySegmentsDisabled" yaml:"lazySegmentsDisabled"`
	SegmentInfoIntoFileNameEnabled      bool   `json:"segmentFileInfoEnabled" yaml:"segmentFileInfoEnabled"`
	WriteMetadataFilesEnabled           bool   `json:"writeMetadataFilesEnabled" yaml:"writeMetadataFilesEnabled"`
	MaxReuseWalSize                     int64  `json:"MaxReuseWalSize" yaml:"MaxReuseWalSize"`
	HNSWMaxLogSize                      int64  `json:"hnswMaxLogSize" yaml:"hnswMaxLogSize"`

	// HNSW snapshot settings below are deprecated no-ops. Kept for YAML/JSON
	// back-compat so existing config files parse without error. No consumer
	// reads them. The env-var equivalents produce a deprecation warning at
	// startup in environment.go; YAML/JSON sets are silently accepted
	// because bool defaults make "user set false" indistinguishable from
	// "unset". See PR #9988 for context.
	HNSWDisableSnapshots                         bool `json:"hnswDisableSnapshots" yaml:"hnswDisableSnapshots"`
	HNSWSnapshotIntervalSeconds                  int  `json:"hnswSnapshotIntervalSeconds" yaml:"hnswSnapshotIntervalSeconds"`
	HNSWSnapshotOnStartup                        bool `json:"hnswSnapshotOnStartup" yaml:"hnswSnapshotOnStartup"`
	HNSWSnapshotMinDeltaCommitlogsNumber         int  `json:"hnswSnapshotMinDeltaCommitlogsNumber" yaml:"hnswSnapshotMinDeltaCommitlogsNumber"`
	HNSWSnapshotMinDeltaCommitlogsSizePercentage int  `json:"hnswSnapshotMinDeltaCommitlogsSizePercentage" yaml:"hnswSnapshotMinDeltaCommitlogsSizePercentage"`
}

// DefaultPersistenceDataPath is the default location for data directory when no location is provided
const DefaultPersistenceDataPath string = "./data"

// DefaultPersistenceLSMMaxSegmentSize is effectively unlimited for backward
// compatibility. TODO: consider changing this in a future release and make
// some noise about it. This is technically a breaking change.
const DefaultPersistenceLSMMaxSegmentSize = math.MaxInt64

// DefaultPersistenceLSMSegmentsCleanupIntervalSeconds = 0 for backward compatibility.
// value = 0 means cleanup is turned off.
const DefaultPersistenceLSMSegmentsCleanupIntervalSeconds = 0

// DefaultPersistenceLSMCycleManagerRoutinesFactor - determines how many goroutines
// are started for cyclemanager (factor * NUMCPU)
const DefaultPersistenceLSMCycleManagerRoutinesFactor = 2

const DefaultPersistenceHNSWMaxLogSize = 500 * 1024 * 1024 // 500MB for backward compatibility

const (
	DefaultReindexerGoroutinesFactor = 0.5
)

// MetadataServer is experimental.
type MetadataServer struct {
	// When enabled startup will include a "metadata server"
	// for separation of storage/compute Weaviate.
	Enabled                   bool   `json:"enabled" yaml:"enabled"`
	GrpcListenAddress         string `json:"grpc_listen_address" yaml:"grpc_listen_address"`
	DataEventsChannelCapacity int    `json:"data_events_channel_capacity" yaml:"data_events_channel_capacity"`
}

const (
	DefaultMetadataServerGrpcListenAddress         = ":9050"
	DefaultMetadataServerDataEventsChannelCapacity = 100
)

const DefaultHNSWVisitedListPoolSize = -1 // unlimited for backward compatibility

const DefaultHNSWFlatSearchConcurrency = 1 // 1 for backward compatibility

const (
	DefaultPersistenceMinMMapSize     = 8192 // 8kb by default
	DefaultPersistenceMaxReuseWalSize = 4096 // 4kb by default
)

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

// Export holds operator-level configuration for data exports.
// Both fields support runtime overrides via the runtime config YAML
// (using flat keys export_enabled / export_default_bucket).
type Export struct {
	// Enabled controls whether the export API is available. Defaults to false.
	// Env: EXPORT_ENABLED, runtime config: export_enabled.
	Enabled *runtime.DynamicValue[bool] `json:"enabled" yaml:"enabled"`

	// DefaultBucket is the storage bucket used for exports (e.g. S3 bucket name).
	// Not required for backends that do not use buckets (e.g. filesystem).
	// Env: EXPORT_DEFAULT_BUCKET, runtime config: export_default_bucket.
	DefaultBucket *runtime.DynamicValue[string] `json:"default_bucket" yaml:"default_bucket"`

	// DefaultPath is the default path prefix within the bucket or filesystem for exports.
	// Defaults to empty string (no prefix). Each backup module provides a separate
	// export backend that does not inherit the backup path (e.g. BACKUP_S3_PATH),
	// so this value is used directly.
	// Env: EXPORT_DEFAULT_PATH, runtime config: export_default_path.
	DefaultPath *runtime.DynamicValue[string] `json:"default_path" yaml:"default_path"`

	// SkipAccessCheck disables the write+delete probe the export client runs on
	// initialize, deferring write/permission errors to export time. Use it for
	// least-privilege credentials that can write but lack DeleteObject.
	// Env: EXPORT_SKIP_ACCESS_CHECK.
	SkipAccessCheck bool `json:"skip_access_check" yaml:"skip_access_check"`
}

// Namespaces configures cluster-level namespace support.
//
// NAMESPACES_ENABLED is a cluster-wide feature flag. Once enabled on a
// bootstrapping cluster it is the exclusive source of truth for namespace
// existence (see cluster/namespaces). The flag must not be toggled on an
// already-populated cluster; startup invariants refuse such configurations.
type Namespaces struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// CleanupInterval drives the deleting-namespace sweep on the leader.
	// NAMESPACE_CLEANUP_INTERVAL; <= 0 disables.
	CleanupInterval *runtime.DynamicValue[time.Duration] `json:"cleanup_interval" yaml:"cleanup_interval"`
}

const (
	DefaultCORSAllowOrigin  = "*"
	DefaultCORSAllowMethods = "*"
	DefaultCORSAllowHeaders = "Content-Type, Authorization, Batch, X-Openai-Api-Key, X-Openai-Organization, X-Openai-Baseurl, X-Anyscale-Baseurl, X-Anyscale-Api-Key, X-Cohere-Api-Key, X-Cohere-Baseurl, X-Huggingface-Api-Key, X-Azure-Api-Key, X-Azure-Deployment-Id, X-Azure-Resource-Name, X-Azure-Concurrency, X-Azure-Block-Size, X-Google-Api-Key, X-Google-Vertex-Api-Key, X-Google-Studio-Api-Key, X-Goog-Api-Key, X-Goog-Vertex-Api-Key, X-Goog-Studio-Api-Key, X-Palm-Api-Key, X-Jinaai-Api-Key, X-Aws-Access-Key, X-Aws-Secret-Key, X-Voyageai-Baseurl, X-Voyageai-Api-Key, X-Mistral-Baseurl, X-Mistral-Api-Key, X-Anthropic-Baseurl, X-Anthropic-Api-Key, X-Databricks-Endpoint, X-Databricks-Token, X-Databricks-User-Agent, X-Friendli-Token, X-Friendli-Baseurl, X-Weaviate-Api-Key, X-Weaviate-Cluster-Url, X-Weaviate-Client, X-Nvidia-Api-Key, X-Nvidia-Baseurl, X-ContextualAI-Baseurl, X-ContextualAI-Api-Key, X-Digitalocean-Baseurl, X-Digitalocean-Api-Key, X-Deepseek-Baseurl, X-Deepseek-Api-Key"
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
	Port              int
	InternalRPCPort   int
	RPCMessageMaxSize int
	Join              []string

	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64

	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	TimeoutsMultiplier *runtime.DynamicValue[int]
	DrainSleep         *runtime.DynamicValue[time.Duration]

	ConsistencyWaitTimeout time.Duration

	BootstrapTimeout   time.Duration
	BootstrapExpect    int
	MetadataOnlyVoters bool

	EnableOneNodeRecovery bool
	ForceOneNodeRecovery  bool
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

	return f.Config.Validate()
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
			return config, fmt.Errorf("error unmarshalling the json config file: %w", err)
		}
	case "yaml":
		err := yaml.Unmarshal(file, &config)
		if err != nil {
			return config, fmt.Errorf("error unmarshalling the yaml config file: %w", err)
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

	if flags.RuntimeOverridesEnabled {
		f.Config.RuntimeOverrides.Enabled = flags.RuntimeOverridesEnabled
	}

	if flags.RuntimeOverridesPath != "" {
		f.Config.RuntimeOverrides.Path = flags.RuntimeOverridesPath
	}

	if flags.RuntimeOverridesLoadInterval > 0 {
		f.Config.RuntimeOverrides.LoadInterval = flags.RuntimeOverridesLoadInterval
	}
}

func configErr(err error) error {
	return fmt.Errorf("invalid config: %w", err)
}
