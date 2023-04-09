//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// FromEnv takes a *Config as it will respect initial config that has been
// provided by other means (e.g. a config file) and will only extend those that
// are set
func FromEnv(config *Config) error {
	if enabled(os.Getenv("PROMETHEUS_MONITORING_ENABLED")) {
		config.Monitoring.Enabled = true
		config.Monitoring.Tool = "prometheus"
		config.Monitoring.Port = 2112
	}

	if enabled(os.Getenv("TRACK_VECTOR_DIMENSIONS")) {
		config.TrackVectorDimensions = true
	}

	if enabled(os.Getenv("REINDEX_VECTOR_DIMENSIONS_AT_STARTUP")) {
		if config.TrackVectorDimensions {
			config.ReindexVectorDimensionsAtStartup = true
		}
	}

	if enabled(os.Getenv("REINDEX_SET_TO_ROARINGSET_AT_STARTUP")) {
		config.ReindexSetToRoaringsetAtStartup = true
	}

	if v := os.Getenv("PROMETHEUS_MONITORING_PORT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse PROMETHEUS_MONITORING_PORT as int")
		}

		config.Monitoring.Port = asInt
	}

	if enabled(os.Getenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED")) {
		config.Authentication.AnonymousAccess.Enabled = true
	}

	if enabled(os.Getenv("AUTHENTICATION_OIDC_ENABLED")) {
		config.Authentication.OIDC.Enabled = true

		if enabled(os.Getenv("AUTHENTICATION_OIDC_SKIP_CLIENT_ID_CHECK")) {
			config.Authentication.OIDC.SkipClientIDCheck = true
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_ISSUER"); v != "" {
			config.Authentication.OIDC.Issuer = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_CLIENT_ID"); v != "" {
			config.Authentication.OIDC.ClientID = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_SCOPES"); v != "" {
			config.Authentication.OIDC.Scopes = strings.Split(v, ",")
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_USERNAME_CLAIM"); v != "" {
			config.Authentication.OIDC.UsernameClaim = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_GROUPS_CLAIM"); v != "" {
			config.Authentication.OIDC.GroupsClaim = v
		}
	}

	if enabled(os.Getenv("AUTHENTICATION_APIKEY_ENABLED")) {
		config.Authentication.APIKey.Enabled = true

		if keysString, ok := os.LookupEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS"); ok {
			keys := strings.Split(keysString, ",")
			config.Authentication.APIKey.AllowedKeys = keys
		}

		if keysString, ok := os.LookupEnv("AUTHENTICATION_APIKEY_USERS"); ok {
			keys := strings.Split(keysString, ",")
			config.Authentication.APIKey.Users = keys
		}
	}

	if enabled(os.Getenv("AUTHORIZATION_ADMINLIST_ENABLED")) {
		config.Authorization.AdminList.Enabled = true

		usersString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_USERS")
		if ok {
			config.Authorization.AdminList.Users = strings.Split(usersString, ",")
		}

		roUsersString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_READONLY_USERS")
		if ok {
			config.Authorization.AdminList.ReadOnlyUsers = strings.Split(roUsersString, ",")
		}
	}

	clusterCfg, err := parseClusterConfig()
	if err != nil {
		return err
	}
	config.Cluster = clusterCfg

	if v := os.Getenv("PERSISTENCE_DATA_PATH"); v != "" {
		config.Persistence.DataPath = v
	}

	if err := config.parseMemtableConfig(); err != nil {
		return err
	}

	if v := os.Getenv("ORIGIN"); v != "" {
		config.Origin = v
	}

	if v := os.Getenv("CONTEXTIONARY_URL"); v != "" {
		config.Contextionary.URL = v
	}

	if v := os.Getenv("QUERY_DEFAULTS_LIMIT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse QUERY_DEFAULTS_LIMIT as int")
		}

		config.QueryDefaults.Limit = int64(asInt)
	}

	if v := os.Getenv("QUERY_MAXIMUM_RESULTS"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse QUERY_MAXIMUM_RESULTS as int")
		}

		config.QueryMaximumResults = int64(asInt)
	} else {
		config.QueryMaximumResults = DefaultQueryMaximumResults
	}

	if v := os.Getenv("MAX_IMPORT_GOROUTINES_FACTOR"); v != "" {
		asFloat, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return errors.Wrapf(err, "parse MAX_IMPORT_GOROUTINES_FACTOR as float")
		} else if asFloat <= 0 {
			return errors.New("negative MAX_IMPORT_GOROUTINES_FACTOR factor")
		}

		config.MaxImportGoroutinesFactor = asFloat
	} else {
		config.MaxImportGoroutinesFactor = DefaultMaxImportGoroutinesFactor
	}

	if v := os.Getenv("DEFAULT_VECTORIZER_MODULE"); v != "" {
		config.DefaultVectorizerModule = v
	} else {
		// env not set, this could either mean, we already have a value from a file
		// or we explicitly want to set the value to "none"
		if config.DefaultVectorizerModule == "" {
			config.DefaultVectorizerModule = VectorizerModuleNone
		}
	}

	if v := os.Getenv("DEFAULT_VECTOR_DISTANCE_METRIC"); v != "" {
		config.DefaultVectorDistanceMetric = v
	}

	if v := os.Getenv("ENABLE_MODULES"); v != "" {
		config.EnableModules = v
	}

	config.AutoSchema.Enabled = true
	if v := os.Getenv("AUTOSCHEMA_ENABLED"); v != "" {
		config.AutoSchema.Enabled = !(strings.ToLower(v) == "false")
	}
	config.AutoSchema.DefaultString = "text"
	if v := os.Getenv("AUTOSCHEMA_DEFAULT_STRING"); v != "" {
		config.AutoSchema.DefaultString = v
	}
	config.AutoSchema.DefaultNumber = "number"
	if v := os.Getenv("AUTOSCHEMA_DEFAULT_NUMBER"); v != "" {
		config.AutoSchema.DefaultNumber = v
	}
	config.AutoSchema.DefaultDate = "date"
	if v := os.Getenv("AUTOSCHEMA_DEFAULT_DATE"); v != "" {
		config.AutoSchema.DefaultDate = v
	}

	ru, err := parseResourceUsageEnvVars()
	if err != nil {
		return err
	}
	config.ResourceUsage = ru

	if v := os.Getenv("GO_BLOCK_PROFILE_RATE"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse GO_BLOCK_PROFILE_RATE as int")
		}

		config.Profiling.BlockProfileRate = asInt
	}

	if v := os.Getenv("GO_MUTEX_PROFILE_FRACTION"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse GO_MUTEX_PROFILE_FRACTION as int")
		}

		config.Profiling.MutexProfileFraction = asInt
	}

	if v := os.Getenv("MAXIMUM_CONCURRENT_GET_REQUESTS"); v != "" {
		asInt, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "parse MAXIMUM_CONCURRENT_GET_REQUESTS as int")
		}
		config.MaximumConcurrentGetRequests = int(asInt)
	} else {
		config.MaximumConcurrentGetRequests = DefaultMaxConcurrentGetRequests
	}

	config.DefaultVectorIndexType = VectorIndexTypeHNSW
	if v := os.Getenv("DEFAULT_VECTOR_INDEX_TYPE"); v != "" {
		config.DefaultVectorIndexType = v
	}

	if v := os.Getenv("MODULES_PATH"); v != "" {
		config.ModulesPath = v // "/Users/gwilliams/Projects/GSI/Weaviate/github.fork/weaviate/modules"
	}

	return nil
}

func (c *Config) parseMemtableConfig() error {
	// first parse old name for flush value
	if err := parsePositiveInt(
		"PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER",
		func(val int) { c.Persistence.FlushIdleMemtablesAfter = val },
		DefaultPersistenceFlushIdleMemtablesAfter,
	); err != nil {
		return err
	}

	// then parse with new name and use previous value in case it's not set
	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS",
		func(val int) { c.Persistence.FlushIdleMemtablesAfter = val },
		c.Persistence.FlushIdleMemtablesAfter,
	); err != nil {
		return err
	}

	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_MAX_SIZE_MB",
		func(val int) { c.Persistence.MemtablesMaxSizeMB = val },
		DefaultPersistenceMemtablesMaxSize,
	); err != nil {
		return err
	}

	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_MIN_ACTIVE_DURATION_SECONDS",
		func(val int) { c.Persistence.MemtablesMinActiveDurationSeconds = val },
		DefaultPersistenceMemtablesMinDuration,
	); err != nil {
		return err
	}

	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_MAX_ACTIVE_DURATION_SECONDS",
		func(val int) { c.Persistence.MemtablesMaxActiveDurationSeconds = val },
		DefaultPersistenceMemtablesMaxDuration,
	); err != nil {
		return err
	}

	return nil
}

func parsePositiveInt(varName string, cb func(val int), defaultValue int) error {
	if v := os.Getenv(varName); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %s as int: %w", varName, err)
		} else if asInt <= 0 {
			return fmt.Errorf("%s must be a positive value larger 0", varName)
		}

		cb(asInt)
	} else {
		cb(defaultValue)
	}

	return nil
}

const DefaultQueryMaximumResults = int64(10000)

const (
	DefaultPersistenceFlushIdleMemtablesAfter = 60
	DefaultPersistenceMemtablesMaxSize        = 200
	DefaultPersistenceMemtablesMinDuration    = 15
	DefaultPersistenceMemtablesMaxDuration    = 45
	DefaultMaxConcurrentGetRequests           = 0
)

const VectorizerModuleNone = "none"

const VectorIndexTypeHNSW = "hnsw" // The default builtin index type

// DefaultGossipBindPort uses the hashicorp/memberlist default
// port value assigned with the use of DefaultLocalConfig
// TODO - 7946 may conflict with Docker Swarm
const DefaultGossipBindPort = 7946

// TODO: This should be retrieved dynamically from all installed modules
const VectorizerModuleText2VecContextionary = "text2vec-contextionary"

func enabled(value string) bool {
	if value == "" {
		return false
	}

	if value == "on" ||
		value == "enabled" ||
		value == "1" ||
		value == "true" {
		return true
	}

	return false
}

func parseResourceUsageEnvVars() (ResourceUsage, error) {
	ru := ResourceUsage{}

	if v := os.Getenv("DISK_USE_WARNING_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, errors.Wrapf(err, "parse DISK_USE_WARNING_PERCENTAGE as uint")
		}
		ru.DiskUse.WarningPercentage = asUint
	} else {
		ru.DiskUse.WarningPercentage = DefaultDiskUseWarningPercentage
	}

	if v := os.Getenv("DISK_USE_READONLY_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, errors.Wrapf(err, "parse DISK_USE_READONLY_PERCENTAGE as uint")
		}
		ru.DiskUse.ReadOnlyPercentage = asUint
	} else {
		ru.DiskUse.ReadOnlyPercentage = DefaultDiskUseReadonlyPercentage
	}

	if v := os.Getenv("MEMORY_WARNING_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, errors.Wrapf(err, "parse MEMORY_WARNING_PERCENTAGE as uint")
		}
		ru.MemUse.WarningPercentage = asUint
	} else {
		ru.MemUse.WarningPercentage = DefaultMemUseWarningPercentage
	}

	if v := os.Getenv("MEMORY_READONLY_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, errors.Wrapf(err, "parse MEMORY_READONLY_PERCENTAGE as uint")
		}
		ru.MemUse.ReadOnlyPercentage = asUint
	} else {
		ru.MemUse.ReadOnlyPercentage = DefaultMemUseReadonlyPercentage
	}

	return ru, nil
}

func parseClusterConfig() (cluster.Config, error) {
	cfg := cluster.Config{}

	cfg.Hostname = os.Getenv("CLUSTER_HOSTNAME")
	cfg.Join = os.Getenv("CLUSTER_JOIN")

	gossipBind, gossipBindSet := os.LookupEnv("CLUSTER_GOSSIP_BIND_PORT")
	dataBind, dataBindSet := os.LookupEnv("CLUSTER_DATA_BIND_PORT")

	if gossipBindSet {
		asInt, err := strconv.Atoi(gossipBind)
		if err != nil {
			return cfg, fmt.Errorf("parse CLUSTER_GOSSIP_BIND_PORT as int: %w", err)
		}
		cfg.GossipBindPort = asInt
	} else {
		cfg.GossipBindPort = DefaultGossipBindPort
	}

	if dataBindSet {
		asInt, err := strconv.Atoi(dataBind)
		if err != nil {
			return cfg, fmt.Errorf("parse CLUSTER_DATA_BIND_PORT as int: %w", err)
		}
		cfg.DataBindPort = asInt
	} else {
		// it is convention in this server that the data bind point is
		// equal to the data bind port + 1
		cfg.DataBindPort = cfg.GossipBindPort + 1
	}

	if cfg.DataBindPort != cfg.GossipBindPort+1 {
		return cfg, fmt.Errorf("CLUSTER_DATA_BIND_PORT must be one port " +
			"number greater than CLUSTER_GOSSIP_BIND_PORT")
	}

	cfg.IgnoreStartupSchemaSync = enabled(
		os.Getenv("CLUSTER_IGNORE_SCHEMA_SYNC"))

	return cfg, nil
}
