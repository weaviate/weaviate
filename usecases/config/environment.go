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
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
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
		if config.TrackVectorDimensions == true {
			config.WantDimensionsReindex = true
		}
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

		if v := os.Getenv("AUTHENTICATION_OIDC_USERNAME_CLAIM"); v != "" {
			config.Authentication.OIDC.UsernameClaim = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_GROUPS_CLAIM"); v != "" {
			config.Authentication.OIDC.GroupsClaim = v
		}
	}

	if enabled(os.Getenv("AUTHORIZATION_ADMINLIST_ENABLED")) {
		config.Authorization.AdminList.Enabled = true

		users := strings.Split(os.Getenv("AUTHORIZATION_ADMINLIST_USERS"), ",")
		roUsers := strings.Split(os.Getenv("AUTHORIZATION_ADMINLIST_READONLY_USERS"),
			",")

		config.Authorization.AdminList.ReadOnlyUsers = roUsers
		config.Authorization.AdminList.Users = users
	}

	config.Cluster.Hostname = os.Getenv("CLUSTER_HOSTNAME")
	config.Cluster.Join = os.Getenv("CLUSTER_JOIN")

	if v := os.Getenv("CLUSTER_GOSSIP_BIND_PORT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse CLUSTER_GOSSIP_BIND_PORT as int")
		}

		config.Cluster.GossipBindPort = asInt
	}

	if v := os.Getenv("CLUSTER_DATA_BIND_PORT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse CLUSTER_DATA_BIND_PORT as int")
		}

		config.Cluster.DataBindPort = asInt
	}

	if v := os.Getenv("PERSISTENCE_DATA_PATH"); v != "" {
		config.Persistence.DataPath = v
	}

	if v := os.Getenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER as int")
		} else if asInt <= 0 {
			return errors.New("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER must be a positive value larger 0")
		}

		config.Persistence.FlushIdleMemtablesAfter = asInt
	} else {
		config.Persistence.FlushIdleMemtablesAfter = DefaultPersistenceFlushIdleMemtablesAfter
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

	return nil
}

const DefaultQueryMaximumResults = int64(10000)

const DefaultPersistenceFlushIdleMemtablesAfter = 60

const VectorizerModuleNone = "none"

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
