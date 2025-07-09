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
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	dbhelpers "github.com/weaviate/weaviate/adapters/repos/db/helpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	DefaultRaftPort         = 8300
	DefaultRaftInternalPort = 8301
	DefaultRaftGRPCMaxSize  = 1024 * 1024 * 1024
	// DefaultRaftBootstrapTimeout is the time raft will wait to bootstrap or rejoin the cluster on a restart. We set it
	// to 600 because if we're loading a large DB we need to wait for it to load before being able to join the cluster
	// on a single node cluster.
	DefaultRaftBootstrapTimeout = 600
	DefaultRaftBootstrapExpect  = 1
	DefaultRaftDir              = "raft"
	DefaultHNSWAcornFilterRatio = 0.4

	DefaultRuntimeOverridesLoadInterval = 2 * time.Minute

	DefaultDistributedTasksSchedulerTickInterval = time.Minute
	DefaultDistributedTasksCompletedTaskTTL      = 5 * 24 * time.Hour

	DefaultReplicationEngineMaxWorkers     = 10
	DefaultReplicaMovementMinimumAsyncWait = 60 * time.Second

	DefaultTransferInactivityTimeout = 5 * time.Minute
)

// FromEnv takes a *Config as it will respect initial config that has been
// provided by other means (e.g. a config file) and will only extend those that
// are set
func FromEnv(config *Config) error {
	if entcfg.Enabled(os.Getenv("PROMETHEUS_MONITORING_ENABLED")) {
		config.Monitoring.Enabled = true
		config.Monitoring.Tool = "prometheus"
		config.Monitoring.Port = 2112
		config.Monitoring.MetricsNamespace = "" // to support backward compabitlity. Metric names won't have prefix by default.

		if entcfg.Enabled(os.Getenv("PROMETHEUS_MONITORING_GROUP_CLASSES")) ||
			entcfg.Enabled(os.Getenv("PROMETHEUS_MONITORING_GROUP")) {
			// The variable was renamed with v1.20. Prior to v1.20 the recommended
			// way to do MT was using classes. This lead to a lot of metrics which
			// could be grouped with this variable. With v1.20 we introduced native
			// multi-tenancy. Now all you need is a single class, but you would
			// still get one set of metrics per shard. To prevent this, you still
			// want to group. The new name reflects that it's just about grouping,
			// not about classes or shards.
			config.Monitoring.Group = true
		}

		if val := strings.TrimSpace(os.Getenv("PROMETHEUS_MONITORING_METRIC_NAMESPACE")); val != "" {
			config.Monitoring.MetricsNamespace = val
		}

		if entcfg.Enabled(os.Getenv("PROMETHEUS_MONITOR_CRITICAL_BUCKETS_ONLY")) {
			config.Monitoring.MonitorCriticalBucketsOnly = true
		}
	}

	if entcfg.Enabled(os.Getenv("TRACK_VECTOR_DIMENSIONS")) {
		config.TrackVectorDimensions = true
	}

	if entcfg.Enabled(os.Getenv("REINDEX_VECTOR_DIMENSIONS_AT_STARTUP")) {
		if config.TrackVectorDimensions {
			config.ReindexVectorDimensionsAtStartup = true
		}
	}

	if entcfg.Enabled(os.Getenv("DISABLE_LAZY_LOAD_SHARDS")) {
		config.DisableLazyLoadShards = true
	}

	if entcfg.Enabled(os.Getenv("FORCE_FULL_REPLICAS_SEARCH")) {
		config.ForceFullReplicasSearch = true
	}

	if v := os.Getenv("TRANSFER_INACTIVITY_TIMEOUT"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse TRANSFER_INACTIVITY_TIMEOUT as duration: %w", err)
		}
		config.TransferInactivityTimeout = timeout
	} else {
		config.TransferInactivityTimeout = DefaultTransferInactivityTimeout
	}

	// Recount all property lengths at startup to support accurate BM25 scoring
	if entcfg.Enabled(os.Getenv("RECOUNT_PROPERTIES_AT_STARTUP")) {
		config.RecountPropertiesAtStartup = true
	}

	if entcfg.Enabled(os.Getenv("REINDEX_SET_TO_ROARINGSET_AT_STARTUP")) {
		config.ReindexSetToRoaringsetAtStartup = true
	}

	if entcfg.Enabled(os.Getenv("INDEX_MISSING_TEXT_FILTERABLE_AT_STARTUP")) {
		config.IndexMissingTextFilterableAtStartup = true
	}

	cptParser := newCollectionPropsTenantsParser()

	// variable expects string in format:
	// "Class1:property11,property12;Class2:property21,property22"
	if v := os.Getenv("REINDEX_INDEXES_AT_STARTUP"); v != "" {
		cpts, err := cptParser.parse(v)
		if err != nil {
			return fmt.Errorf("parse REINDEX_INDEXES_AT_STARTUP as class with props: %w", err)
		}

		asClassesWithProps := make(map[string][]string, len(cpts))
		for _, cpt := range cpts {
			asClassesWithProps[cpt.Collection] = cpt.Props
		}
		config.ReindexIndexesAtStartup = asClassesWithProps
	}

	if v := os.Getenv("PROMETHEUS_MONITORING_PORT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse PROMETHEUS_MONITORING_PORT as int: %w", err)
		}

		config.Monitoring.Port = asInt
	}

	if v := os.Getenv("GO_PROFILING_PORT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse GO_PROFILING_PORT as int: %w", err)
		}

		config.Profiling.Port = asInt
	}

	if entcfg.Enabled(os.Getenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED")) {
		config.Authentication.AnonymousAccess.Enabled = true
	}

	if entcfg.Enabled(os.Getenv("AUTHENTICATION_OIDC_ENABLED")) {
		config.Authentication.OIDC.Enabled = true
		var (
			skipClientCheck bool
			issuer          string
			clientID        string
			scopes          []string
			userClaim       string
			groupsClaim     string
			certificate     string
		)

		if entcfg.Enabled(os.Getenv("AUTHENTICATION_OIDC_SKIP_CLIENT_ID_CHECK")) {
			skipClientCheck = true
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_ISSUER"); v != "" {
			issuer = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_CLIENT_ID"); v != "" {
			clientID = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_SCOPES"); v != "" {
			scopes = strings.Split(v, ",")
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_USERNAME_CLAIM"); v != "" {
			userClaim = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_GROUPS_CLAIM"); v != "" {
			groupsClaim = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_CERTIFICATE"); v != "" {
			certificate = v
		}

		config.Authentication.OIDC.SkipClientIDCheck = runtime.NewDynamicValue(skipClientCheck)
		config.Authentication.OIDC.Issuer = runtime.NewDynamicValue(issuer)
		config.Authentication.OIDC.ClientID = runtime.NewDynamicValue(clientID)
		config.Authentication.OIDC.Scopes = runtime.NewDynamicValue(scopes)
		config.Authentication.OIDC.UsernameClaim = runtime.NewDynamicValue(userClaim)
		config.Authentication.OIDC.GroupsClaim = runtime.NewDynamicValue(groupsClaim)
		config.Authentication.OIDC.Certificate = runtime.NewDynamicValue(certificate)
	}

	if entcfg.Enabled(os.Getenv("AUTHENTICATION_DB_USERS_ENABLED")) {
		config.Authentication.DBUsers.Enabled = true
	}

	if entcfg.Enabled(os.Getenv("AUTHENTICATION_APIKEY_ENABLED")) {
		config.Authentication.APIKey.Enabled = true

		if rawKeys, ok := os.LookupEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS"); ok {
			keys := strings.Split(rawKeys, ",")
			config.Authentication.APIKey.AllowedKeys = keys
		}

		if rawUsers, ok := os.LookupEnv("AUTHENTICATION_APIKEY_USERS"); ok {
			users := strings.Split(rawUsers, ",")
			config.Authentication.APIKey.Users = users
		}

	}

	if entcfg.Enabled(os.Getenv("AUTHORIZATION_ADMINLIST_ENABLED")) {
		config.Authorization.AdminList.Enabled = true

		usersString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_USERS")
		if ok {
			config.Authorization.AdminList.Users = strings.Split(usersString, ",")
		}

		roUsersString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_READONLY_USERS")
		if ok {
			config.Authorization.AdminList.ReadOnlyUsers = strings.Split(roUsersString, ",")
		}

		groupsString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_GROUPS")
		if ok {
			config.Authorization.AdminList.Groups = strings.Split(groupsString, ",")
		}

		roGroupsString, ok := os.LookupEnv("AUTHORIZATION_ADMINLIST_READONLY_GROUPS")
		if ok {
			config.Authorization.AdminList.ReadOnlyGroups = strings.Split(roGroupsString, ",")
		}
	}

	if entcfg.Enabled(os.Getenv("AUTHORIZATION_ENABLE_RBAC")) || entcfg.Enabled(os.Getenv("AUTHORIZATION_RBAC_ENABLED")) {
		config.Authorization.Rbac.Enabled = true

		if entcfg.Enabled(os.Getenv("AUTHORIZATION_RBAC_IP_IN_AUDIT_LOG_DISABLED")) {
			config.Authorization.Rbac.IpInAuditDisabled = true
		}

		adminsString, ok := os.LookupEnv("AUTHORIZATION_RBAC_ROOT_USERS")
		if ok {
			config.Authorization.Rbac.RootUsers = strings.Split(adminsString, ",")
		} else {
			adminsString, ok := os.LookupEnv("AUTHORIZATION_ADMIN_USERS")
			if ok {
				config.Authorization.Rbac.RootUsers = strings.Split(adminsString, ",")
			}
		}

		groupString, ok := os.LookupEnv("AUTHORIZATION_RBAC_ROOT_GROUPS")
		if ok {
			config.Authorization.Rbac.RootGroups = strings.Split(groupString, ",")
		}

		viewerGroupString, ok := os.LookupEnv("AUTHORIZATION_RBAC_READONLY_GROUPS")
		if ok {
			config.Authorization.Rbac.ViewerGroups = strings.Split(viewerGroupString, ",")
		} else {
			// delete this after 1.30.11 + 1.31.3 is the minimum version in WCD
			viewerGroupString, ok := os.LookupEnv("EXPERIMENTAL_AUTHORIZATION_RBAC_READONLY_ROOT_GROUPS")
			if ok {
				config.Authorization.Rbac.ViewerGroups = strings.Split(viewerGroupString, ",")
			}
		}

		readOnlyUsersString, ok := os.LookupEnv("EXPERIMENTAL_AUTHORIZATION_RBAC_READONLY_USERS")
		if ok {
			config.Authorization.Rbac.ViewerUsers = strings.Split(readOnlyUsersString, ",")
		}

		adminUsersString, ok := os.LookupEnv("EXPERIMENTAL_AUTHORIZATION_RBAC_ADMIN_USERS")
		if ok {
			config.Authorization.Rbac.AdminUsers = strings.Split(adminUsersString, ",")
		}
	}

	config.Profiling.Disabled = entcfg.Enabled(os.Getenv("GO_PROFILING_DISABLE"))

	if !config.Authentication.AnyAuthMethodSelected() {
		config.Authentication = DefaultAuthentication
	}

	if os.Getenv("PERSISTENCE_LSM_ACCESS_STRATEGY") == "pread" {
		config.AvoidMmap = true
	}

	if v := os.Getenv("PERSISTENCE_LSM_MAX_SEGMENT_SIZE"); v != "" {
		parsed, err := parseResourceString(v)
		if err != nil {
			return fmt.Errorf("parse PERSISTENCE_LSM_MAX_SEGMENT_SIZE: %w", err)
		}

		config.Persistence.LSMMaxSegmentSize = parsed
	} else {
		config.Persistence.LSMMaxSegmentSize = DefaultPersistenceLSMMaxSegmentSize
	}

	if err := parseNonNegativeInt(
		"PERSISTENCE_LSM_SEGMENTS_CLEANUP_INTERVAL_HOURS",
		func(hours int) { config.Persistence.LSMSegmentsCleanupIntervalSeconds = hours * 3600 },
		DefaultPersistenceLSMSegmentsCleanupIntervalSeconds,
	); err != nil {
		return err
	}

	if entcfg.Enabled(os.Getenv("PERSISTENCE_LSM_SEPARATE_OBJECTS_COMPACTIONS")) {
		config.Persistence.LSMSeparateObjectsCompactions = true
	}

	if entcfg.Enabled(os.Getenv("PERSISTENCE_LSM_ENABLE_SEGMENTS_CHECKSUM_VALIDATION")) {
		config.Persistence.LSMEnableSegmentsChecksumValidation = true
	}

	if v := os.Getenv("PERSISTENCE_MIN_MMAP_SIZE"); v != "" {
		parsed, err := parseResourceString(v)
		if err != nil {
			return fmt.Errorf("parse PERSISTENCE_MIN_MMAP_SIZE: %w", err)
		}

		config.Persistence.MinMMapSize = parsed
	} else {
		config.Persistence.MinMMapSize = DefaultPersistenceMinMMapSize
	}

	if entcfg.Enabled(os.Getenv("PERSISTENCE_LAZY_SEGMENTS_DISABLED")) {
		config.Persistence.LazySegmentsDisabled = true
	}

	if v := os.Getenv("PERSISTENCE_MAX_REUSE_WAL_SIZE"); v != "" {
		parsed, err := parseResourceString(v)
		if err != nil {
			return fmt.Errorf("parse PERSISTENCE_MAX_REUSE_WAL_SIZE: %w", err)
		}

		config.Persistence.MaxReuseWalSize = parsed
	} else {
		config.Persistence.MaxReuseWalSize = DefaultPersistenceMaxReuseWalSize
	}

	if err := parseInt(
		"PERSISTENCE_LSM_CYCLEMANAGER_ROUTINES_FACTOR",
		func(factor int) { config.Persistence.LSMCycleManagerRoutinesFactor = factor },
		DefaultPersistenceLSMCycleManagerRoutinesFactor,
	); err != nil {
		return err
	}

	if v := os.Getenv("PERSISTENCE_HNSW_MAX_LOG_SIZE"); v != "" {
		parsed, err := parseResourceString(v)
		if err != nil {
			return fmt.Errorf("parse PERSISTENCE_HNSW_MAX_LOG_SIZE: %w", err)
		}

		config.Persistence.HNSWMaxLogSize = parsed
	} else {
		config.Persistence.HNSWMaxLogSize = DefaultPersistenceHNSWMaxLogSize
	}

	// ---- HNSW snapshots ----
	config.Persistence.HNSWDisableSnapshots = DefaultHNSWSnapshotDisabled
	if v := os.Getenv("PERSISTENCE_HNSW_DISABLE_SNAPSHOTS"); v != "" {
		config.Persistence.HNSWDisableSnapshots = entcfg.Enabled(v)
	}

	if err := parseNonNegativeInt(
		"PERSISTENCE_HNSW_SNAPSHOT_INTERVAL_SECONDS",
		func(seconds int) { config.Persistence.HNSWSnapshotIntervalSeconds = seconds },
		DefaultHNSWSnapshotIntervalSeconds,
	); err != nil {
		return err
	}

	config.Persistence.HNSWSnapshotOnStartup = DefaultHNSWSnapshotOnStartup
	if v := os.Getenv("PERSISTENCE_HNSW_SNAPSHOT_ON_STARTUP"); v != "" {
		config.Persistence.HNSWSnapshotOnStartup = entcfg.Enabled(v)
	}

	if err := parsePositiveInt(
		"PERSISTENCE_HNSW_SNAPSHOT_MIN_DELTA_COMMITLOGS_NUMBER",
		func(number int) { config.Persistence.HNSWSnapshotMinDeltaCommitlogsNumber = number },
		DefaultHNSWSnapshotMinDeltaCommitlogsNumber,
	); err != nil {
		return err
	}

	if err := parseNonNegativeInt(
		"PERSISTENCE_HNSW_SNAPSHOT_MIN_DELTA_COMMITLOGS_SIZE_PERCENTAGE",
		func(percentage int) { config.Persistence.HNSWSnapshotMinDeltaCommitlogsSizePercentage = percentage },
		DefaultHNSWSnapshotMinDeltaCommitlogsSizePercentage,
	); err != nil {
		return err
	}
	// ---- HNSW snapshots ----

	if entcfg.Enabled(os.Getenv("INDEX_RANGEABLE_IN_MEMORY")) {
		config.Persistence.IndexRangeableInMemory = true
	}

	if err := parseInt(
		"HNSW_VISITED_LIST_POOL_MAX_SIZE",
		func(size int) { config.HNSWVisitedListPoolMaxSize = size },
		DefaultHNSWVisitedListPoolSize,
	); err != nil {
		return err
	}

	if err := parseNonNegativeInt(
		"HNSW_FLAT_SEARCH_CONCURRENCY",
		func(val int) { config.HNSWFlatSearchConcurrency = val },
		DefaultHNSWFlatSearchConcurrency,
	); err != nil {
		return err
	}

	if err := parsePercentage(
		"HNSW_ACORN_FILTER_RATIO",
		func(val float64) { config.HNSWAcornFilterRatio = val },
		DefaultHNSWAcornFilterRatio,
	); err != nil {
		return err
	}

	clusterCfg, err := parseClusterConfig()
	if err != nil {
		return err
	}
	config.Cluster = clusterCfg

	if v := os.Getenv("PERSISTENCE_DATA_PATH"); v != "" {
		config.Persistence.DataPath = v
	} else {
		if config.Persistence.DataPath == "" {
			config.Persistence.DataPath = DefaultPersistenceDataPath
		}
	}

	parsePositiveFloat("REINDEXER_GOROUTINES_FACTOR",
		func(val float64) { config.ReindexerGoroutinesFactor = val },
		DefaultReindexerGoroutinesFactor)

	if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_AT_STARTUP", clusterCfg.Hostname) {
		config.ReindexMapToBlockmaxAtStartup = true
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_SWAP_BUCKETS", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.SwapBuckets = true
		}
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_UNSWAP_BUCKETS", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.UnswapBuckets = true
		}
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_TIDY_BUCKETS", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.TidyBuckets = true
		}
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_RELOAD_SHARDS", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.ReloadShards = true
		}
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_ROLLBACK", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.Rollback = true
		}
		if enabledForHost("REINDEX_MAP_TO_BLOCKMAX_CONDITIONAL_START", clusterCfg.Hostname) {
			config.ReindexMapToBlockmaxConfig.ConditionalStart = true
		}
		parsePositiveInt("REINDEX_MAP_TO_BLOCKMAX_PROCESSING_DURATION_SECONDS",
			func(val int) { config.ReindexMapToBlockmaxConfig.ProcessingDurationSeconds = val },
			DefaultMapToBlockmaxProcessingDurationSeconds)
		parsePositiveInt("REINDEX_MAP_TO_BLOCKMAX_PAUSE_DURATION_SECONDS",
			func(val int) { config.ReindexMapToBlockmaxConfig.PauseDurationSeconds = val },
			DefaultMapToBlockmaxPauseDurationSeconds)
		parsePositiveInt("REINDEX_MAP_TO_BLOCKMAX_PER_OBJECT_DELAY_MILLISECONDS",
			func(val int) { config.ReindexMapToBlockmaxConfig.PerObjectDelayMilliseconds = val },
			DefaultMapToBlockmaxPerObjectDelayMilliseconds)

		cptSelected, err := cptParser.parse(os.Getenv("REINDEX_MAP_TO_BLOCKMAX_SELECT"))
		if err != nil {
			return err
		}
		config.ReindexMapToBlockmaxConfig.Selected = cptSelected
	}

	if err := config.parseMemtableConfig(); err != nil {
		return err
	}

	if err := config.parseCORSConfig(); err != nil {
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
			return fmt.Errorf("parse QUERY_DEFAULTS_LIMIT as int: %w", err)
		}

		config.QueryDefaults.Limit = int64(asInt)
	} else {
		if config.QueryDefaults.Limit == 0 {
			config.QueryDefaults.Limit = DefaultQueryDefaultsLimit
		}
	}

	if v := os.Getenv("QUERY_MAXIMUM_RESULTS"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse QUERY_MAXIMUM_RESULTS as int: %w", err)
		}

		config.QueryMaximumResults = int64(asInt)
	} else {
		config.QueryMaximumResults = DefaultQueryMaximumResults
	}

	if v := os.Getenv("QUERY_NESTED_CROSS_REFERENCE_LIMIT"); v != "" {
		limit, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse QUERY_NESTED_CROSS_REFERENCE_LIMIT as int: %w", err)
		} else if limit <= 0 {
			limit = math.MaxInt
		}
		config.QueryNestedCrossReferenceLimit = limit
	} else {
		config.QueryNestedCrossReferenceLimit = DefaultQueryNestedCrossReferenceLimit
	}

	if err := parsePositiveInt(
		"QUERY_CROSS_REFERENCE_DEPTH_LIMIT",
		func(val int) { config.QueryCrossReferenceDepthLimit = val },
		DefaultQueryCrossReferenceDepthLimit,
	); err != nil {
		return err
	}

	if v := os.Getenv("MAX_IMPORT_GOROUTINES_FACTOR"); v != "" {
		asFloat, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("parse MAX_IMPORT_GOROUTINES_FACTOR as float: %w", err)
		} else if asFloat <= 0 {
			return fmt.Errorf("negative MAX_IMPORT_GOROUTINES_FACTOR factor")
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

	if v := os.Getenv("MODULES_CLIENT_TIMEOUT"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse MODULES_CLIENT_TIMEOUT as time.Duration: %w", err)
		}
		config.ModuleHttpClientTimeout = timeout
	} else {
		config.ModuleHttpClientTimeout = 50 * time.Second
	}

	if v := os.Getenv("DEFAULT_VECTOR_DISTANCE_METRIC"); v != "" {
		config.DefaultVectorDistanceMetric = v
	}

	if v := os.Getenv("ENABLE_MODULES"); v != "" {
		config.EnableModules = v
	}

	if entcfg.Enabled(os.Getenv("ENABLE_API_BASED_MODULES")) {
		config.EnableApiBasedModules = true
	}

	autoSchemaEnabled := true
	if v := os.Getenv("AUTOSCHEMA_ENABLED"); v != "" {
		autoSchemaEnabled = !(strings.ToLower(v) == "false")
	}
	config.AutoSchema.Enabled = runtime.NewDynamicValue(autoSchemaEnabled)

	config.AutoSchema.DefaultString = schema.DataTypeText.String()
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

	tenantActivityReadLogLevel := "debug"
	if v := os.Getenv("TENANT_ACTIVITY_READ_LOG_LEVEL"); v != "" {
		tenantActivityReadLogLevel = v
	}
	config.TenantActivityReadLogLevel = runtime.NewDynamicValue(tenantActivityReadLogLevel)

	tenantActivityWriteLogLevel := "debug"
	if v := os.Getenv("TENANT_ACTIVITY_WRITE_LOG_LEVEL"); v != "" {
		tenantActivityWriteLogLevel = v
	}
	config.TenantActivityWriteLogLevel = runtime.NewDynamicValue(tenantActivityWriteLogLevel)

	ru, err := parseResourceUsageEnvVars()
	if err != nil {
		return err
	}
	config.ResourceUsage = ru

	if v := os.Getenv("GO_BLOCK_PROFILE_RATE"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse GO_BLOCK_PROFILE_RATE as int: %w", err)
		}

		config.Profiling.BlockProfileRate = asInt
	}

	if v := os.Getenv("GO_MUTEX_PROFILE_FRACTION"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse GO_MUTEX_PROFILE_FRACTION as int: %w", err)
		}

		config.Profiling.MutexProfileFraction = asInt
	}

	if v := os.Getenv("MAXIMUM_CONCURRENT_GET_REQUESTS"); v != "" {
		asInt, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse MAXIMUM_CONCURRENT_GET_REQUESTS as int: %w", err)
		}
		config.MaximumConcurrentGetRequests = int(asInt)
	} else {
		config.MaximumConcurrentGetRequests = DefaultMaxConcurrentGetRequests
	}

	if err = parsePositiveInt(
		"MAXIMUM_CONCURRENT_SHARD_LOADS",
		func(val int) { config.MaximumConcurrentShardLoads = val },
		DefaultMaxConcurrentShardLoads,
	); err != nil {
		return err
	}

	if err := parsePositiveInt(
		"GRPC_MAX_MESSAGE_SIZE",
		func(val int) { config.GRPC.MaxMsgSize = val },
		DefaultGRPCMaxMsgSize,
	); err != nil {
		return err
	}
	if err := parsePositiveInt(
		"GRPC_PORT",
		func(val int) { config.GRPC.Port = val },
		DefaultGRPCPort,
	); err != nil {
		return err
	}
	config.GRPC.CertFile = ""
	if v := os.Getenv("GRPC_CERT_FILE"); v != "" {
		config.GRPC.CertFile = v
	}
	config.GRPC.KeyFile = ""
	if v := os.Getenv("GRPC_KEY_FILE"); v != "" {
		config.GRPC.KeyFile = v
	}

	config.DisableGraphQL = entcfg.Enabled(os.Getenv("DISABLE_GRAPHQL"))

	if config.Raft, err = parseRAFTConfig(config.Cluster.Hostname); err != nil {
		return fmt.Errorf("parse raft config: %w", err)
	}

	if err := parsePositiveInt(
		"REPLICATION_MINIMUM_FACTOR",
		func(val int) { config.Replication.MinimumFactor = val },
		DefaultMinimumReplicationFactor,
	); err != nil {
		return err
	}

	config.Replication.AsyncReplicationDisabled = runtime.NewDynamicValue(entcfg.Enabled(os.Getenv("ASYNC_REPLICATION_DISABLED")))

	if v := os.Getenv("REPLICATION_FORCE_DELETION_STRATEGY"); v != "" {
		config.Replication.DeletionStrategy = v
	}

	config.DisableTelemetry = false
	if entcfg.Enabled(os.Getenv("DISABLE_TELEMETRY")) {
		config.DisableTelemetry = true
	}

	if entcfg.Enabled(os.Getenv("HNSW_STARTUP_WAIT_FOR_VECTOR_CACHE")) {
		config.HNSWStartupWaitForVectorCache = true
	}

	if err := parseInt(
		"MAXIMUM_ALLOWED_COLLECTIONS_COUNT",
		func(val int) {
			config.SchemaHandlerConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(val)
		},
		DefaultMaximumAllowedCollectionsCount,
	); err != nil {
		return err
	}

	// explicitly reset sentry config
	sentry.Config = nil
	config.Sentry, err = sentry.InitSentryConfig()
	if err != nil {
		return fmt.Errorf("parse sentry config from env: %w", err)
	}

	config.MetadataServer.Enabled = false
	if entcfg.Enabled(os.Getenv("EXPERIMENTAL_METADATA_SERVER_ENABLED")) {
		config.MetadataServer.Enabled = true
	}
	config.MetadataServer.GrpcListenAddress = DefaultMetadataServerGrpcListenAddress
	if v := os.Getenv("EXPERIMENTAL_METADATA_SERVER_GRPC_LISTEN_ADDRESS"); v != "" {
		config.MetadataServer.GrpcListenAddress = v
	}
	if err := parsePositiveInt(
		"EXPERIMENTAL_METADATA_SERVER_DATA_EVENTS_CHANNEL_CAPACITY",
		func(val int) { config.MetadataServer.DataEventsChannelCapacity = val },
		DefaultMetadataServerDataEventsChannelCapacity,
	); err != nil {
		return err
	}

	config.RuntimeOverrides.Enabled = entcfg.Enabled(os.Getenv("RUNTIME_OVERRIDES_ENABLED"))

	if v := os.Getenv("RUNTIME_OVERRIDES_PATH"); v != "" {
		config.RuntimeOverrides.Path = v
	}

	config.RuntimeOverrides.LoadInterval = DefaultRuntimeOverridesLoadInterval
	if v := os.Getenv("RUNTIME_OVERRIDES_LOAD_INTERVAL"); v != "" {
		interval, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse RUNTIME_OVERRIDES_LOAD_INTERVAL as time.Duration: %w", err)
		}
		config.RuntimeOverrides.LoadInterval = interval
	}

	if err = parsePositiveInt(
		"DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS",
		func(val int) { config.DistributedTasks.SchedulerTickInterval = time.Duration(val) * time.Second },
		int(DefaultDistributedTasksSchedulerTickInterval.Seconds()),
	); err != nil {
		return err
	}

	if err = parsePositiveInt(
		"DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		func(val int) { config.DistributedTasks.CompletedTaskTTL = time.Duration(val) * time.Hour },
		int(DefaultDistributedTasksCompletedTaskTTL.Hours()),
	); err != nil {
		return err
	}

	if v := os.Getenv("DISTRIBUTED_TASKS_ENABLED"); v != "" {
		config.DistributedTasks.Enabled = entcfg.Enabled(v)
	}

	if v := os.Getenv("REPLICA_MOVEMENT_ENABLED"); v != "" {
		config.ReplicaMovementEnabled = entcfg.Enabled(v)
	}

	if v := os.Getenv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT"); v != "" {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT as time.Duration: %w", err)
		}
		if duration < 0 {
			return fmt.Errorf("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT must be a positive duration")
		}
		config.ReplicaMovementMinimumAsyncWait = runtime.NewDynamicValue(duration)
	} else {
		config.ReplicaMovementMinimumAsyncWait = runtime.NewDynamicValue(DefaultReplicaMovementMinimumAsyncWait)
	}
	revoctorizeCheckDisabled := false
	if v := os.Getenv("REVECTORIZE_CHECK_DISABLED"); v != "" {
		revoctorizeCheckDisabled = !(strings.ToLower(v) == "false")
	}
	config.RevectorizeCheckDisabled = runtime.NewDynamicValue(revoctorizeCheckDisabled)

	querySlowLogEnabled := entcfg.Enabled(os.Getenv("QUERY_SLOW_LOG_ENABLED"))
	config.QuerySlowLogEnabled = runtime.NewDynamicValue(querySlowLogEnabled)

	querySlowLogThreshold := dbhelpers.DefaultSlowLogThreshold
	if v := os.Getenv("QUERY_SLOW_LOG_THRESHOLD"); v != "" {
		threshold, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse QUERY_SLOW_LOG_THRESHOLD as time.Duration: %w", err)
		}
		querySlowLogThreshold = threshold
	}
	config.QuerySlowLogThreshold = runtime.NewDynamicValue(querySlowLogThreshold)

	invertedSorterDisabled := false
	if v := os.Getenv("INVERTED_SORTER_DISABLED"); v != "" {
		invertedSorterDisabled = !(strings.ToLower(v) == "false")
	}
	config.InvertedSorterDisabled = runtime.NewDynamicValue(invertedSorterDisabled)

	return nil
}

func parseRAFTConfig(hostname string) (Raft, error) {
	// flag.IntVar()
	cfg := Raft{
		MetadataOnlyVoters: entcfg.Enabled(os.Getenv("RAFT_METADATA_ONLY_VOTERS")),
	}

	if err := parsePositiveInt(
		"RAFT_PORT",
		func(val int) { cfg.Port = val },
		DefaultRaftPort,
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_INTERNAL_RPC_PORT",
		func(val int) { cfg.InternalRPCPort = val },
		DefaultRaftInternalPort,
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_GRPC_MESSAGE_MAX_SIZE",
		func(val int) { cfg.RPCMessageMaxSize = val },
		DefaultRaftGRPCMaxSize,
	); err != nil {
		return cfg, err
	}

	parseStringList(
		"RAFT_JOIN",
		func(val []string) { cfg.Join = val },
		// Default RAFT_JOIN must be the configured node name and the configured raft port. This allows us to have a one-node raft cluster
		// able to bootstrap itself if the user doesn't pass any raft parameter.
		[]string{fmt.Sprintf("%s:%d", hostname, cfg.InternalRPCPort)},
	)
	if err := parsePositiveInt(
		"RAFT_BOOTSTRAP_TIMEOUT",
		func(val int) { cfg.BootstrapTimeout = time.Second * time.Duration(val) },
		DefaultRaftBootstrapTimeout,
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_BOOTSTRAP_EXPECT",
		func(val int) { cfg.BootstrapExpect = val },
		DefaultRaftBootstrapExpect,
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_HEARTBEAT_TIMEOUT",
		func(val int) { cfg.HeartbeatTimeout = time.Second * time.Duration(val) },
		1, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_ELECTION_TIMEOUT",
		func(val int) { cfg.ElectionTimeout = time.Second * time.Duration(val) },
		1, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveFloat(
		"RAFT_LEADER_LEASE_TIMEOUT",
		func(val float64) { cfg.LeaderLeaseTimeout = time.Second * time.Duration(val) },
		0.5, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_TIMEOUTS_MULTIPLIER",
		func(val int) { cfg.TimeoutsMultiplier = val },
		1, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_SNAPSHOT_INTERVAL",
		func(val int) { cfg.SnapshotInterval = time.Second * time.Duration(val) },
		120, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_SNAPSHOT_THRESHOLD",
		func(val int) { cfg.SnapshotThreshold = uint64(val) },
		8192, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_TRAILING_LOGS",
		func(val int) { cfg.TrailingLogs = uint64(val) },
		10240, // raft default
	); err != nil {
		return cfg, err
	}

	if err := parsePositiveInt(
		"RAFT_CONSISTENCY_WAIT_TIMEOUT",
		func(val int) { cfg.ConsistencyWaitTimeout = time.Second * time.Duration(val) },
		10,
	); err != nil {
		return cfg, err
	}

	cfg.EnableOneNodeRecovery = entcfg.Enabled(os.Getenv("RAFT_ENABLE_ONE_NODE_RECOVERY"))
	cfg.ForceOneNodeRecovery = entcfg.Enabled(os.Getenv("RAFT_FORCE_ONE_NODE_RECOVERY"))

	return cfg, nil
}

func (c *Config) parseCORSConfig() error {
	if v := os.Getenv("CORS_ALLOW_ORIGIN"); v != "" {
		c.CORS.AllowOrigin = v
	} else {
		c.CORS.AllowOrigin = DefaultCORSAllowOrigin
	}

	if v := os.Getenv("CORS_ALLOW_METHODS"); v != "" {
		c.CORS.AllowMethods = v
	} else {
		c.CORS.AllowMethods = DefaultCORSAllowMethods
	}

	if v := os.Getenv("CORS_ALLOW_HEADERS"); v != "" {
		c.CORS.AllowHeaders = v
	} else {
		c.CORS.AllowHeaders = DefaultCORSAllowHeaders
	}

	return nil
}

func (c *Config) parseMemtableConfig() error {
	// first parse old idle name for flush value
	if err := parsePositiveInt(
		"PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER",
		func(val int) { c.Persistence.MemtablesFlushDirtyAfter = val },
		DefaultPersistenceMemtablesFlushDirtyAfter,
	); err != nil {
		return err
	}
	// then parse with new idle name and use previous value in case it's not set
	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS",
		func(val int) { c.Persistence.MemtablesFlushDirtyAfter = val },
		c.Persistence.MemtablesFlushDirtyAfter,
	); err != nil {
		return err
	}
	// then parse with dirty name and use idle value as fallback
	if err := parsePositiveInt(
		"PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS",
		func(val int) { c.Persistence.MemtablesFlushDirtyAfter = val },
		c.Persistence.MemtablesFlushDirtyAfter,
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

	if err := parsePositiveInt(
		"REPLICATION_ENGINE_MAX_WORKERS",
		func(val int) { c.ReplicationEngineMaxWorkers = val },
		DefaultReplicationEngineMaxWorkers,
	); err != nil {
		return err
	}

	return nil
}

func parsePercentage(envName string, cb func(val float64), defaultValue float64) error {
	return parseFloat64(envName, defaultValue, func(val float64) error {
		if val < 0 || val > 1 {
			return fmt.Errorf("%s must be between 0 and 1", envName)
		}
		return nil
	}, cb)
}

func parseFloat64(envName string, defaultValue float64, verify func(val float64) error, cb func(val float64)) error {
	var err error
	asFloat := defaultValue

	if v := os.Getenv(envName); v != "" {
		asFloat, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("parse %s as float64: %w", envName, err)
		}
		if err = verify(asFloat); err != nil {
			return err
		}
	}

	cb(asFloat)
	return nil
}

func parseInt(envName string, cb func(val int), defaultValue int) error {
	return parseIntVerify(envName, defaultValue, cb, func(val int, envName string) error { return nil })
}

func parsePositiveInt(envName string, cb func(val int), defaultValue int) error {
	return parseIntVerify(envName, defaultValue, cb, func(val int, envName string) error {
		if val <= 0 {
			return fmt.Errorf("%s must be an integer greater than 0. Got: %v", envName, val)
		}
		return nil
	})
}

func parseNonNegativeInt(envName string, cb func(val int), defaultValue int) error {
	return parseIntVerify(envName, defaultValue, cb, func(val int, envName string) error {
		if val < 0 {
			return fmt.Errorf("%s must be an integer greater than or equal 0. Got %v", envName, val)
		}
		return nil
	})
}

func parseIntVerify(envName string, defaultValue int, cb func(val int), verify func(val int, envName string) error) error {
	var err error
	asInt := defaultValue

	if v := os.Getenv(envName); v != "" {
		asInt, err = strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %s as int: %w", envName, err)
		}
		if err = verify(asInt, envName); err != nil {
			return err
		}
	}

	cb(asInt)
	return nil
}

// func parseFloat(envName string, cb func(val float64), defaultValue float64) error {
// 	return parseFloatVerify(envName, defaultValue, cb, func(val float64) error { return nil })
// }

func parsePositiveFloat(envName string, cb func(val float64), defaultValue float64) error {
	return parseFloatVerify(envName, defaultValue, cb, func(val float64) error {
		if val <= 0 {
			return fmt.Errorf("%s must be a float greater than 0. Got: %v", envName, val)
		}
		return nil
	})
}

// func parseNonNegativeFloat(envName string, cb func(val float64), defaultValue float64) error {
// 	return parseFloatVerify(envName, defaultValue, cb, func(val float64) error {
// 		if val < 0 {
// 			return fmt.Errorf("%s must be a float greater than or equal 0. Got %v", envName, val)
// 		}
// 		return nil
// 	})
// }

func parseFloatVerify(envName string, defaultValue float64, cb func(val float64), verify func(val float64) error) error {
	var err error
	asFloat := defaultValue

	if v := os.Getenv(envName); v != "" {
		asFloat, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("parse %s as float: %w", envName, err)
		}
		if err = verify(asFloat); err != nil {
			return err
		}
	}

	cb(asFloat)
	return nil
}

const (
	DefaultQueryMaximumResults = int64(10000)
	// DefaultQueryNestedCrossReferenceLimit describes the max number of nested crossrefs returned for a query
	DefaultQueryNestedCrossReferenceLimit = int64(100000)
	// DefaultQueryCrossReferenceDepthLimit describes the max depth of nested crossrefs in a query
	DefaultQueryCrossReferenceDepthLimit = 5
)

const (
	DefaultPersistenceMemtablesFlushDirtyAfter = 60
	DefaultPersistenceMemtablesMaxSize         = 200
	DefaultPersistenceMemtablesMinDuration     = 15
	DefaultPersistenceMemtablesMaxDuration     = 45
	DefaultMaxConcurrentGetRequests            = 0
	DefaultMaxConcurrentShardLoads             = 500
	DefaultGRPCPort                            = 50051
	DefaultGRPCMaxMsgSize                      = 104858000 // 100 * 1024 * 1024 + 400
	DefaultMinimumReplicationFactor            = 1
	DefaultMaximumAllowedCollectionsCount      = -1 // unlimited
)

const VectorizerModuleNone = "none"

// DefaultGossipBindPort uses the hashicorp/memberlist default
// port value assigned with the use of DefaultLocalConfig
const DefaultGossipBindPort = 7946

// TODO: This should be retrieved dynamically from all installed modules
const VectorizerModuleText2VecContextionary = "text2vec-contextionary"

func parseStringList(varName string, cb func(val []string), defaultValue []string) {
	if v := os.Getenv(varName); v != "" {
		cb(strings.Split(v, ","))
	} else {
		cb(defaultValue)
	}
}

func parseResourceUsageEnvVars() (ResourceUsage, error) {
	ru := ResourceUsage{}

	if v := os.Getenv("DISK_USE_WARNING_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, fmt.Errorf("parse DISK_USE_WARNING_PERCENTAGE as uint: %w", err)
		}
		ru.DiskUse.WarningPercentage = asUint
	} else {
		ru.DiskUse.WarningPercentage = DefaultDiskUseWarningPercentage
	}

	if v := os.Getenv("DISK_USE_READONLY_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, fmt.Errorf("parse DISK_USE_READONLY_PERCENTAGE as uint: %w", err)
		}
		ru.DiskUse.ReadOnlyPercentage = asUint
	} else {
		ru.DiskUse.ReadOnlyPercentage = DefaultDiskUseReadonlyPercentage
	}

	if v := os.Getenv("MEMORY_WARNING_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, fmt.Errorf("parse MEMORY_WARNING_PERCENTAGE as uint: %w", err)
		}
		ru.MemUse.WarningPercentage = asUint
	} else {
		ru.MemUse.WarningPercentage = DefaultMemUseWarningPercentage
	}

	if v := os.Getenv("MEMORY_READONLY_PERCENTAGE"); v != "" {
		asUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return ru, fmt.Errorf("parse MEMORY_READONLY_PERCENTAGE as uint: %w", err)
		}
		ru.MemUse.ReadOnlyPercentage = asUint
	} else {
		ru.MemUse.ReadOnlyPercentage = DefaultMemUseReadonlyPercentage
	}

	return ru, nil
}

func parseClusterConfig() (cluster.Config, error) {
	cfg := cluster.Config{}

	// by default memberlist assigns hostname to os.Hostname() incase hostname is empty
	// ref: https://github.com/hashicorp/memberlist/blob/3f82dc10a89f82efe300228752f7077d0d9f87e4/config.go#L303
	// it's handled at parseClusterConfig step to be consistent from the config start point and conveyed to all
	// underlying functions see parseRAFTConfig(..) for example
	cfg.Hostname = os.Getenv("CLUSTER_HOSTNAME")
	if cfg.Hostname == "" {
		cfg.Hostname, _ = os.Hostname()
	}
	cfg.Join = os.Getenv("CLUSTER_JOIN")

	advertiseAddr, advertiseAddrSet := os.LookupEnv("CLUSTER_ADVERTISE_ADDR")
	advertisePort, advertisePortSet := os.LookupEnv("CLUSTER_ADVERTISE_PORT")

	cfg.Localhost = entcfg.Enabled(os.Getenv("CLUSTER_IN_LOCALHOST"))
	gossipBind, gossipBindSet := os.LookupEnv("CLUSTER_GOSSIP_BIND_PORT")
	dataBind, dataBindSet := os.LookupEnv("CLUSTER_DATA_BIND_PORT")

	if advertiseAddrSet {
		cfg.AdvertiseAddr = advertiseAddr
	}

	if advertisePortSet {
		asInt, err := strconv.Atoi(advertisePort)
		if err != nil {
			return cfg, fmt.Errorf("parse CLUSTER_ADVERTISE_PORT as int: %w", err)
		}
		cfg.AdvertisePort = asInt
	}

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

	cfg.IgnoreStartupSchemaSync = entcfg.Enabled(
		os.Getenv("CLUSTER_IGNORE_SCHEMA_SYNC"))
	cfg.SkipSchemaSyncRepair = entcfg.Enabled(
		os.Getenv("CLUSTER_SKIP_SCHEMA_REPAIR"))

	basicAuthUsername := os.Getenv("CLUSTER_BASIC_AUTH_USERNAME")
	basicAuthPassword := os.Getenv("CLUSTER_BASIC_AUTH_PASSWORD")

	cfg.AuthConfig = cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{
			Username: basicAuthUsername,
			Password: basicAuthPassword,
		},
	}

	cfg.FastFailureDetection = entcfg.Enabled(os.Getenv("FAST_FAILURE_DETECTION"))

	// MAINTENANCE_NODES is experimental and subject to removal/change. It is an optional, comma
	// separated list of hostnames that are in maintenance mode. In maintenance mode, the node will
	// return an error for all data requests, but will still participate in the raft cluster and
	// schema operations. This can be helpful is a node is too overwhelmed by startup tasks to handle
	// data requests and you need to start up the node to give it time to "catch up". Note that in
	// general one should not use the MaintenanceNodes field directly, but since we don't have
	// access to the State here and the cluster has not yet initialized, we have to set it here.

	// avoid the case where strings.Split creates a slice with only the empty string as I think
	// that will be confusing for future code. eg ([]string{""}) instead of an empty slice ([]string{}).
	// https://go.dev/play/p/3BDp1vhbkYV shows len(1) when m = "".
	cfg.MaintenanceNodes = []string{}
	if m := os.Getenv("MAINTENANCE_NODES"); m != "" {
		for _, node := range strings.Split(m, ",") {
			if node != "" {
				cfg.MaintenanceNodes = append(cfg.MaintenanceNodes, node)
			}
		}
	}

	return cfg, nil
}

func enabledForHost(envName string, localHostname string) bool {
	if v := os.Getenv(envName); v != "" {
		if entcfg.Enabled(v) {
			return true
		}
		return slices.Contains(strings.Split(v, ","), localHostname)
	}
	return false
}

/*
parses variable of format "colName1:propNames1:tenantNames1;colName2:propNames2:tenantNames2"
propNames = prop1,prop2,...
tenantNames = tenant1,tenant2,...

examples:
  - collection:
    "ColName1"
    "ColName1;ColName2"
  - collection + properties:
    "ColName1:propName1"
    "ColName1:propName1,propName2;ColName2:propName3"
  - collection + properties + tenants/shards:
    "ColName1:propName1:tenantName1,tenantName2"
    "ColName1:propName1:tenantName1,tenantName2;ColName2:propName2,propName3:tenantName3"
  - collection + tenants/shards:
    "ColName1::tenantName1"
    "ColName1::tenantName1,tenantName2;ColName2::tenantName3"
*/
type collectionPropsTenantsParser struct {
	regexpCollection *regexp.Regexp
	regexpProp       *regexp.Regexp
	regexpTenant     *regexp.Regexp
}

func newCollectionPropsTenantsParser() *collectionPropsTenantsParser {
	return &collectionPropsTenantsParser{
		regexpCollection: regexp.MustCompile(`^` + schema.ClassNameRegexCore + `$`),
		regexpProp:       regexp.MustCompile(`^` + schema.PropertyNameRegex + `$`),
		regexpTenant:     regexp.MustCompile(`^` + schema.ShardNameRegexCore + `$`),
	}
}

func (p *collectionPropsTenantsParser) parse(v string) ([]CollectionPropsTenants, error) {
	if v = strings.TrimSpace(v); v == "" {
		return []CollectionPropsTenants{}, nil
	}

	split := strings.Split(v, ";")
	count := len(split)
	cpts := make([]CollectionPropsTenants, 0, count)
	uniqMapIdx := make(map[string]int, count)

	ec := errorcompounder.New()
	for _, single := range split {
		if single = strings.TrimSpace(single); single != "" {
			if cpt, err := p.parseSingle(single); err != nil {
				ec.Add(fmt.Errorf("parse '%s': %w", single, err))
			} else {
				if prevIdx, ok := uniqMapIdx[cpt.Collection]; ok {
					cpts[prevIdx] = p.mergeCpt(cpts[prevIdx], cpt)
				} else {
					uniqMapIdx[cpt.Collection] = len(cpts)
					cpts = append(cpts, cpt)
				}
			}
		}
	}

	return cpts, ec.ToError()
}

func (p *collectionPropsTenantsParser) parseSingle(single string) (CollectionPropsTenants, error) {
	split := strings.Split(single, ":")
	empty := CollectionPropsTenants{}

	switch count := len(split); count {
	case 1:
		collection, err := p.parseCollection(split[0])
		if err != nil {
			return empty, err
		}
		return CollectionPropsTenants{Collection: collection}, nil

	case 2:
		collection, err := p.parseCollection(split[0])
		if err != nil {
			return empty, err
		}
		props, err := p.parseProps(split[1])
		if err != nil {
			return empty, err
		}
		return CollectionPropsTenants{Collection: collection, Props: props}, nil

	case 3:
		collection, err := p.parseCollection(split[0])
		if err != nil {
			return empty, err
		}
		props, err := p.parseProps(split[1])
		if err != nil {
			return empty, err
		}
		tenants, err := p.parseTenants(split[2])
		if err != nil {
			return empty, err
		}
		return CollectionPropsTenants{Collection: collection, Props: props, Tenants: tenants}, nil

	default:
		return empty, fmt.Errorf("too many parts in '%s'. Expected 1-3, got %d", single, count)
	}
}

func (p *collectionPropsTenantsParser) parseCollection(collection string) (string, error) {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return "", fmt.Errorf("missing collection name")
	}
	if !p.regexpCollection.MatchString(collection) {
		return "", fmt.Errorf("invalid collection name '%s'. Does not match regexp", collection)
	}
	return collection, nil
}

func (p *collectionPropsTenantsParser) parseProps(propsStr string) ([]string, error) {
	return p.parseElems(propsStr, p.regexpProp, "invalid property name '%s'. Does not match regexp")
}

func (p *collectionPropsTenantsParser) parseTenants(tenantsStr string) ([]string, error) {
	return p.parseElems(tenantsStr, p.regexpTenant, "invalid tenant/shard name '%s'. Does not match regexp")
}

func (p *collectionPropsTenantsParser) parseElems(str string, reg *regexp.Regexp, errMsg string) ([]string, error) {
	split := strings.Split(str, ",")
	count := len(split)
	elems := make([]string, 0, count)
	uniqMap := make(map[string]struct{}, count)

	ec := errorcompounder.New()
	for _, elem := range split {
		if elem = strings.TrimSpace(elem); elem != "" {
			if reg.MatchString(elem) {
				if _, ok := uniqMap[elem]; !ok {
					elems = append(elems, elem)
					uniqMap[elem] = struct{}{}
				}
			} else {
				ec.Add(fmt.Errorf(errMsg, elem))
			}
		}
	}

	if len(elems) == 0 {
		return nil, ec.ToError()
	}
	return elems, ec.ToError()
}

func (p *collectionPropsTenantsParser) mergeCpt(cptDst, cptSrc CollectionPropsTenants) CollectionPropsTenants {
	if cptDst.Collection != cptSrc.Collection {
		return cptDst
	}
	cptDst.Props = p.mergeUniqueElems(cptDst.Props, cptSrc.Props)
	cptDst.Tenants = p.mergeUniqueElems(cptDst.Tenants, cptSrc.Tenants)
	return cptDst
}

func (p *collectionPropsTenantsParser) mergeUniqueElems(uniqueA, uniqueB []string) []string {
	lA, lB := len(uniqueA), len(uniqueB)
	if lB == 0 {
		return uniqueA
	}
	if lA == 0 {
		return uniqueB
	}

	uniqMapA := make(map[string]struct{}, lA)
	for _, a := range uniqueA {
		uniqMapA[a] = struct{}{}
	}
	for _, b := range uniqueB {
		if _, ok := uniqMapA[b]; !ok {
			uniqueA = append(uniqueA, b)
		}
	}
	return uniqueA
}
