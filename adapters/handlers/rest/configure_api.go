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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	goruntime "runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/fgprof"

	"github.com/KimMachineGun/automemlimit/memlimit"
	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/pbnjay/memory"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/handlers/rest/tenantactivity"
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	modulestorage "github.com/weaviate/weaviate/adapters/repos/modules"
	schemarepo "github.com/weaviate/weaviate/adapters/repos/schema"
	rCluster "github.com/weaviate/weaviate/cluster"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/replication"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativedummy "github.com/weaviate/weaviate/modules/generative-dummy"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativemistral "github.com/weaviate/weaviate/modules/generative-mistral"
	modgenerativeoctoai "github.com/weaviate/weaviate/modules/generative-octoai"
	modgenerativeollama "github.com/weaviate/weaviate/modules/generative-ollama"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modimage "github.com/weaviate/weaviate/modules/img2vec-neural"
	modbind "github.com/weaviate/weaviate/modules/multi2vec-bind"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecgoogle "github.com/weaviate/weaviate/modules/multi2vec-google"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modner "github.com/weaviate/weaviate/modules/ner-transformers"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modqna "github.com/weaviate/weaviate/modules/qna-transformers"
	modcentroid "github.com/weaviate/weaviate/modules/ref2vec-centroid"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankerdummy "github.com/weaviate/weaviate/modules/reranker-dummy"
	modrerankertransformers "github.com/weaviate/weaviate/modules/reranker-transformers"
	modrerankervoyageai "github.com/weaviate/weaviate/modules/reranker-voyageai"
	modsum "github.com/weaviate/weaviate/modules/sum-transformers"
	modspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modcontextionary "github.com/weaviate/weaviate/modules/text2vec-contextionary"
	modtext2vecgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modgpt4all "github.com/weaviate/weaviate/modules/text2vec-gpt4all"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modmistral "github.com/weaviate/weaviate/modules/text2vec-mistral"
	modtext2vecoctoai "github.com/weaviate/weaviate/modules/text2vec-octoai"
	modollama "github.com/weaviate/weaviate/modules/text2vec-ollama"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/telemetry"
	"github.com/weaviate/weaviate/usecases/traverser"

	"github.com/getsentry/sentry-go"
)

const MinimumRequiredContextionaryVersion = "1.0.2"

func makeConfigureServer(appState *state.State) func(*http.Server, string, string) {
	return func(s *http.Server, scheme, addr string) {
		// Add properties to the config
		appState.ServerConfig.Hostname = addr
		appState.ServerConfig.Scheme = scheme
	}
}

type vectorRepo interface {
	objects.BatchVectorRepo
	traverser.VectorSearcher
	classification.VectorRepo
	scaler.BackUpper
	SetSchemaGetter(schemaUC.SchemaGetter)
	WaitForStartup(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

func getCores() (int, error) {
	cpuset, err := os.ReadFile("/sys/fs/cgroup/cpuset/cpuset.cpus")
	if err != nil {
		return 0, errors.Wrap(err, "read cpuset")
	}
	return calcCPUs(strings.TrimSpace(string(cpuset)))
}

func calcCPUs(cpuString string) (int, error) {
	cores := 0
	if cpuString == "" {
		return 0, nil
	}

	// Split by comma to handle multiple ranges
	ranges := strings.Split(cpuString, ",")
	for _, r := range ranges {
		// Check if it's a range (contains a hyphen)
		if strings.Contains(r, "-") {
			parts := strings.Split(r, "-")
			if len(parts) != 2 {
				return 0, fmt.Errorf("invalid CPU range format: %s", r)
			}
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, fmt.Errorf("invalid start of CPU range: %s", parts[0])
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, fmt.Errorf("invalid end of CPU range: %s", parts[1])
			}
			cores += end - start + 1
		} else {
			// Single CPU
			cores++
		}
	}

	return cores, nil
}

func MakeAppState(ctx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	appState := startupRoutine(ctx, options)

	if appState.ServerConfig.Config.Monitoring.Enabled {
		appState.TenantActivity = tenantactivity.NewHandler()

		// only monitoring tool supported at the moment is prometheus
		enterrors.GoWrapper(func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			mux.Handle("/tenant-activity", appState.TenantActivity)
			http.ListenAndServe(fmt.Sprintf(":%d", appState.ServerConfig.Config.Monitoring.Port), mux)
		}, appState.Logger)
	}

	if appState.ServerConfig.Config.Sentry.Enabled {
		err := sentry.Init(sentry.ClientOptions{
			// Setup related config
			Dsn:         appState.ServerConfig.Config.Sentry.DSN,
			Debug:       appState.ServerConfig.Config.Sentry.Debug,
			Release:     "weaviate-core@" + config.ImageTag,
			Environment: appState.ServerConfig.Config.Sentry.Environment,
			// Enable tracing if requested
			EnableTracing:    !appState.ServerConfig.Config.Sentry.TracingDisabled,
			AttachStacktrace: true,
			// Sample rates based on the config
			SampleRate:         appState.ServerConfig.Config.Sentry.ErrorSampleRate,
			ProfilesSampleRate: appState.ServerConfig.Config.Sentry.ProfileSampleRate,
			TracesSampler: sentry.TracesSampler(func(ctx sentry.SamplingContext) float64 {
				// Inherit decision from parent transaction (if any) if it is sampled or not
				if ctx.Parent != nil && ctx.Parent.Sampled != sentry.SampledUndefined {
					return 1.0
				}

				// Filter out uneeded traces
				switch ctx.Span.Name {
				// We are not interested in traces related to metrics endpoint
				case "GET /metrics":
				// These are some usual internet bot that will spam the server. Won't catch them all but we can reduce
				// the number a bit
				case "GET /favicon.ico":
				case "GET /t4":
				case "GET /ab2g":
				case "PRI *":
				case "GET /api/sonicos/tfa":
				case "GET /RDWeb/Pages/en-US/login.aspx":
				case "GET /_profiler/phpinfo":
				case "POST /wsman":
				case "POST /dns-query":
				case "GET /dns-query":
					return 0.0
				}

				// Filter out graphql queries, currently we have no context intrumentation around it and it's therefore
				// just a blank line with 0 info except graphql resolve -> do -> return.
				if ctx.Span.Name == "POST /v1/graphql" {
					return 0.0
				}

				// Return the configured sample rate otherwise
				return appState.ServerConfig.Config.Sentry.TracesSampleRate
			}),
		})
		if err != nil {
			appState.Logger.
				WithField("action", "startup").WithError(err).
				Fatal("sentry initialization failed")
		}

		sentry.ConfigureScope(func(scope *sentry.Scope) {
			// Set cluster ID and cluster owner using sentry user feature to distinguish multiple clusters in the UI
			scope.SetUser(sentry.User{
				ID:       appState.ServerConfig.Config.Sentry.ClusterId,
				Username: appState.ServerConfig.Config.Sentry.ClusterOwner,
			})
			// Set any tags defined
			for key, value := range appState.ServerConfig.Config.Sentry.Tags {
				scope.SetTag(key, value)
			}
		})
	}

	limitResources(appState)

	err := registerModules(appState)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't load")
	}

	// now that modules are loaded we can run the remaining config validation
	// which is module dependent
	if err := appState.ServerConfig.Config.Validate(appState.Modules); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid config")
	}

	appState.ClusterHttpClient = reasonableHttpClient(appState.ServerConfig.Config.Cluster.AuthConfig)
	appState.MemWatch = memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, 0.97)

	var vectorRepo vectorRepo
	// var vectorMigrator schema.Migrator
	// var migrator schema.Migrator

	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.GetMetrics()
		appState.Metrics = promMetrics
	}

	// TODO: configure http transport for efficient intra-cluster comm
	remoteIndexClient := clients.NewRemoteIndex(appState.ClusterHttpClient)
	remoteNodesClient := clients.NewRemoteNode(appState.ClusterHttpClient)
	replicationClient := clients.NewReplicationClient(appState.ClusterHttpClient)
	repo, err := db.New(appState.Logger, db.Config{
		ServerVersion:                  config.ServerVersion,
		GitHash:                        config.GitHash,
		MemtablesFlushDirtyAfter:       appState.ServerConfig.Config.Persistence.MemtablesFlushDirtyAfter,
		MemtablesInitialSizeMB:         10,
		MemtablesMaxSizeMB:             appState.ServerConfig.Config.Persistence.MemtablesMaxSizeMB,
		MemtablesMinActiveSeconds:      appState.ServerConfig.Config.Persistence.MemtablesMinActiveDurationSeconds,
		MemtablesMaxActiveSeconds:      appState.ServerConfig.Config.Persistence.MemtablesMaxActiveDurationSeconds,
		SegmentsCleanupIntervalSeconds: appState.ServerConfig.Config.Persistence.LSMSegmentsCleanupIntervalSeconds,
		MaxSegmentSize:                 appState.ServerConfig.Config.Persistence.LSMMaxSegmentSize,
		HNSWMaxLogSize:                 appState.ServerConfig.Config.Persistence.HNSWMaxLogSize,
		HNSWWaitForCachePrefill:        appState.ServerConfig.Config.HNSWStartupWaitForVectorCache,
		VisitedListPoolMaxSize:         appState.ServerConfig.Config.HNSWVisitedListPoolMaxSize,
		RootPath:                       appState.ServerConfig.Config.Persistence.DataPath,
		QueryLimit:                     appState.ServerConfig.Config.QueryDefaults.Limit,
		QueryMaximumResults:            appState.ServerConfig.Config.QueryMaximumResults,
		QueryNestedRefLimit:            appState.ServerConfig.Config.QueryNestedCrossReferenceLimit,
		MaxImportGoroutinesFactor:      appState.ServerConfig.Config.MaxImportGoroutinesFactor,
		TrackVectorDimensions:          appState.ServerConfig.Config.TrackVectorDimensions,
		ResourceUsage:                  appState.ServerConfig.Config.ResourceUsage,
		AvoidMMap:                      appState.ServerConfig.Config.AvoidMmap,
		DisableLazyLoadShards:          appState.ServerConfig.Config.DisableLazyLoadShards,
		ForceFullReplicasSearch:        appState.ServerConfig.Config.ForceFullReplicasSearch,
		// Pass dummy replication config with minimum factor 1. Otherwise the
		// setting is not backward-compatible. The user may have created a class
		// with factor=1 before the change was introduced. Now their setup would no
		// longer start up if the required minimum is now higher than 1. We want
		// the required minimum to only apply to newly created classes - not block
		// loading existing ones.
		Replication: replication.GlobalConfig{MinimumFactor: 1},
	}, remoteIndexClient, appState.Cluster, remoteNodesClient, replicationClient, appState.Metrics, appState.MemWatch) // TODO client
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid new DB")
	}

	appState.DB = repo
	if appState.ServerConfig.Config.Monitoring.Enabled {
		appState.TenantActivity.SetSource(appState.DB)
	}
	setupDebugHandlers(appState)
	setupGoProfiling(appState.ServerConfig.Config, appState.Logger)

	migrator := db.NewMigrator(repo, appState.Logger)
	vectorRepo = repo
	// migrator = vectorMigrator
	explorer := traverser.NewExplorer(repo, appState.Logger, appState.Modules, traverser.NewMetrics(appState.Metrics), appState.ServerConfig.Config)
	schemaRepo := schemarepo.NewStore(appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err = schemaRepo.Open(); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema repo")
		os.Exit(1)
	}

	localClassifierRepo, err := classifications.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize classifications repo")
		os.Exit(1)
	}

	// TODO: configure http transport for efficient intra-cluster comm
	classificationsTxClient := clients.NewClusterClassifications(appState.ClusterHttpClient)
	classifierRepo := classifications.NewDistributeRepo(classificationsTxClient,
		appState.Cluster, localClassifierRepo, appState.Logger)
	appState.ClassificationRepo = classifierRepo

	scaler := scaler.New(appState.Cluster, vectorRepo,
		remoteIndexClient, appState.Logger, appState.ServerConfig.Config.Persistence.DataPath)
	appState.Scaler = scaler

	server2port, err := parseNode2Port(appState)
	if len(server2port) == 0 || err != nil {
		appState.Logger.
			WithField("action", "startup").
			WithField("raft-join", appState.ServerConfig.Config.Raft.Join).
			WithError(err).
			Fatal("parsing raft-join")
		os.Exit(1)
	}

	nodeName := appState.Cluster.LocalName()
	nodeAddr, _ := appState.Cluster.NodeHostname(nodeName)
	addrs := strings.Split(nodeAddr, ":")
	dataPath := appState.ServerConfig.Config.Persistence.DataPath

	rConfig := rCluster.Config{
		WorkDir:                filepath.Join(dataPath, config.DefaultRaftDir),
		NodeID:                 nodeName,
		Host:                   addrs[0],
		RaftPort:               appState.ServerConfig.Config.Raft.Port,
		RPCPort:                appState.ServerConfig.Config.Raft.InternalRPCPort,
		RaftRPCMessageMaxSize:  appState.ServerConfig.Config.Raft.RPCMessageMaxSize,
		BootstrapTimeout:       appState.ServerConfig.Config.Raft.BootstrapTimeout,
		BootstrapExpect:        appState.ServerConfig.Config.Raft.BootstrapExpect,
		HeartbeatTimeout:       appState.ServerConfig.Config.Raft.HeartbeatTimeout,
		ElectionTimeout:        appState.ServerConfig.Config.Raft.ElectionTimeout,
		SnapshotInterval:       appState.ServerConfig.Config.Raft.SnapshotInterval,
		SnapshotThreshold:      appState.ServerConfig.Config.Raft.SnapshotThreshold,
		ConsistencyWaitTimeout: appState.ServerConfig.Config.Raft.ConsistencyWaitTimeout,
		MetadataOnlyVoters:     appState.ServerConfig.Config.Raft.MetadataOnlyVoters,
		EnableOneNodeRecovery:  appState.ServerConfig.Config.Raft.EnableOneNodeRecovery,
		ForceOneNodeRecovery:   appState.ServerConfig.Config.Raft.ForceOneNodeRecovery,
		DB:                     nil,
		Parser:                 schema.NewParser(appState.Cluster, vectorIndex.ParseAndValidateConfig, migrator, appState.Modules),
		NodeNameToPortMap:      server2port,
		NodeToAddressResolver:  appState.Cluster,
		NodeSelector:           appState.Cluster,
		Logger:                 appState.Logger,
		IsLocalHost:            appState.ServerConfig.Config.Cluster.Localhost,
		LoadLegacySchema:       schemaRepo.LoadLegacySchema,
		SaveLegacySchema:       schemaRepo.SaveLegacySchema,
		EnableFQDNResolver:     appState.ServerConfig.Config.Raft.EnableFQDNResolver,
		FQDNResolverTLD:        appState.ServerConfig.Config.Raft.FQDNResolverTLD,
		SentryEnabled:          appState.ServerConfig.Config.Sentry.Enabled,
	}
	for _, name := range appState.ServerConfig.Config.Raft.Join[:rConfig.BootstrapExpect] {
		if strings.Contains(name, rConfig.NodeID) {
			rConfig.Voter = true
			break
		}
	}

	appState.ClusterService = rCluster.New(rConfig)
	executor := schema.NewExecutor(migrator,
		appState.ClusterService.SchemaReader(),
		appState.Logger, backup.RestoreClassDir(dataPath),
	)
	schemaManager, err := schemaUC.NewManager(migrator,
		appState.ClusterService.Raft,
		appState.ClusterService.SchemaReader(),
		schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		vectorIndex.ParseAndValidateConfig, appState.Modules, inverted.ValidateConfig,
		appState.Modules, appState.Cluster, scaler,
	)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}

	appState.SchemaManager = schemaManager
	appState.RemoteIndexIncoming = sharding.NewRemoteIndexIncoming(repo, appState.ClusterService.SchemaReader(), appState.Modules)
	appState.RemoteNodeIncoming = sharding.NewRemoteNodeIncoming(repo)
	appState.RemoteReplicaIncoming = replica.NewRemoteReplicaIncoming(repo, appState.ClusterService.SchemaReader())

	backupManager := backup.NewHandler(appState.Logger, appState.Authorizer,
		schemaManager, repo, appState.Modules)
	appState.BackupManager = backupManager

	enterrors.GoWrapper(func() { clusterapi.Serve(appState) }, appState.Logger)

	vectorRepo.SetSchemaGetter(schemaManager)
	explorer.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	appState.Traverser = traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics),
		appState.ServerConfig.Config.MaximumConcurrentGetRequests)

	updateSchemaCallback := makeUpdateSchemaCall(appState)
	executor.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// while we accept an overall longer startup, e.g. due to a recovery, we
	// still want to limit the module startup context, as that's mostly service
	// discovery / dependency checking
	moduleCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = initModules(moduleCtx, appState)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't initialize")
	}

	enterrors.GoWrapper(func() {
		if err := appState.ClusterService.Open(context.Background(), executor); err != nil {
			appState.Logger.
				WithField("action", "startup").
				WithError(err).
				Fatal("could not open cloud meta store")
		}
	}, appState.Logger)

	// TODO-RAFT: refactor remove this sleep
	// this sleep was used to block GraphQL and give time to RAFT to start.
	time.Sleep(2 * time.Second)

	batchManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics)
	appState.BatchManager = batchManager

	err = migrator.AdjustFilterablePropSettings(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "adjustFilterablePropSettings").
			Fatal("migration failed")
		os.Exit(1)
	}

	// FIXME to avoid import cycles, tasks are passed as strings
	reindexTaskNames := []string{}
	var reindexCtx context.Context
	reindexCtx, appState.ReindexCtxCancel = context.WithCancel(context.Background())
	reindexFinished := make(chan error, 1)

	if appState.ServerConfig.Config.ReindexSetToRoaringsetAtStartup {
		reindexTaskNames = append(reindexTaskNames, "ShardInvertedReindexTaskSetToRoaringSet")
	}
	if appState.ServerConfig.Config.IndexMissingTextFilterableAtStartup {
		reindexTaskNames = append(reindexTaskNames, "ShardInvertedReindexTaskMissingTextFilterable")
	}
	if len(reindexTaskNames) > 0 {
		// start reindexing inverted indexes (if requested by user) in the background
		// allowing db to complete api configuration and start handling requests
		enterrors.GoWrapper(func() {
			appState.Logger.
				WithField("action", "startup").
				Info("Reindexing inverted indexes")
			reindexFinished <- migrator.InvertedReindex(reindexCtx, reindexTaskNames...)
		}, appState.Logger)
	}

	configureServer = makeConfigureServer(appState)

	// Add dimensions to all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.ReindexVectorDimensionsAtStartup {
		appState.Logger.
			WithField("action", "startup").
			Info("Reindexing dimensions")
		migrator.RecalculateVectorDimensions(ctx)
	}

	// Add recount properties of all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.RecountPropertiesAtStartup {
		migrator.RecountProperties(ctx)
	}

	return appState
}

func parseNode2Port(appState *state.State) (m map[string]int, err error) {
	m = make(map[string]int, len(appState.ServerConfig.Config.Raft.Join))
	for _, raftNamePort := range appState.ServerConfig.Config.Raft.Join {
		np := strings.Split(raftNamePort, ":")
		if np[0] == appState.Cluster.LocalName() {
			m[np[0]] = appState.ServerConfig.Config.Raft.Port
			continue
		}
		if m[np[0]], err = strconv.Atoi(np[1]); err != nil {
			return m, fmt.Errorf("expect integer as raft port: got %s:: %w", raftNamePort, err)
		}
	}

	return m, nil
}

// parseVotersNames parses names of all voters.
// If we reach this point, we assume that the configuration is valid
func parseVotersNames(cfg config.Raft) (m map[string]struct{}) {
	m = make(map[string]struct{}, cfg.BootstrapExpect)
	for _, raftNamePort := range cfg.Join[:cfg.BootstrapExpect] {
		m[strings.Split(raftNamePort, ":")[0]] = struct{}{}
	}
	return m
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	config.ServerVersion = parseVersionFromSwaggerSpec()
	appState := MakeAppState(ctx, connectorOptionGroup)

	appState.Logger.WithFields(logrus.Fields{
		"server_version":   config.ServerVersion,
		"docker_image_tag": config.ImageTag,
	}).Infof("configured versions")

	api.ServeError = openapierrors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = composer.New(
		appState.ServerConfig.Config.Authentication,
		appState.APIKey, appState.OIDC)

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithFields(logrus.Fields{"action": "restapi_management", "docker_image_tag": config.ImageTag}).Infof(msg, args...)
	}

	classifier := classification.New(appState.SchemaManager, appState.ClassificationRepo, appState.DB, // the DB is the vectorrepo
		appState.Authorizer,
		appState.Logger, appState.Modules)

	setupSchemaHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger)
	objectsManager := objects.NewManager(appState.Locks,
		appState.SchemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.DB, appState.Modules,
		objects.NewMetrics(appState.Metrics), appState.MemWatch)
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger,
		appState.Modules, appState.Metrics)
	setupObjectBatchHandlers(api, appState.BatchManager, appState.Metrics, appState.Logger)
	setupGraphQLHandlers(api, appState, appState.SchemaManager, appState.ServerConfig.Config.DisableGraphQL,
		appState.Metrics, appState.Logger)
	setupMiscHandlers(api, appState.ServerConfig, appState.Modules,
		appState.Metrics, appState.Logger)
	setupClassificationHandlers(api, classifier, appState.Metrics, appState.Logger)
	backupScheduler := backup.NewScheduler(
		appState.Authorizer,
		clients.NewClusterBackups(appState.ClusterHttpClient),
		appState.DB, appState.Modules,
		membership{appState.Cluster, appState.ClusterService},
		appState.SchemaManager,
		appState.Logger)
	setupBackupHandlers(api, backupScheduler, appState.Metrics, appState.Logger)
	setupNodesHandlers(api, appState.SchemaManager, appState.DB, appState)

	grpcServer := createGrpcServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)

	telemeter := telemetry.New(appState.DB, appState.SchemaManager, appState.Logger)
	if telemetryEnabled(appState) {
		enterrors.GoWrapper(func() {
			if err := telemeter.Start(context.Background()); err != nil {
				appState.Logger.
					WithField("action", "startup").
					Errorf("telemetry failed to start: %s", err.Error())
			}
		}, appState.Logger)
	}

	api.ServerShutdown = func() {
		if telemetryEnabled(appState) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// must be shutdown before the db, to ensure the
			// termination payload contains the correct
			// object count
			if err := telemeter.Stop(ctx); err != nil {
				appState.Logger.WithField("action", "stop_telemetry").
					Errorf("failed to stop telemetry: %s", err.Error())
			}
		}

		// stop reindexing on server shutdown
		appState.ReindexCtxCancel()

		// gracefully stop gRPC server
		grpcServer.GracefulStop()

		if appState.ServerConfig.Config.Sentry.Enabled {
			sentry.Flush(2 * time.Second)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := appState.ClusterService.Close(ctx); err != nil {
			panic(err)
		}
	}

	startGrpcServer(grpcServer, appState)

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine(ctx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	appState := &state.State{}

	logger := logger()
	appState.Logger = logger

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created startup context, nothing done so far")

	// Load the config using the flags
	serverConfig := &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err := serverConfig.LoadConfig(options, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).Error("could not load config")
		logger.Exit(1)
	}
	dataPath := serverConfig.Config.Persistence.DataPath
	if err := os.MkdirAll(dataPath, 0o777); err != nil {
		logger.WithField("action", "startup").
			WithField("path", dataPath).Error("cannot create data directory")
		logger.Exit(1)
	}

	monitoring.InitConfig(serverConfig.Config.Monitoring)

	if serverConfig.Config.DisableGraphQL {
		logger.WithFields(logrus.Fields{
			"action":          "startup",
			"disable_graphql": true,
		}).Warnf("GraphQL API disabled, relying only on gRPC API for querying. " +
			"This is considered experimental and will likely experience breaking changes " +
			"before reaching general availability")
	}

	logger.WithFields(logrus.Fields{
		"action":                    "startup",
		"default_vectorizer_module": serverConfig.Config.DefaultVectorizerModule,
	}).Infof("the default vectorizer modules is set to %q, as a result all new "+
		"schema classes without an explicit vectorizer setting, will use this "+
		"vectorizer", serverConfig.Config.DefaultVectorizerModule)

	logger.WithFields(logrus.Fields{
		"action":              "startup",
		"auto_schema_enabled": serverConfig.Config.AutoSchema.Enabled,
	}).Infof("auto schema enabled setting is set to \"%v\"", serverConfig.Config.AutoSchema.Enabled)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("config loaded")

	appState.OIDC = configureOIDC(appState)
	appState.APIKey = configureAPIKey(appState)
	appState.AnonymousAccess = configureAnonymousAccess(appState)
	appState.Authorizer = configureAuthorizer(appState)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	appState.Locks = &dummyLock{}

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	var nonStorageNodes map[string]struct{}
	if cfg := serverConfig.Config.Raft; cfg.MetadataOnlyVoters {
		nonStorageNodes = parseVotersNames(cfg)
	}
	clusterState, err := cluster.Init(serverConfig.Config.Cluster, dataPath, nonStorageNodes, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).
			Error("could not init cluster state")
		logger.Exit(1)
	}

	appState.Cluster = clusterState
	appState.Logger.
		WithField("action", "startup").
		Debug("startup routine complete")

	return appState
}

// logger does not parse the regular config object, as logging needs to be
// configured before the configuration is even loaded/parsed. We are thus
// "manually" reading the desired env vars and set reasonable defaults if they
// are not set.
//
// Defaults to log level info and json format
func logger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&WeaviateTextFormatter{
		config.GitHash,
		config.ImageTag,
		config.ServerVersion,
		goruntime.Version(),
		&logrus.TextFormatter{},
	})

	if os.Getenv("LOG_FORMAT") != "text" {
		logger.SetFormatter(&WeaviateJSONFormatter{
			config.GitHash,
			config.ImageTag,
			config.ServerVersion,
			goruntime.Version(),
			&logrus.JSONFormatter{},
		})
	}
	switch os.Getenv("LOG_LEVEL") {
	case "panic":
		logger.SetLevel(logrus.PanicLevel)
	case "fatal":
		logger.SetLevel(logrus.FatalLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "warning":
		logger.SetLevel(logrus.WarnLevel)
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "trace":
		logger.SetLevel(logrus.TraceLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}

	return logger
}

type dummyLock struct{}

func (d *dummyLock) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (d *dummyLock) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

// everything hard-coded right now, to be made dynamic (from go plugins later)
func registerModules(appState *state.State) error {
	appState.Logger.
		WithField("action", "startup").
		Debug("start registering modules")

	appState.Modules = modules.NewProvider()

	enabledModules := map[string]bool{}
	if len(appState.ServerConfig.Config.EnableModules) > 0 {
		modules := strings.Split(appState.ServerConfig.Config.EnableModules, ",")
		for _, module := range modules {
			enabledModules[strings.TrimSpace(module)] = true
		}
	}

	if _, ok := enabledModules["text2vec-contextionary"]; ok {
		appState.Modules.Register(modcontextionary.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "text2vec-contextionary").
			Debug("enabled module")
	}

	if _, ok := enabledModules["text2vec-transformers"]; ok {
		appState.Modules.Register(modtransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "text2vec-transformers").
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgpt4all.Name]; ok {
		appState.Modules.Register(modgpt4all.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgpt4all.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankervoyageai.Name]; ok {
		appState.Modules.Register(modrerankervoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankervoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankertransformers.Name]; ok {
		appState.Modules.Register(modrerankertransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankertransformers.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankercohere.Name]; ok {
		appState.Modules.Register(modrerankercohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankercohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankerdummy.Name]; ok {
		appState.Modules.Register(modrerankerdummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankerdummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules["qna-transformers"]; ok {
		appState.Modules.Register(modqna.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "qna-transformers").
			Debug("enabled module")
	}

	if _, ok := enabledModules["sum-transformers"]; ok {
		appState.Modules.Register(modsum.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "sum-transformers").
			Debug("enabled module")
	}

	if _, ok := enabledModules["img2vec-neural"]; ok {
		appState.Modules.Register(modimage.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "img2vec-neural").
			Debug("enabled module")
	}

	if _, ok := enabledModules["ner-transformers"]; ok {
		appState.Modules.Register(modner.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "ner-transformers").
			Debug("enabled module")
	}

	if _, ok := enabledModules["text-spellcheck"]; ok {
		appState.Modules.Register(modspellcheck.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "text-spellcheck").
			Debug("enabled module")
	}

	if _, ok := enabledModules["multi2vec-clip"]; ok {
		appState.Modules.Register(modclip.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "multi2vec-clip").
			Debug("enabled module")
	}

	_, enabledMulti2VecGoogle := enabledModules[modmulti2vecgoogle.Name]
	_, enabledMulti2VecPaLM := enabledModules[modmulti2vecgoogle.LegacyName]
	if enabledMulti2VecGoogle || enabledMulti2VecPaLM {
		appState.Modules.Register(modmulti2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2veccohere.Name]; ok {
		appState.Modules.Register(modmulti2veccohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2veccohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecjinaai.Name]; ok {
		appState.Modules.Register(modmulti2vecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules["text2vec-openai"]; ok {
		appState.Modules.Register(modopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "text2vec-openai").
			Debug("enabled module")
	}

	if _, ok := enabledModules["qna-openai"]; ok {
		appState.Modules.Register(modqnaopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "qna-openai").
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativecohere.Name]; ok {
		appState.Modules.Register(modgenerativecohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativecohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativemistral.Name]; ok {
		appState.Modules.Register(modgenerativemistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativemistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeopenai.Name]; ok {
		appState.Modules.Register(modgenerativeopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeollama.Name]; ok {
		appState.Modules.Register(modgenerativeollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedummy.Name]; ok {
		appState.Modules.Register(modgenerativedummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeaws.Name]; ok {
		appState.Modules.Register(modgenerativeaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modhuggingface.Name]; ok {
		appState.Modules.Register(modhuggingface.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modhuggingface.Name).
			Debug("enabled module")
	}

	_, enabledGenerativeGoogle := enabledModules[modgenerativegoogle.Name]
	_, enabledGenerativePaLM := enabledModules[modgenerativegoogle.LegacyName]
	if enabledGenerativeGoogle || enabledGenerativePaLM {
		appState.Modules.Register(modgenerativegoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativegoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanyscale.Name]; ok {
		appState.Modules.Register(modgenerativeanyscale.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanyscale.Name).
			Debug("enabled module")
	}

	_, enabledText2vecGoogle := enabledModules[modtext2vecgoogle.Name]
	_, enabledText2vecPaLM := enabledModules[modtext2vecgoogle.LegacyName]
	if enabledText2vecGoogle || enabledText2vecPaLM {
		appState.Modules.Register(modtext2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecaws.Name]; ok {
		appState.Modules.Register(modtext2vecaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgfs.Name]; ok {
		appState.Modules.Register(modstgfs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgfs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgs3.Name]; ok {
		appState.Modules.Register(modstgs3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgs3.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstggcs.Name]; ok {
		appState.Modules.Register(modstggcs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstggcs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgazure.Name]; ok {
		appState.Modules.Register(modstgazure.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgazure.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcentroid.Name]; ok {
		appState.Modules.Register(modcentroid.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcentroid.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcohere.Name]; ok {
		appState.Modules.Register(modcohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modvoyageai.Name]; ok {
		appState.Modules.Register(modvoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modvoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmistral.Name]; ok {
		appState.Modules.Register(modmistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modbind.Name]; ok {
		appState.Modules.Register(modbind.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modbind.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modjinaai.Name]; ok {
		appState.Modules.Register(modjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modollama.Name]; ok {
		appState.Modules.Register(modollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeoctoai.Name]; ok {
		appState.Modules.Register(modgenerativeoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeoctoai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecoctoai.Name]; ok {
		appState.Modules.Register(modtext2vecoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecoctoai.Name).
			Debug("enabled module")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("completed registering modules")

	return nil
}

func initModules(ctx context.Context, appState *state.State) error {
	storageProvider, err := modulestorage.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		return errors.Wrap(err, "init storage provider")
	}

	// TODO: gh-1481 don't pass entire appState in, but only what's needed. Probably only
	// config?
	moduleParams := moduletools.NewInitParams(storageProvider, appState,
		appState.ServerConfig.Config, appState.Logger)

	appState.Logger.
		WithField("action", "startup").
		Debug("start initializing modules")
	if err := appState.Modules.Init(ctx, moduleParams, appState.Logger); err != nil {
		return errors.Wrap(err, "init modules")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("finished initializing modules")

	return nil
}

type clientWithAuth struct {
	r         http.RoundTripper
	basicAuth cluster.BasicAuth
}

func (c clientWithAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	r.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	return c.r.RoundTrip(r)
}

func reasonableHttpClient(authConfig cluster.AuthConfig) *http.Client {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if authConfig.BasicAuth.Enabled() {
		return &http.Client{Transport: clientWithAuth{r: t, basicAuth: authConfig.BasicAuth}}
	}
	return &http.Client{Transport: t}
}

func setupGoProfiling(config config.Config, logger logrus.FieldLogger) {
	functionsToIgnoreInProfiling := []string{
		"raft",
		"http2",
		"memberlist",
		"selectgo", // various tickers
		"cluster",
		"rest",
		"signal_recv",
		"backgroundRead",
		"SetupGoProfiling",
		"serve",
		"Serve",
		"batchWorker",
	}
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler(functionsToIgnoreInProfiling...))
	enterrors.GoWrapper(func() {
		portNumber := config.Profiling.Port
		if portNumber == 0 {
			fmt.Println(http.ListenAndServe(":6060", nil))
		} else {
			http.ListenAndServe(fmt.Sprintf(":%d", portNumber), nil)
		}
	}, logger)

	if config.Profiling.BlockProfileRate > 0 {
		goruntime.SetBlockProfileRate(config.Profiling.BlockProfileRate)
	}

	if config.Profiling.MutexProfileFraction > 0 {
		goruntime.SetMutexProfileFraction(config.Profiling.MutexProfileFraction)
	}
}

func parseVersionFromSwaggerSpec() string {
	spec := struct {
		Info struct {
			Version string `json:"version"`
		} `json:"info"`
	}{}

	err := json.Unmarshal(SwaggerJSON, &spec)
	if err != nil {
		panic(err)
	}

	return spec.Info.Version
}

func limitResources(appState *state.State) {
	if os.Getenv("LIMIT_RESOURCES") == "true" {
		appState.Logger.Info("Limiting resources:  memory: 80%, cores: all but one")
		if os.Getenv("GOMAXPROCS") == "" {
			// Fetch the number of cores from the cgroups cpuset
			// and parse it into an int
			cores, err := getCores()
			if err == nil {
				appState.Logger.WithField("cores", cores).
					Warn("GOMAXPROCS not set, and unable to read from cgroups, setting to number of cores")
				goruntime.GOMAXPROCS(cores)
			} else {
				cores = goruntime.NumCPU() - 1
				if cores > 0 {
					appState.Logger.WithField("cores", cores).
						Warnf("Unable to read from cgroups: %v, setting to max cores to: %v", err, cores)
					goruntime.GOMAXPROCS(cores)
				}
			}
		}

		limit, err := memlimit.SetGoMemLimit(0.8)
		if err != nil {
			appState.Logger.WithError(err).Warnf("Unable to set memory limit from cgroups: %v", err)
			// Set memory limit to 90% of the available memory
			limit := int64(float64(memory.TotalMemory()) * 0.8)
			debug.SetMemoryLimit(limit)
			appState.Logger.WithField("limit", limit).Info("Set memory limit based on available memory")
		} else {
			appState.Logger.WithField("limit", limit).Info("Set memory limit")
		}
	} else {
		appState.Logger.Info("No resource limits set, weaviate will use all available memory and CPU. " +
			"To limit resources, set LIMIT_RESOURCES=true")
	}
}

func telemetryEnabled(state *state.State) bool {
	return !state.ServerConfig.Config.DisableTelemetry
}

type membership struct {
	*cluster.State
	raft *rCluster.Service
}

func (m membership) LeaderID() string {
	_, id := m.raft.LeaderWithID()
	return id
}
