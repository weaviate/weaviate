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
	"os"
	goruntime "runtime"
	"runtime/debug"
	"strings"
	"time"

	_ "net/http/pprof"

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
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	modulestorage "github.com/weaviate/weaviate/adapters/repos/modules"
	schemarepo "github.com/weaviate/weaviate/adapters/repos/schema"
	txstore "github.com/weaviate/weaviate/adapters/repos/transactions"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/replication"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativepalm "github.com/weaviate/weaviate/modules/generative-palm"
	modimage "github.com/weaviate/weaviate/modules/img2vec-neural"
	modbind "github.com/weaviate/weaviate/modules/multi2vec-bind"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modner "github.com/weaviate/weaviate/modules/ner-transformers"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modqna "github.com/weaviate/weaviate/modules/qna-transformers"
	modcentroid "github.com/weaviate/weaviate/modules/ref2vec-centroid"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankertransformers "github.com/weaviate/weaviate/modules/reranker-transformers"
	modsum "github.com/weaviate/weaviate/modules/sum-transformers"
	modspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modcontextionary "github.com/weaviate/weaviate/modules/text2vec-contextionary"
	modgpt4all "github.com/weaviate/weaviate/modules/text2vec-gpt4all"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtext2vecpalm "github.com/weaviate/weaviate/modules/text2vec-palm"
	modtransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/scaler"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/traverser"
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

	cores := strings.Split(strings.TrimSpace(string(cpuset)), ",")
	return len(cores), nil
}

func MakeAppState(ctx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	appState := startupRoutine(ctx, options)
	setupGoProfiling(appState.ServerConfig.Config)

	if appState.ServerConfig.Config.Monitoring.Enabled {
		// only monitoring tool supported at the moment is prometheus
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", appState.ServerConfig.Config.Monitoring.Port), mux)
		}()
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

	var vectorRepo vectorRepo
	var vectorMigrator migrate.Migrator
	var migrator migrate.Migrator

	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.GetMetrics()
		appState.Metrics = promMetrics
	}

	// TODO: configure http transport for efficient intra-cluster comm
	remoteIndexClient := clients.NewRemoteIndex(appState.ClusterHttpClient)
	remoteNodesClient := clients.NewRemoteNode(appState.ClusterHttpClient)
	replicationClient := clients.NewReplicationClient(appState.ClusterHttpClient)
	repo, err := db.New(appState.Logger, db.Config{
		ServerVersion:             config.ServerVersion,
		GitHash:                   config.GitHash,
		MemtablesFlushIdleAfter:   appState.ServerConfig.Config.Persistence.FlushIdleMemtablesAfter,
		MemtablesInitialSizeMB:    10,
		MemtablesMaxSizeMB:        appState.ServerConfig.Config.Persistence.MemtablesMaxSizeMB,
		MemtablesMinActiveSeconds: appState.ServerConfig.Config.Persistence.MemtablesMinActiveDurationSeconds,
		MemtablesMaxActiveSeconds: appState.ServerConfig.Config.Persistence.MemtablesMaxActiveDurationSeconds,
		RootPath:                  appState.ServerConfig.Config.Persistence.DataPath,
		QueryLimit:                appState.ServerConfig.Config.QueryDefaults.Limit,
		QueryMaximumResults:       appState.ServerConfig.Config.QueryMaximumResults,
		QueryNestedRefLimit:       appState.ServerConfig.Config.QueryNestedCrossReferenceLimit,
		MaxImportGoroutinesFactor: appState.ServerConfig.Config.MaxImportGoroutinesFactor,
		TrackVectorDimensions:     appState.ServerConfig.Config.TrackVectorDimensions,
		ResourceUsage:             appState.ServerConfig.Config.ResourceUsage,
		AvoidMMap:                 appState.ServerConfig.Config.AvoidMmap,
		DisableLazyLoadShards:     appState.ServerConfig.Config.DisableLazyLoadShards,
		// Pass dummy replication config with minimum factor 1. Otherwise the
		// setting is not backward-compatible. The user may have created a class
		// with factor=1 before the change was introduced. Now their setup would no
		// longer start up if the required minimum is now higher than 1. We want
		// the required minimum to only apply to newly created classes - not block
		// loading existing ones.
		Replication: replication.GlobalConfig{MinimumFactor: 1},
	}, remoteIndexClient, appState.Cluster, remoteNodesClient, replicationClient, appState.Metrics) // TODO client
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid new DB")
	}

	appState.DB = repo
	vectorMigrator = db.NewMigrator(repo, appState.Logger)
	vectorRepo = repo
	migrator = vectorMigrator
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

	// TODO: configure http transport for efficient intra-cluster comm
	schemaTxClient := clients.NewClusterSchema(appState.ClusterHttpClient)
	schemaTxPersistence := txstore.NewStore(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	schemaTxPersistence.SetUmarshalFn(schemaUC.UnmarshalTransaction)
	if err := schemaTxPersistence.Open(); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not open tx repo")
		os.Exit(1)

	}

	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		vectorIndex.ParseAndValidateConfig, appState.Modules, inverted.ValidateConfig,
		appState.Modules, appState.Cluster, schemaTxClient,
		schemaTxPersistence, scaler,
	)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}

	appState.SchemaManager = schemaManager

	appState.RemoteIndexIncoming = sharding.NewRemoteIndexIncoming(repo)
	appState.RemoteNodeIncoming = sharding.NewRemoteNodeIncoming(repo)
	appState.RemoteReplicaIncoming = replica.NewRemoteReplicaIncoming(repo)

	backupManager := backup.NewHandler(appState.Logger, appState.Authorizer,
		schemaManager, repo, appState.Modules)
	appState.BackupManager = backupManager

	go clusterapi.Serve(appState)

	vectorRepo.SetSchemaGetter(schemaManager)
	explorer.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	err = vectorRepo.WaitForStartup(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").
			Fatal("db didn't start up")
		os.Exit(1)
	}

	if err := schemaManager.StartServing(ctx); err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").
			Fatal("schema manager: resume dangling txs")
		os.Exit(1)

	}

	batchManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics)
	appState.BatchManager = batchManager
	objectsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics),
		appState.ServerConfig.Config.MaximumConcurrentGetRequests)
	appState.Traverser = objectsTraverser

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, objectsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

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
		go func() {
			appState.Logger.
				WithField("action", "startup").
				Info("Reindexing inverted indexes")
			reindexFinished <- migrator.InvertedReindex(reindexCtx, reindexTaskNames...)
		}()
	}

	configureServer = makeConfigureServer(appState)

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

	// manually update schema once
	schema := schemaManager.GetSchemaSkipAuth()
	updateSchemaCallback(schema)

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

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	config.ServerVersion = parseVersionFromSwaggerSpec()
	appState := MakeAppState(ctx, connectorOptionGroup)

	api.ServeError = openapierrors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = composer.New(
		appState.ServerConfig.Config.Authentication,
		appState.APIKey, appState.OIDC)

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithField("action", "restapi_management").Infof(msg, args...)
	}

	classifier := classification.New(appState.SchemaManager, appState.ClassificationRepo, appState.DB, // the DB is the vectorrepo
		appState.Authorizer,
		appState.Logger, appState.Modules)

	setupSchemaHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger)
	objectsManager := objects.NewManager(appState.Locks,
		appState.SchemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.DB, appState.Modules,
		objects.NewMetrics(appState.Metrics))
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger,
		appState.Modules, appState.Metrics)
	setupObjectBatchHandlers(api, appState.BatchManager, appState.Metrics, appState.Logger)
	setupGraphQLHandlers(api, appState, appState.SchemaManager, appState.ServerConfig.Config.DisableGraphQL,
		appState.Metrics, appState.Logger)
	setupMiscHandlers(api, appState.ServerConfig, appState.SchemaManager, appState.Modules,
		appState.Metrics, appState.Logger)
	setupClassificationHandlers(api, classifier, appState.Metrics, appState.Logger)
	backupScheduler := backup.NewScheduler(
		appState.Authorizer,
		clients.NewClusterBackups(appState.ClusterHttpClient),
		appState.DB, appState.Modules,
		appState.Cluster,
		appState.Logger)
	setupBackupHandlers(api, backupScheduler, appState.Metrics, appState.Logger)
	setupNodesHandlers(api, appState.SchemaManager, appState.DB, appState)

	grpcServer := createGrpcServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)

	api.ServerShutdown = func() {
		// stop reindexing on server shutdown
		appState.ReindexCtxCancel()

		// gracefully stop gRPC server
		grpcServer.GracefulStop()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := appState.SchemaManager.Shutdown(ctx); err != nil {
			panic(err)
		}

		if err := appState.DB.Shutdown(ctx); err != nil {
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

	clusterState, err := cluster.Init(serverConfig.Config.Cluster, serverConfig.Config.Persistence.DataPath, logger)
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
	if os.Getenv("LOG_FORMAT") != "text" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}
	switch os.Getenv("LOG_LEVEL") {
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

	if _, ok := enabledModules[modgenerativeopenai.Name]; ok {
		appState.Modules.Register(modgenerativeopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeopenai.Name).
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

	if _, ok := enabledModules[modgenerativepalm.Name]; ok {
		appState.Modules.Register(modgenerativepalm.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativepalm.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanyscale.Name]; ok {
		appState.Modules.Register(modgenerativeanyscale.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanyscale.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecpalm.Name]; ok {
		appState.Modules.Register(modtext2vecpalm.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecpalm.Name).
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

func setupGoProfiling(config config.Config) {
	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()

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
