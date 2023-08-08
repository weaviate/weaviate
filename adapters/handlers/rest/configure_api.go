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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	goruntime "runtime"
	"strings"
	"time"

	_ "net/http/pprof"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
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
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/replication"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativepalm "github.com/weaviate/weaviate/modules/generative-palm"
	modimage "github.com/weaviate/weaviate/modules/img2vec-neural"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modner "github.com/weaviate/weaviate/modules/ner-transformers"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modqna "github.com/weaviate/weaviate/modules/qna-transformers"
	modcentroid "github.com/weaviate/weaviate/modules/ref2vec-centroid"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankertransformers "github.com/weaviate/weaviate/modules/reranker-transformers"
	modsum "github.com/weaviate/weaviate/modules/sum-transformers"
	modspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modcontextionary "github.com/weaviate/weaviate/modules/text2vec-contextionary"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
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

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	config.ServerVersion = parseVersionFromSwaggerSpec()

	appState := startupRoutine(ctx)
	setupGoProfiling(appState.ServerConfig.Config)

	if appState.ServerConfig.Config.Monitoring.Enabled {
		// only monitoring tool supported at the moment is prometheus
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", appState.ServerConfig.Config.Monitoring.Port), mux)
		}()
	}

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

	api.ServeError = openapierrors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = composer.New(
		appState.ServerConfig.Config.Authentication,
		appState.APIKey, appState.OIDC)

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithField("action", "restapi_management").Infof(msg, args...)
	}

	clusterHttpClient := reasonableHttpClient()

	var vectorRepo vectorRepo
	var vectorMigrator migrate.Migrator
	var migrator migrate.Migrator

	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.GetMetrics()
		appState.Metrics = promMetrics
	}

	// TODO: configure http transport for efficient intra-cluster comm
	remoteIndexClient := clients.NewRemoteIndex(clusterHttpClient)
	remoteNodesClient := clients.NewRemoteNode(clusterHttpClient)
	replicationClient := clients.NewReplicationClient(clusterHttpClient)
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
		MaxImportGoroutinesFactor: appState.ServerConfig.Config.MaxImportGoroutinesFactor,
		TrackVectorDimensions:     appState.ServerConfig.Config.TrackVectorDimensions,
		ResourceUsage:             appState.ServerConfig.Config.ResourceUsage,
		AvoidMMap:                 appState.ServerConfig.Config.AvoidMmap,
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
	classificationsTxClient := clients.NewClusterClassifications(clusterHttpClient)
	classifierRepo := classifications.NewDistributeRepo(classificationsTxClient,
		appState.Cluster, localClassifierRepo, appState.Logger)
	appState.ClassificationRepo = classifierRepo

	scaler := scaler.New(appState.Cluster, vectorRepo,
		remoteIndexClient, appState.Logger, appState.ServerConfig.Config.Persistence.DataPath)
	appState.Scaler = scaler

	// TODO: configure http transport for efficient intra-cluster comm
	schemaTxClient := clients.NewClusterSchema(clusterHttpClient)
	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		enthnsw.ParseAndValidateConfig, appState.Modules, inverted.ValidateConfig,
		appState.Modules, appState.Cluster, schemaTxClient, scaler,
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

	backupScheduler := backup.NewScheduler(
		appState.Authorizer,
		clients.NewClusterBackups(clusterHttpClient),
		repo, appState.Modules,
		appState.Cluster,
		appState.Logger)

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

	objectsManager := objects.NewManager(appState.Locks,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, vectorRepo, appState.Modules,
		objects.NewMetrics(appState.Metrics))
	batchObjectsManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics)

	objectsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics),
		appState.ServerConfig.Config.MaximumConcurrentGetRequests)
	appState.Traverser = objectsTraverser

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer,
		appState.Logger, appState.Modules)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, objectsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	setupSchemaHandlers(api, schemaManager, appState.Metrics, appState.Logger)
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger,
		appState.Modules, appState.Metrics)
	setupObjectBatchHandlers(api, batchObjectsManager, appState.Metrics, appState.Logger)
	setupGraphQLHandlers(api, appState, schemaManager, appState.ServerConfig.Config.DisableGraphQL,
		appState.Metrics, appState.Logger)
	setupMiscHandlers(api, appState.ServerConfig, schemaManager, appState.Modules,
		appState.Metrics, appState.Logger)
	setupClassificationHandlers(api, classifier, appState.Metrics, appState.Logger)
	setupBackupHandlers(api, backupScheduler, appState.Metrics, appState.Logger)
	setupNodesHandlers(api, schemaManager, repo, appState)

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
	reindexCtx, reindexCtxCancel := context.WithCancel(context.Background())
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

	grpcServer := createGrpcServer(appState)

	api.ServerShutdown = func() {
		// stop reindexing on server shutdown
		reindexCtxCancel()

		// gracefully stop gRPC server
		grpcServer.GracefulStop()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := repo.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
	configureServer = makeConfigureServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)

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

	startGrpcServer(grpcServer, appState)

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine(ctx context.Context) *state.State {
	appState := &state.State{}

	logger := logger()
	appState.Logger = logger

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created startup context, nothing done so far")

	// Load the config using the flags
	serverConfig := &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err := serverConfig.LoadConfig(connectorOptionGroup, logger)
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

// everything hard-coded right now, to be made dynmaic (from go plugins later)
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

	if _, ok := enabledModules[modtext2vecpalm.Name]; ok {
		appState.Modules.Register(modtext2vecpalm.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecpalm.Name).
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
		appState.Logger)

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

func reasonableHttpClient() *http.Client {
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
