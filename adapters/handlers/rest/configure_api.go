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
	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/repos/classifications"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	modulestorage "github.com/semi-technologies/weaviate/adapters/repos/modules"
	schemarepo "github.com/semi-technologies/weaviate/adapters/repos/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
	modimage "github.com/semi-technologies/weaviate/modules/img2vec-neural"
	modclip "github.com/semi-technologies/weaviate/modules/multi2vec-clip"
	modner "github.com/semi-technologies/weaviate/modules/ner-transformers"
	modqna "github.com/semi-technologies/weaviate/modules/qna-transformers"
	modstgs3 "github.com/semi-technologies/weaviate/modules/storage-aws-s3"
	modstgfs "github.com/semi-technologies/weaviate/modules/storage-filesystem"
	modstggcs "github.com/semi-technologies/weaviate/modules/storage-gcs"
	modspellcheck "github.com/semi-technologies/weaviate/modules/text-spellcheck"
	modcontextionary "github.com/semi-technologies/weaviate/modules/text2vec-contextionary"
	modopenai "github.com/semi-technologies/weaviate/modules/text2vec-openai"
	modtransformers "github.com/semi-technologies/weaviate/modules/text2vec-transformers"
	"github.com/semi-technologies/weaviate/usecases/backup"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/semi-technologies/weaviate/usecases/objects"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
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
	SetSchemaGetter(schemaUC.SchemaGetter)
	WaitForStartup(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type explorer interface {
	GetClass(ctx context.Context, params traverser.GetParams) ([]interface{}, error)
	CrossClassVectorSearch(ctx context.Context, params traverser.ExploreParams) ([]search.Result, error)
	SetSchemaGetter(schemaUC.SchemaGetter)
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
			http.ListenAndServe(":2112", mux)
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

	api.OidcAuth = func(token string, scopes []string) (*models.Principal, error) {
		return appState.OIDC.ValidateAndExtract(token, scopes)
	}

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithField("action", "restapi_management").Infof(msg, args...)
	}

	clusterHttpClient := reasonableHttpClient()

	var vectorRepo vectorRepo
	var vectorMigrator migrate.Migrator
	var migrator migrate.Migrator
	var explorer explorer
	var schemaRepo schemaUC.Repo
	// var classifierRepo classification.Repo

	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.NewPrometheusMetrics()
		appState.Metrics = promMetrics
	}

	// TODO: configure http transport for efficient intra-cluster comm
	remoteIndexClient := clients.NewRemoteIndex(clusterHttpClient)
	repo := db.New(appState.Logger, db.Config{
		RootPath:                  appState.ServerConfig.Config.Persistence.DataPath,
		QueryLimit:                appState.ServerConfig.Config.QueryDefaults.Limit,
		QueryMaximumResults:       appState.ServerConfig.Config.QueryMaximumResults,
		DiskUseWarningPercentage:  appState.ServerConfig.Config.DiskUse.WarningPercentage,
		DiskUseReadOnlyPercentage: appState.ServerConfig.Config.DiskUse.ReadOnlyPercentage,
		MaxImportGoroutinesFactor: appState.ServerConfig.Config.MaxImportGoroutinesFactor,
	}, remoteIndexClient, appState.Cluster, appState.Metrics) // TODO client
	vectorMigrator = db.NewMigrator(repo, appState.Logger)
	vectorRepo = repo
	migrator = vectorMigrator
	explorer = traverser.NewExplorer(repo, appState.Logger, appState.Modules, traverser.NewMetrics(appState.Metrics))
	schemaRepo, err = schemarepo.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
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

	// SchemaManager is not set at that point, so it is passed as a callback
	shardingStateFunc := func(className string) *sharding.State {
		return appState.SchemaManager.ShardingState(className)
	}
	snapshotterProvider := newSnapshotterProvider(repo)
	backupManager := backup.NewBackupManager(appState.Logger, snapshotterProvider, appState.Modules, shardingStateFunc)

	// TODO: configure http transport for efficient intra-cluster comm
	classificationsTxClient := clients.NewClusterClassifications(clusterHttpClient)
	classifierRepo := classifications.NewDistributeRepo(classificationsTxClient,
		appState.Cluster, localClassifierRepo)
	appState.ClassificationRepo = classifierRepo

	// TODO: configure http transport for efficient intra-cluster comm
	schemaTxClient := clients.NewClusterSchema(clusterHttpClient)
	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		hnsw.ParseUserConfig, appState.Modules, inverted.ValidateConfig, appState.Modules, appState.Cluster,
		schemaTxClient, backupManager)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}
	appState.SchemaManager = schemaManager

	appState.RemoteIncoming = sharding.NewRemoteIndexIncoming(repo)

	go clusterapi.Serve(appState)

	vectorRepo.SetSchemaGetter(schemaManager)
	explorer.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	err = vectorRepo.WaitForStartup(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").WithError(err).
			Fatal("db didn't start up")
		os.Exit(1)
	}

	objectsManager := objects.NewManager(appState.Locks,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Modules, vectorRepo, appState.Modules,
		objects.NewMetrics(appState.Metrics))
	batchObjectsManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics)

	objectsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics))

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer,
		appState.Logger, appState.Modules)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, objectsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	setupSchemaHandlers(api, schemaManager)
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger, appState.Modules)
	setupObjectBatchHandlers(api, batchObjectsManager)
	setupGraphQLHandlers(api, appState)
	setupMiscHandlers(api, appState.ServerConfig, schemaManager, appState.Modules)
	setupClassificationHandlers(api, classifier)

	api.ServerShutdown = func() {
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
	appState.AnonymousAccess = configureAnonymousAccess(appState)
	appState.Authorizer = configureAuthorizer(appState)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	appState.Locks = &dummyLock{}

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	clusterState, err := cluster.Init(serverConfig.Config.Cluster, logger)
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

	if _, ok := enabledModules["qna-transformers"]; ok {
		appState.Modules.Register(modqna.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", "qna-transformers").
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
