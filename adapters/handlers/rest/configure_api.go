//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	_ "net/http/pprof"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/repos/classifications"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	modulestorage "github.com/semi-technologies/weaviate/adapters/repos/modules"
	schemarepo "github.com/semi-technologies/weaviate/adapters/repos/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
	modimage "github.com/semi-technologies/weaviate/modules/img2vec-neural"
	modqna "github.com/semi-technologies/weaviate/modules/qna-transformers"
	modcontextionary "github.com/semi-technologies/weaviate/modules/text2vec-contextionary"
	modtransformers "github.com/semi-technologies/weaviate/modules/text2vec-transformers"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/objects"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
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
	Concepts(ctx context.Context, params traverser.ExploreParams) ([]search.Result, error)
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	appState := startupRoutine(ctx)

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

	var vectorRepo vectorRepo
	var vectorMigrator migrate.Migrator
	var migrator migrate.Migrator
	var explorer explorer
	var schemaRepo schemaUC.Repo
	var classifierRepo classification.Repo

	repo := db.New(appState.Logger, db.Config{
		RootPath: appState.ServerConfig.Config.Persistence.DataPath,
	})
	vectorMigrator = db.NewMigrator(repo, appState.Logger)
	vectorRepo = repo
	migrator = vectorMigrator
	explorer = traverser.NewExplorer(repo, libvectorizer.NormalizedDistance,
		appState.Logger, appState.Modules)
	schemaRepo, err = schemarepo.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema repo")
		os.Exit(1)
	}

	classifierRepo, err = classifications.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize classifications repo")
		os.Exit(1)
	}

	schemaTxClient := clients.NewClusterSchema(&http.Client{})
	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		hnsw.ParseUserConfig, appState.Modules, appState.Modules, appState.Cluster,
		schemaTxClient)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}
	appState.SchemaManager = schemaManager

	go clusterapi.Serve(appState)

	vectorRepo.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	err = vectorRepo.WaitForStartup(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").WithError(err).
			Fatal("db didn't start up")
		os.Exit(1)
	}

	kindsManager := objects.NewManager(appState.Locks,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Modules, vectorRepo, appState.Modules)
	batchKindsManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer)

	kindsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager)

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer,
		appState.Logger, appState.Modules)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, kindsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	setupSchemaHandlers(api, schemaManager)
	setupKindHandlers(api, kindsManager, appState.ServerConfig.Config, appState.Logger, appState.Modules)
	setupKindBatchHandlers(api, batchKindsManager)
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
	}

	if _, ok := enabledModules["text2vec-transformers"]; ok {
		appState.Modules.Register(modtransformers.New())
	}

	if _, ok := enabledModules["qna-transformers"]; ok {
		appState.Modules.Register(modqna.New())
	}

	if _, ok := enabledModules["img2vec-neural"]; ok {
		appState.Modules.Register(modimage.New())
	}

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

	if err := appState.Modules.Init(ctx, moduleParams); err != nil {
		return errors.Wrap(err, "init modules")
	}

	return nil
}
