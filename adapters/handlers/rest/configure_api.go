//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/clients/contextionary"
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
	modcontextionary "github.com/semi-technologies/weaviate/modules/text2vec-contextionary"
	modtransformers "github.com/semi-technologies/weaviate/modules/text2vec-transformers"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/nearestneighbors"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/projector"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"

	_ "net/http/pprof"
)

const MinimumRequiredContextionaryVersion = "1.0.0"

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
	WaitForStartup(time.Duration) error
}

type explorer interface {
	GetClass(ctx context.Context, params traverser.GetParams) ([]interface{}, error)
	Concepts(ctx context.Context, params traverser.ExploreParams) ([]search.Result, error)
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	appState := startupRoutine()

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

	validateContextionaryVersion(appState)

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
	nnExtender := nearestneighbors.NewExtender(appState.Contextionary)
	featureProjector := projector.New()
	pathBuilder := sempath.New(appState.Contextionary)
	var schemaRepo schemaUC.Repo
	var classifierRepo classification.Repo

	repo := db.New(appState.Logger, db.Config{
		RootPath: appState.ServerConfig.Config.Persistence.DataPath,
	})
	vectorMigrator = db.NewMigrator(repo, appState.Logger)
	vectorRepo = repo
	migrator = vectorMigrator
	explorer = traverser.NewExplorer(repo, libvectorizer.NormalizedDistance,
		appState.Logger, nnExtender, featureProjector, pathBuilder, appState.Modules)
	schemaRepo, err = schemarepo.NewRepo("./data", appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema repo")
		os.Exit(1)
	}

	classifierRepo, err = classifications.NewRepo("./data", appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize classifications repo")
		os.Exit(1)
	}

	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Logger, appState.Authorizer, appState.ServerConfig.Config,
		hnsw.ParseUserConfig, appState.Modules, appState.Modules)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}
	appState.SchemaManager = schemaManager

	vectorRepo.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	err = vectorRepo.WaitForStartup(2 * time.Minute)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").WithError(err).
			Fatal("db didn't start up")
		os.Exit(1)
	}

	kindsManager := objects.NewManager(appState.Locks,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Modules, vectorRepo, nnExtender, featureProjector)
	batchKindsManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		appState.Locks, schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer)

	kindsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager)

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer,
		appState.Contextionary, appState.Logger)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, kindsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// manually update schema once
	schema := schemaManager.GetSchemaSkipAuth()
	updateSchemaCallback(schema)

	setupSchemaHandlers(api, schemaManager)
	setupKindHandlers(api, kindsManager, appState.ServerConfig.Config, appState.Logger)
	setupKindBatchHandlers(api, batchKindsManager)
	setupGraphQLHandlers(api, appState)
	setupMiscHandlers(api, appState.ServerConfig, schemaManager, appState.Contextionary)
	setupClassificationHandlers(api, classifier)

	api.ServerShutdown = func() {}
	configureServer = makeConfigureServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)

	err = initModules(appState)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't initialize")
	}
	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine() *state.State {
	appState := &state.State{}
	// context for the startup procedure. (So far the only subcommand respecting
	// the context is the schema initialization. Nevertheless it would make sense
	// to have everything that goes on in here pay attention to the context, so
	// we can have a "startup in x seconds or fail")
	ctx := context.Background()
	// The timeout is arbitrary we have to adjust it as we go along, if we
	// realize it is to big/small
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

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

	c11y, err := contextionary.NewClient(appState.ServerConfig.Config.Contextionary.URL)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Fatal("cannot create c11y client")
	}

	appState.Contextionary = c11y

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

// TODO: This should move into the text2vec-contextionary code once we deal
// with modularization
func validateContextionaryVersion(appState *state.State) {
	for {
		time.Sleep(1 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		v, err := appState.Contextionary.Version(ctx)
		if err != nil {
			appState.Logger.WithField("action", "startup_check_contextionary").WithError(err).
				Warnf("could not connect to contextionary at startup, trying again in 1 sec")
			continue
		}

		ok, err := extractVersionAndCompare(v, MinimumRequiredContextionaryVersion)
		if err != nil {
			appState.Logger.WithField("action", "startup_check_contextionary").
				WithField("requiredMinimumContextionaryVersion", MinimumRequiredContextionaryVersion).
				WithField("contextionaryVersion", v).
				WithError(err).
				Warnf("cannot determine if contextionary version is compatible. This is fine in development, but probelematic if you see this production")
			break
		}

		if ok {
			appState.Logger.WithField("action", "startup_check_contextionary").
				WithField("requiredMinimumContextionaryVersion", MinimumRequiredContextionaryVersion).
				WithField("contextionaryVersion", v).
				Infof("found a valid contextionary version")
			break
		} else {
			appState.Logger.WithField("action", "startup_check_contextionary").
				WithField("requiredMinimumContextionaryVersion", MinimumRequiredContextionaryVersion).
				WithField("contextionaryVersion", v).
				Fatalf("insufficient contextionary version, cannot start up")
			break
		}
	}
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

	return nil
}

func initModules(appState *state.State) error {
	storageProvider, err := modulestorage.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		return errors.Wrap(err, "init storage provider")
	}

	// TODO: don't pass entire appState in, but only what's needed. Probably only
	// config?
	moduleParams := moduletools.NewInitParams(storageProvider, appState)

	if err := appState.Modules.Init(moduleParams); err != nil {
		return errors.Wrap(err, "init modules")
	}

	return nil
}
