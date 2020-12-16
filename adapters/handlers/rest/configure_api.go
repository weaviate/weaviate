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
	"time"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/clients/contextionary"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/repos/classifications"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	modulestorage "github.com/semi-technologies/weaviate/adapters/repos/modules"
	schemarepo "github.com/semi-technologies/weaviate/adapters/repos/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	modcontextionary "github.com/semi-technologies/weaviate/modules/text2vec-contextionary"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/nearestneighbors"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/projector"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"

	_ "net/http/pprof"
)

const MinimumRequiredContextionaryVersion = "0.4.21"

func makeConfigureServer(appState *state.State) func(*http.Server, string, string) {
	return func(s *http.Server, scheme, addr string) {
		// Add properties to the config
		appState.ServerConfig.Hostname = addr
		appState.ServerConfig.Scheme = scheme
	}
}

type vectorRepo interface {
	kinds.BatchVectorRepo
	traverser.VectorSearcher
	classification.VectorRepo
	SetSchemaGetter(schemaUC.SchemaGetter)
	WaitForStartup(time.Duration) error
}

type vectorizer interface {
	kinds.Vectorizer
	traverser.CorpiVectorizer
	SetIndexChecker(libvectorizer.IndexCheck)
}

type explorer interface {
	GetClass(ctx context.Context, params traverser.GetParams) ([]interface{}, error)
	Concepts(ctx context.Context, params traverser.ExploreParams) ([]search.Result, error)
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	appState := startupRoutine()

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
	var vectorizer vectorizer
	var migrator migrate.Migrator
	var explorer explorer
	nnExtender := nearestneighbors.NewExtender(appState.Contextionary)
	featureProjector := projector.New()
	pathBuilder := sempath.New(appState.Contextionary)
	var schemaRepo schemaUC.Repo
	var classifierRepo classification.Repo

	if appState.ServerConfig.Config.Standalone {
		repo := db.New(appState.Logger, db.Config{
			RootPath: appState.ServerConfig.Config.Persistence.DataPath,
		})
		vectorMigrator = db.NewMigrator(repo, appState.Logger)
		vectorRepo = repo
		migrator = vectorMigrator
		vectorizer = libvectorizer.New(appState.Contextionary, nil)
		explorer = traverser.NewExplorer(repo, vectorizer, libvectorizer.NormalizedDistance,
			appState.Logger, nnExtender, featureProjector, pathBuilder)
		var err error
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
	} else {
		panic("tried to start up with non standalone mode")
	}

	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Locks, appState.Network, appState.Logger, appState.Contextionary,
		appState.Authorizer, appState.StopwordDetector)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}

	vectorRepo.SetSchemaGetter(schemaManager)
	vectorizer.SetIndexChecker(schemaManager)

	err = vectorRepo.WaitForStartup(2 * time.Minute)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "startup").WithError(err).
			Fatal("db didn't start up")
		os.Exit(1)
	}

	kindsManager := kinds.NewManager(appState.Locks,
		schemaManager, appState.Network, appState.ServerConfig, appState.Logger,
		appState.Authorizer, vectorizer, vectorRepo, nnExtender, featureProjector)
	batchKindsManager := kinds.NewBatchManager(vectorRepo, vectorizer, appState.Locks,
		schemaManager, appState.Network, appState.ServerConfig, appState.Logger,
		appState.Authorizer)
	vectorInspector := libvectorizer.NewInspector(appState.Contextionary)

	kindsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorizer,
		vectorRepo, explorer, schemaManager)

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer,
		appState.Contextionary, appState.Logger)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, kindsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// manually update schema once
	schema := schemaManager.GetSchemaSkipAuth()
	updateSchemaCallback(schema)

	appState.Network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		schemaManager.TriggerSchemaUpdateCallbacks()
	})
	appState.Network.RegisterSchemaGetter(schemaManager)

	setupSchemaHandlers(api, schemaManager)
	setupKindHandlers(api, kindsManager, appState.ServerConfig.Config, appState.Logger)
	setupKindBatchHandlers(api, batchKindsManager)
	setupC11yHandlers(api, vectorInspector, appState.Contextionary)
	setupGraphQLHandlers(api, appState)
	setupMiscHandlers(api, appState.ServerConfig, appState.Network, schemaManager, appState.Contextionary)
	setupClassificationHandlers(api, classifier)

	api.ServerShutdown = func() {}
	configureServer = makeConfigureServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)

	registerModules(appState)

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine() *state.State {
	appState := &state.State{}
	// context for the startup procedure. (So far the only subcommand respecting
	// the context is the schema initialization, as this uses the etcd client
	// requiring context. Nevertheless it would make sense to have everything
	// that goes on in here pay attention to the context, so we can have a
	// "startup in x seconds or fail")
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

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("config loaded")

	appState.OIDC = configureOIDC(appState)
	appState.AnonymousAccess = configureAnonymousAccess(appState)
	appState.Authorizer = configureAuthorizer(appState)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	appState.Network = connectToNetwork(logger, appState.ServerConfig.Config)
	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("network configured")

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created db connector")

	if appState.ServerConfig.Config.Standalone {
		appState.Locks = &dummyLock{}
	} else {
		panic("tried to start up with non standalone mode")
	}

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created etcd session")
		// END remove

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized stopword detector")

	c11y, err := contextionary.NewClient(appState.ServerConfig.Config.Contextionary.URL)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Fatal("cannot create c11y client")
	}

	appState.StopwordDetector = c11y
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
	storageProvider, err := modulestorage.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		return errors.Wrap(err, "init storage provider")
	}

	appState.Modules = modules.NewProvider()
	appState.Modules.Register(modcontextionary.New(storageProvider))

	err = appState.Modules.Init()
	if err != nil {
		return errors.Wrap(err, "init modules")
	}

	return nil
}
