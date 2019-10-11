//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/elastic/go-elasticsearch/v5"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/semi-technologies/weaviate/adapters/clients/contextionary"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/locks"
	"github.com/semi-technologies/weaviate/adapters/repos/esvector"
	"github.com/semi-technologies/weaviate/adapters/repos/etcd"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	libvectorizer "github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"
)

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
	InitCacheIndexing(int, time.Duration, time.Duration)
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
	appState, etcdClient, esClient := startupRoutine()

	api.ServeError = errors.ServeError

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

	repo := esvector.NewRepo(esClient, appState.Logger, nil,
		appState.ServerConfig.Config.VectorIndex.DenormalizationDepth)
	vectorMigrator = esvector.NewMigrator(repo)
	vectorRepo = repo
	migrator = vectorMigrator
	vectorizer = libvectorizer.New(appState.Contextionary, nil)
	explorer = traverser.NewExplorer(repo, vectorizer, libvectorizer.NormalizedDistance)

	schemaRepo := etcd.NewSchemaRepo(etcdClient)
	classifierRepo := etcd.NewClassificationRepo(etcdClient)

	schemaManager, err := schemaUC.NewManager(migrator, schemaRepo,
		appState.Locks, appState.Network, appState.Logger, appState.Contextionary, appState.Authorizer, appState.StopwordDetector)
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
			Fatal("esvector didn't start up")
		os.Exit(1)
	}
	vectorRepo.InitCacheIndexing(
		appState.ServerConfig.Config.VectorIndex.CacheCycleBulkSize,
		time.Duration(appState.ServerConfig.Config.VectorIndex.CacheCycleIdleWaitTime)*time.Millisecond,
		time.Duration(appState.ServerConfig.Config.VectorIndex.CacheCycleBusyWaitTime)*time.Millisecond,
	)

	kindsManager := kinds.NewManager(appState.Locks,
		schemaManager, appState.Network, appState.ServerConfig, appState.Logger,
		appState.Authorizer, vectorizer, vectorRepo)
	batchKindsManager := kinds.NewBatchManager(vectorRepo, vectorizer, appState.Locks,
		schemaManager, appState.Network, appState.ServerConfig, appState.Logger,
		appState.Authorizer)
	vectorInspector := libvectorizer.NewInspector(appState.Contextionary)

	kindsTraverser := traverser.NewTraverser(appState.ServerConfig, appState.Locks,
		appState.Logger, appState.Authorizer, vectorizer,
		vectorRepo, explorer, schemaManager)

	classifier := classification.New(schemaManager, classifierRepo, vectorRepo, appState.Authorizer)

	updateSchemaCallback := makeUpdateSchemaCall(appState.Logger, appState, kindsTraverser)
	schemaManager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// manually update schema once
	schema := schemaManager.GetSchemaSkipAuth()
	updateSchemaCallback(schema)

	appState.Network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		schemaManager.TriggerSchemaUpdateCallbacks()
	})
	appState.Network.RegisterSchemaGetter(schemaManager)

	setupSchemaHandlers(api, appState.TelemetryLogger, schemaManager)
	setupKindHandlers(api, appState.TelemetryLogger, kindsManager)
	setupKindBatchHandlers(api, appState.TelemetryLogger, batchKindsManager)
	setupC11yHandlers(api, appState.TelemetryLogger, vectorInspector)
	setupGraphQLHandlers(api, appState.TelemetryLogger, appState)
	setupMiscHandlers(api, appState.TelemetryLogger, appState.ServerConfig, appState.Network, schemaManager, appState.Contextionary)
	setupClassificationHandlers(api, appState.TelemetryLogger, classifier)

	api.ServerShutdown = func() {}
	configureServer = makeConfigureServer(appState)
	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState)
	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine() (*state.State, *clientv3.Client, *elasticsearch.Client) {
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

	// parse config store URL
	configURL := serverConfig.Config.ConfigurationStorage.URL
	configStore, err := url.Parse(configURL)
	if err != nil || configURL == "" {
		logger.WithField("action", "startup").WithField("url", configURL).
			WithError(err).Error("cannot parse config store URL")
		logger.Exit(1)
	}

	// Construct a distributed lock
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{configStore.String()}})
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Error("cannot construct distributed lock with etcd")
		logger.Exit(1)
	}
	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created etcd client")

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{serverConfig.Config.VectorIndex.URL},
	})
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Error("cannot create es client for vector index")
		logger.Exit(1)
	}
	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created es client for vector index")

	appState.TelemetryLogger = configureTelemetry(appState, etcdClient, logger)

	// new lock
	etcdLock, err := locks.NewEtcdLock(etcdClient, "/weaviate/schema-connector-rw-lock", logger)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Error("cannot create etcd-based lock")
		logger.Exit(1)
	}
	appState.Locks = etcdLock

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
			WithError(err).Error("cannot create c11y client")
		logger.Exit(1)
	}

	appState.StopwordDetector = c11y
	appState.Contextionary = c11y

	return appState, etcdClient, esClient
}

func configureTelemetry(appState *state.State, etcdClient *clientv3.Client,
	logger logrus.FieldLogger) *telemetry.RequestsLog {
	// Extract environment variables needed for logging
	mainLog := telemetry.NewLog()
	loggingInterval := appState.ServerConfig.Config.Telemetry.Interval
	loggingURL := appState.ServerConfig.Config.Telemetry.RemoteURL
	loggingDisabled := appState.ServerConfig.Config.Telemetry.Disabled
	loggingDebug := appState.ServerConfig.Config.Debug

	if loggingURL == "" {
		loggingURL = telemetry.DefaultURL
	}

	if loggingInterval == 0 {
		loggingInterval = telemetry.DefaultInterval
	}

	// Propagate the peer name (if any), debug toggle and the enabled toggle to the requestsLog
	if appState.ServerConfig.Config.Network != nil {
		mainLog.PeerName = appState.ServerConfig.Config.Network.PeerName
	}
	mainLog.Debug = loggingDebug
	mainLog.Disabled = loggingDisabled

	// Initialize a non-expiring context for the reporter
	reportingContext := context.Background()
	// Initialize the reporter
	reporter := telemetry.NewReporter(reportingContext, mainLog, loggingInterval, loggingURL, loggingDisabled, loggingDebug, etcdClient, logger)

	// Start reporting
	go func() {
		reporter.Start()
	}()

	return mainLog
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
	if os.Getenv("LOG_LEVEL") == "debug" {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	return logger
}
