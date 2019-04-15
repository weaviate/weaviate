/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package rest

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/batch"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/state"
	"github.com/creativesoftwarefdn/weaviate/database"
	dblisting "github.com/creativesoftwarefdn/weaviate/database/listing"
	etcdSchemaManager "github.com/creativesoftwarefdn/weaviate/database/schema_manager/etcd"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	schemaUC "github.com/creativesoftwarefdn/weaviate/usecases/schema"
	"github.com/creativesoftwarefdn/weaviate/usecases/telemetry"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
)

var mainLog *telemetry.RequestsLog
var reporter *telemetry.Reporter

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	appState := startupRoutine()

	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = func(token string, scopes []string) (*models.Principal, error) {
		return appState.OIDC.ValidateAndExtract(token, scopes)
	}

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithField("action", "restapi_management").Infof(msg, args...)
	}

	kindsManager := kinds.NewManager(db, network, serverConfig)

	setupSchemaHandlers(api, mainLog, schemaUC.NewManager(db))
	setupKindHandlers(api, mainLog, kindsManager)
	setupBatchHandlers(api, mainLog)
	setupC11yHandlers(api, mainLog)
	setupGraphQLHandlers(api, mainLog)
	setupMiscHandlers(api, mainLog)

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func setupBatchHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	batchAPI := batch.New(appState, requestsLog)

	api.WeaviateBatchingThingsCreateHandler = operations.
		WeaviateBatchingThingsCreateHandlerFunc(batchAPI.ThingsCreate)
	api.WeaviateBatchingActionsCreateHandler = operations.
		WeaviateBatchingActionsCreateHandlerFunc(batchAPI.ActionsCreate)
	api.WeaviateBatchingReferencesCreateHandler = operations.
		WeaviateBatchingReferencesCreateHandlerFunc(batchAPI.References)
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine() *state.State {
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

	logger := logrus.New()
	// logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.DebugLevel)
	appState.Logger = logger

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created startup context, nothing done so far")

	// Load the config using the flags
	serverConfig = &config.WeaviateConfig{}
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

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	// Extract environment variables needed for logging
	mainLog = telemetry.NewLog()
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

	loadContextionary(logger)
	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("contextionary loaded")

	connectToNetwork(logger)
	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("network configured")

	// Create the database connector using the config
	err, dbConnector := dblisting.NewConnector(serverConfig.Config.Database.Name, serverConfig.Config.Database.DatabaseConfig, serverConfig.Config)
	// Could not find, or configure connector.
	if err != nil {
		logger.WithField("action", "startup").WithError(err).Error("could not load config")
		logger.Exit(1)
	}

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

	s1, err := concurrency.NewSession(etcdClient)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Error("cannot create etcd session")
		logger.Exit(1)
	}

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created etcd session")

	manager, err := etcdSchemaManager.New(ctx, etcdClient, dbConnector, network)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).Error("cannot (etcd) schema manager and initialize schema")
		logger.Exit(1)
	}

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	updateSchemaCallback := makeUpdateSchemaCallBackWithLogger(logger)
	manager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// Initialize a non-expiring context for the reporter
	reportingContext := context.Background()
	// Initialize the reporter
	reporter = telemetry.NewReporter(reportingContext, mainLog, loggingInterval, loggingURL, loggingDisabled, loggingDebug, etcdClient, logger)

	// Start reporting
	go func() {
		reporter.Start()
	}()

	// initialize the contextinoary with the rawContextionary, it will get updated on each schema update
	contextionary = rawContextionary

	// Now instantiate a database, with the configured lock, manager and connector.
	dbParams := &database.Params{
		LockerKey:     "/weaviate/schema-connector-rw-lock",
		LockerSession: s1,
		SchemaManager: manager,
		Connector:     dbConnector,
		Contextionary: contextionary,
		Logger:        logger,
	}
	db, err = database.New(ctx, dbParams)
	if err != nil {
		logger.
			WithField("action", "startup").
			WithField("params", dbParams).
			WithError(err).
			Error("cannot initialize connected db")
		logger.Exit(1)
	}
	appState.Database = db

	manager.TriggerSchemaUpdateCallbacks()
	network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		manager.TriggerSchemaUpdateCallbacks()
	})

	network.RegisterSchemaGetter(&schemaGetter{db: db})

	return appState
}
