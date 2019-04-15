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
 */package restapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	dblisting "github.com/creativesoftwarefdn/weaviate/database/listing"
	etcdSchemaManager "github.com/creativesoftwarefdn/weaviate/database/schema_manager/etcd"
	"github.com/creativesoftwarefdn/weaviate/kinds"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/creativesoftwarefdn/weaviate/restapi/batch"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	schemaUC "github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
)

var mainLog *telemetry.RequestsLog
var reporter *telemetry.Reporter

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	startupRoutine()

	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = func(token string, scopes []string) (*models.Principal, error) {
		return appState.OIDC.ValidateAndExtract(token, scopes)
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
func startupRoutine() {
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

	// Create message service
	messaging = &messages.Messaging{}
	appState.Messaging = messaging

	messaging.InfoMessage(fmt.Sprintf("created the context, nothing done yet, time left is: %s", timeTillDeadline(ctx)))

	// Load the config using the flags
	serverConfig = &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err := serverConfig.LoadConfig(connectorOptionGroup, messaging)
	if err != nil {
		messaging.ExitError(1, "could not load config: "+err.Error())
	}
	messaging.InfoMessage(fmt.Sprintf("loaded the config, time left is: %s", timeTillDeadline(ctx)))

	appState.OIDC = configureOIDC(appState)
	appState.AnonymousAccess = configureAnonymousAccess(appState)

	messaging.InfoMessage(fmt.Sprintf("configured OIDC client, time left is: %s", timeTillDeadline(ctx)))

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
		spew.Dump(appState.ServerConfig.Config.Network.PeerName)
		mainLog.PeerName = appState.ServerConfig.Config.Network.PeerName
	}
	mainLog.Debug = loggingDebug
	mainLog.Disabled = loggingDisabled

	loadContextionary()
	messaging.InfoMessage(fmt.Sprintf("loaded the contextionary, time left is: %s", timeTillDeadline(ctx)))

	connectToNetwork()
	messaging.InfoMessage(fmt.Sprintf("connected to network, time left is: %s", timeTillDeadline(ctx)))

	// Create the database connector using the config
	err, dbConnector := dblisting.NewConnector(serverConfig.Config.Database.Name, serverConfig.Config.Database.DatabaseConfig, serverConfig.Config)
	// Could not find, or configure connector.
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	messaging.InfoMessage(fmt.Sprintf("created db connector, time left is: %s", timeTillDeadline(ctx)))

	// parse config store URL
	configURL := serverConfig.Config.ConfigurationStorage.URL
	configStore, err := url.Parse(configURL)
	if err != nil || configURL == "" {
		messaging.ExitError(78, fmt.Sprintf("cannot parse config store URL: %s", err))
	}

	// Construct a distributed lock
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{configStore.String()}})
	if err != nil {
		log.Fatal(err)
	}

	messaging.InfoMessage(fmt.Sprintf("created an etcd client, time left is: %s", timeTillDeadline(ctx)))

	s1, err := concurrency.NewSession(etcdClient)
	if err != nil {
		log.Fatal(err)
	}
	messaging.InfoMessage(fmt.Sprintf("created an etcd session, time left is: %s", timeTillDeadline(ctx)))

	manager, err := etcdSchemaManager.New(ctx, etcdClient, dbConnector, network)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not initialize local database state: %v", err))
	}

	messaging.InfoMessage(fmt.Sprintf("initialized the schema, time left is: %s", timeTillDeadline(ctx)))

	manager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// Initialize a non-expiring context for the reporter
	reportingContext := context.Background()
	// Initialize the reporter
	reporter = telemetry.NewReporter(reportingContext, mainLog, loggingInterval, loggingURL, loggingDisabled, loggingDebug, etcdClient, messaging)

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
		Messaging:     messaging,
	}
	db, err = database.New(ctx, dbParams)
	if err != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not initialize the database: %s", err.Error()))
	}
	appState.Database = db

	manager.TriggerSchemaUpdateCallbacks()
	network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		manager.TriggerSchemaUpdateCallbacks()
	})

	network.RegisterSchemaGetter(&schemaGetter{db: db})

}
