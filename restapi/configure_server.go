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
	"github.com/creativesoftwarefdn/weaviate/auth/authentication/anonymous"
	"github.com/creativesoftwarefdn/weaviate/auth/authentication/oidc"
	weaviateBroker "github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database"
	dblisting "github.com/creativesoftwarefdn/weaviate/database/listing"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	databaseSchema "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	schemaContextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	etcdSchemaManager "github.com/creativesoftwarefdn/weaviate/database/schema_manager/etcd"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	"github.com/creativesoftwarefdn/weaviate/messages"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	libnetworkFake "github.com/creativesoftwarefdn/weaviate/network/fake"
	libnetworkP2P "github.com/creativesoftwarefdn/weaviate/network/p2p"
	"github.com/creativesoftwarefdn/weaviate/restapi/state"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/go-openapi/strfmt"
)

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
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
	messaging.InfoMessage(fmt.Sprintf("loaded the config, time left is: %s", timeTillDeadline(ctx)))

	appState.OIDC = configureOIDC(appState)
	appState.AnonymousAccess = configureAnonymousAccess(appState)

	messaging.InfoMessage(fmt.Sprintf("configured OIDC client, time left is: %s", timeTillDeadline(ctx)))

	// Extract environment variables needed for logging
	loggingInterval := appState.ServerConfig.Environment.Telemetry.Interval
	loggingUrl := appState.ServerConfig.Environment.Telemetry.RemoteURL
	loggingDisabled := appState.ServerConfig.Environment.Telemetry.Disabled
	loggingDebug := appState.ServerConfig.Environment.Debug

	if loggingUrl == "" {
		loggingUrl = telemetry.DefaultURL
	}

	if loggingInterval == 0 {
		loggingInterval = telemetry.DefaultInterval
	}

	// Propagate the peer name (if any), debug toggle and the enabled toggle to the requestsLog
	mainLog.PeerName = appState.ServerConfig.Environment.Network.PeerName
	mainLog.Debug = loggingDebug
	mainLog.Disabled = loggingDisabled

	// Add properties to the config
	serverConfig.Hostname = addr
	serverConfig.Scheme = scheme

	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	loadContextionary()
	messaging.InfoMessage(fmt.Sprintf("loaded the contextionary, time left is: %s", timeTillDeadline(ctx)))

	connectToNetwork()
	messaging.InfoMessage(fmt.Sprintf("connected to network, time left is: %s", timeTillDeadline(ctx)))

	// Connect to MQTT via Broker
	weaviateBroker.ConnectToMqtt(serverConfig.Environment.Broker.Host, serverConfig.Environment.Broker.Port)
	messaging.InfoMessage(fmt.Sprintf("connected to broker, time left is: %s", timeTillDeadline(ctx)))

	// Create the database connector using the config
	err, dbConnector := dblisting.NewConnector(serverConfig.Environment.Database.Name, serverConfig.Environment.Database.DatabaseConfig, serverConfig.Environment)
	// Could not find, or configure connector.
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	messaging.InfoMessage(fmt.Sprintf("created db connector, time left is: %s", timeTillDeadline(ctx)))

	// parse config store URL
	configURL := serverConfig.Environment.ConfigurationStorage.URL
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
	reporter = telemetry.NewReporter(reportingContext, mainLog, loggingInterval, loggingUrl, loggingDisabled, loggingDebug, etcdClient, messaging)

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

func updateSchemaCallback(updatedSchema schema.Schema) {
	// Note that this is thread safe; we're running in a single go-routine, because the event
	// handlers are called when the SchemaLock is still held.
	rebuildContextionary(updatedSchema)
	rebuildGraphQL(updatedSchema)
}

func rebuildContextionary(updatedSchema schema.Schema) {
	// build new contextionary extended by the local schema
	schemaContextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(updatedSchema, &rawContextionary)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not build in-memory contextionary from schema; %+v", err))
	}

	// Combine contextionaries
	contextionaries := []libcontextionary.Contextionary{rawContextionary, *schemaContextionary}
	combined, err := libcontextionary.CombineVectorIndices(contextionaries)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not combine the contextionary database with the in-memory generated contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary extended with names in the schema")

	contextionary = libcontextionary.Contextionary(combined)
}

func rebuildGraphQL(updatedSchema schema.Schema) {
	peers, err := network.ListPeers()
	if err != nil {
		graphQL = nil
		messaging.ErrorMessage(fmt.Sprintf("could not list network peers to regenerate schema:\n%#v\n", err))
		return
	}

	c11y := schemaContextionary.New(contextionary)
	root := graphQLRoot{Database: db, Network: network, contextionary: c11y, log: mainLog}
	updatedGraphQL, err := graphqlapi.Build(&updatedSchema, peers, root, messaging, serverConfig.Environment)
	if err != nil {
		// TODO: turn on safe mode gh-520
		graphQL = nil
		messaging.ErrorMessage(fmt.Sprintf("Could not re-generate GraphQL schema, because:\n%#v\n", err))
	} else {
		messaging.InfoMessage("Updated GraphQL schema")
		graphQL = updatedGraphQL
	}
}

type schemaGetter struct {
	db database.Database
}

func (s *schemaGetter) Schema() (schema.Schema, error) {
	dbLock, err := s.db.ConnectorLock()
	if err != nil {
		return schema.Schema{}, err
	}

	defer dbLock.Unlock()
	return dbLock.GetSchema(), nil
}

type graphQLRoot struct {
	database.Database
	libnetwork.Network
	contextionary *schemaContextionary.Contextionary
	log           *telemetry.RequestsLog
}

func (r graphQLRoot) GetNetworkResolver() graphqlnetwork.Resolver {
	return r.Network
}

func (r graphQLRoot) GetContextionary() fetch.Contextionary {
	return r.contextionary
}

func (r graphQLRoot) GetRequestsLog() fetch.RequestsLog {
	return r.log
}

// configureOIDC will always be called, even if OIDC is disabled, this way the
// middleware will still be able to provide the user with a valuable error
// message, even when OIDC is globally disabled.
func configureOIDC(appState *state.State) *oidc.Client {
	c, err := oidc.New(appState.ServerConfig.Environment)
	if err != nil {
		appState.Messaging.ExitError(1, fmt.Sprintf("oidc client couldn't start up: %v", err))
	}

	return c
}

// configureAnonymousAccess will always be called, even if anonymous access is
// disabled. In this case the middleware provided by this client will block
// anonymous requests
func configureAnonymousAccess(appState *state.State) *anonymous.Client {
	return anonymous.New(appState.ServerConfig.Environment)
}

func timeTillDeadline(ctx context.Context) time.Duration {
	dl, _ := ctx.Deadline()
	return time.Until(dl)
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary() {
	// First load the file backed contextionary
	if serverConfig.Environment.Contextionary.KNNFile == "" {
		messaging.ExitError(78, "Contextionary KNN file not specified")
	}

	if serverConfig.Environment.Contextionary.IDXFile == "" {
		messaging.ExitError(78, "Contextionary IDX file not specified")
	}

	mmapedContextionary, err := libcontextionary.LoadVectorFromDisk(serverConfig.Environment.Contextionary.KNNFile, serverConfig.Environment.Contextionary.IDXFile)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not load Contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary loaded from disk")

	rawContextionary = mmapedContextionary
}

func connectToNetwork() {
	if serverConfig.Environment.Network == nil {
		messaging.InfoMessage(fmt.Sprintf("No network configured, not joining one"))
		network = libnetworkFake.FakeNetwork{}
		appState.Network = network
	} else {
		genesis_url := strfmt.URI(serverConfig.Environment.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Environment.Network.PublicURL)
		peer_name := serverConfig.Environment.Network.PeerName

		messaging.InfoMessage(fmt.Sprintf("Network configured, connecting to Genesis '%v'", genesis_url))
		new_net, err := libnetworkP2P.BootstrapNetwork(messaging, genesis_url, public_url, peer_name)
		if err != nil {
			messaging.ExitError(78, fmt.Sprintf("Could not connect to network! Reason: %+v", err))
		} else {
			network = *new_net
			appState.Network = *new_net
		}
	}
}
