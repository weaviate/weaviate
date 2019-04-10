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
	"net/http"
	"time"

	"github.com/creativesoftwarefdn/weaviate/auth/authentication/oidc"
	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	databaseSchema "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	schemaContextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
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
	// Add properties to the config
	serverConfig.Hostname = addr
	serverConfig.Scheme = scheme
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
	updatedGraphQL, err := graphqlapi.Build(&updatedSchema, peers, root, messaging, serverConfig.Config)
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
	c, err := oidc.New(appState.ServerConfig.Config)
	if err != nil {
		appState.Messaging.ExitError(1, fmt.Sprintf("oidc client couldn't start up: %v", err))
	}

	return c
}

func timeTillDeadline(ctx context.Context) time.Duration {
	dl, _ := ctx.Deadline()
	return time.Until(dl)
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary() {
	// First load the file backed contextionary
	if serverConfig.Config.Contextionary.KNNFile == "" {
		messaging.ExitError(78, "Contextionary KNN file not specified")
	}

	if serverConfig.Config.Contextionary.IDXFile == "" {
		messaging.ExitError(78, "Contextionary IDX file not specified")
	}

	mmapedContextionary, err := libcontextionary.LoadVectorFromDisk(serverConfig.Config.Contextionary.KNNFile, serverConfig.Config.Contextionary.IDXFile)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not load Contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary loaded from disk")

	rawContextionary = mmapedContextionary
}

func connectToNetwork() {
	if serverConfig.Config.Network == nil {
		messaging.InfoMessage(fmt.Sprintf("No network configured, not joining one"))
		network = libnetworkFake.FakeNetwork{}
		appState.Network = network
	} else {
		genesis_url := strfmt.URI(serverConfig.Config.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Config.Network.PublicURL)
		peer_name := serverConfig.Config.Network.PeerName

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
