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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/fetch"
	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/network"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/state"
	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	databaseSchema "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	schemaContextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/usecases/auth/authentication/anonymous"
	"github.com/creativesoftwarefdn/weaviate/usecases/auth/authentication/oidc"
	libnetwork "github.com/creativesoftwarefdn/weaviate/usecases/network"
	libnetworkFake "github.com/creativesoftwarefdn/weaviate/usecases/network/fake"
	libnetworkP2P "github.com/creativesoftwarefdn/weaviate/usecases/network/p2p"
	"github.com/creativesoftwarefdn/weaviate/usecases/telemetry"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
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

func makeUpdateSchemaCallBackWithLogger(logger logrus.FieldLogger) func(schema.Schema) {
	return func(updatedSchema schema.Schema) {
		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.
		if err := rebuildContextionary(updatedSchema, logger); err != nil {
			logger.WithField("action", "contextionary_rebuild").
				WithError(err).Fatal("could not (re)build contextionary")
		}

		if err := rebuildGraphQL(updatedSchema, logger); err != nil {
			logger.WithField("action", "graphql_rebuild").
				WithError(err).Error("could not (re)build graphql provider")
		}
	}
}

func rebuildContextionary(updatedSchema schema.Schema, logger logrus.FieldLogger) error {
	// build new contextionary extended by the local schema
	schemaContextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(updatedSchema, &rawContextionary)
	if err != nil {
		return fmt.Errorf("Could not build in-memory contextionary from schema; %v", err)
	}

	// Combine contextionaries
	contextionaries := []libcontextionary.Contextionary{rawContextionary, *schemaContextionary}
	combined, err := libcontextionary.CombineVectorIndices(contextionaries)

	if err != nil {
		return fmt.Errorf("Could not combine the contextionary database with the in-memory generated contextionary; %v", err)
	}

	logger.WithField("action", "contextionary_rebuild").Debug("contextionary extended with new schema")

	contextionary = libcontextionary.Contextionary(combined)
	return nil
}

func rebuildGraphQL(updatedSchema schema.Schema, logger logrus.FieldLogger) error {
	peers, err := network.ListPeers()
	if err != nil {
		graphQL = nil
		return fmt.Errorf("could not list network peers to regenerate schema: %v", err)
	}

	c11y := schemaContextionary.New(contextionary)
	root := graphQLRoot{Database: db, Network: network, contextionary: c11y, log: mainLog}
	updatedGraphQL, err := graphql.Build(&updatedSchema, peers, root, logger, serverConfig.Config)
	if err != nil {
		graphQL = nil
		return fmt.Errorf("Could not re-generate GraphQL schema, because: %v", err)
	}

	graphQL = updatedGraphQL
	logger.WithField("action", "graphql_rebuild").Debug("successfully rebuild graphql schema")
	return nil
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
		appState.Logger.WithField("action", "oidc_init").WithError(err).Fatal("oidc client could not start up")
		os.Exit(1)
	}

	return c
}

// configureAnonymousAccess will always be called, even if anonymous access is
// disabled. In this case the middleware provided by this client will block
// anonymous requests
func configureAnonymousAccess(appState *state.State) *anonymous.Client {
	return anonymous.New(appState.ServerConfig.Config)
}

func timeTillDeadline(ctx context.Context) string {
	dl, _ := ctx.Deadline()
	return fmt.Sprintf("%s", time.Until(dl))
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary(logger *logrus.Logger) {
	// First load the file backed contextionary
	if serverConfig.Config.Contextionary.KNNFile == "" {
		logger.WithField("action", "startup").Error("contextionary KNN file not set")
		logger.Exit(1)
	}

	if serverConfig.Config.Contextionary.IDXFile == "" {
		logger.WithField("action", "startup").Error("contextionary IDX file not set")
		logger.Exit(1)
	}

	mmapedContextionary, err := libcontextionary.LoadVectorFromDisk(
		serverConfig.Config.Contextionary.KNNFile, serverConfig.Config.Contextionary.IDXFile)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).
			Error("could not load contextionary")
		logger.Exit(1)
	}

	logger.Debug("contextionary loaded")
	rawContextionary = mmapedContextionary
}

func connectToNetwork(logger *logrus.Logger) {
	if serverConfig.Config.Network == nil {
		logger.Info("No network configured. Not Joining one.")
		network = libnetworkFake.FakeNetwork{}
		appState.Network = network
	} else {
		genesis_url := strfmt.URI(serverConfig.Config.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Config.Network.PublicURL)
		peer_name := serverConfig.Config.Network.PeerName

		logger.
			WithField("peer_name", peer_name).
			WithField("genesis_url", genesis_url).
			Info("Network configured. Attempting to join.")
		new_net, err := libnetworkP2P.BootstrapNetwork(logger, genesis_url, public_url, peer_name)
		if err != nil {
			logger.WithField("action", "startup").
				WithError(err).
				Error("could not connect to network")
			logger.Exit(1)
		} else {
			network = new_net
			appState.Network = new_net
		}
	}
}
