/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package rest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/contextionary"
	libcontextionary "github.com/semi-technologies/weaviate/contextionary"
	databaseSchema "github.com/semi-technologies/weaviate/contextionary/schema"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/network"
	libnetworkFake "github.com/semi-technologies/weaviate/usecases/network/fake"
	libnetworkP2P "github.com/semi-technologies/weaviate/usecases/network/p2p"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/sirupsen/logrus"
)

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
//
// we will set it through configureAPI() as it needs access to resources that
// are only available within there
var configureServer func(*http.Server, string, string)

func makeUpdateSchemaCall(logger logrus.FieldLogger, appState *state.State, traverser *kinds.Traverser) func(schema.Schema) {
	return func(updatedSchema schema.Schema) {
		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.
		c11y, err := rebuildContextionary(updatedSchema, logger, appState)
		if err != nil {
			logger.WithField("action", "contextionary_rebuild").
				WithError(err).Fatal("could not (re)build contextionary")
		}
		appState.Contextionary = c11y

		gql, err := rebuildGraphQL(
			updatedSchema,
			logger,
			appState.Network,
			appState.ServerConfig.Config,
			traverser,
			appState.TelemetryLogger,
		)
		if err != nil {
			logger.WithField("action", "graphql_rebuild").
				WithError(err).Error("could not (re)build graphql provider")
		}
		appState.GraphQL = gql
	}
}

func rebuildContextionary(updatedSchema schema.Schema, logger logrus.FieldLogger,
	appState *state.State) (contextionary.Contextionary, error) {
	// build new contextionary extended by the local schema
	schemaContextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(updatedSchema, &appState.RawContextionary, appState.StopwordDetector)
	if err != nil {
		return nil, fmt.Errorf("Could not build in-memory contextionary from schema; %v", err)
	}

	// Combine contextionaries
	contextionaries := []libcontextionary.Contextionary{appState.RawContextionary, *schemaContextionary}
	combined, err := libcontextionary.CombineVectorIndices(contextionaries)

	if err != nil {
		return nil, fmt.Errorf("Could not combine the contextionary database with the in-memory generated contextionary; %v", err)
	}

	logger.WithField("action", "contextionary_rebuild").Debug("contextionary extended with new schema")

	return libcontextionary.Contextionary(combined), nil
}

func rebuildGraphQL(updatedSchema schema.Schema, logger logrus.FieldLogger,
	network network.Network, config config.Config, traverser *kinds.Traverser,
	telemetryLogger *telemetry.RequestsLog) (graphql.GraphQL, error) {
	peers, err := network.ListPeers()
	if err != nil {
		return nil, fmt.Errorf("could not list network peers to regenerate schema: %v", err)
	}

	updatedGraphQL, err := graphql.Build(&updatedSchema, peers, traverser, network, telemetryLogger, logger, config)
	if err != nil {
		return nil, fmt.Errorf("Could not re-generate GraphQL schema, because: %v", err)
	}

	logger.WithField("action", "graphql_rebuild").Debug("successfully rebuild graphql schema")
	return updatedGraphQL, nil
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

func configureAuthorizer(appState *state.State) authorization.Authorizer {
	return authorization.New(appState.ServerConfig.Config)
}

func timeTillDeadline(ctx context.Context) string {
	dl, _ := ctx.Deadline()
	return fmt.Sprintf("%s", time.Until(dl))
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary(logger *logrus.Logger, config config.Config) contextionary.Contextionary {
	// First load the file backed contextionary
	if config.Contextionary.KNNFile == "" {
		logger.WithField("action", "startup").Error("contextionary KNN file not set")
		logger.Exit(1)
	}

	if config.Contextionary.IDXFile == "" {
		logger.WithField("action", "startup").Error("contextionary IDX file not set")
		logger.Exit(1)
	}

	if config.Contextionary.StopwordsFile == "" {
		logger.WithField("action", "startup").Error("stopwords file not set")
		logger.Exit(1)
	}

	mmapedContextionary, err := libcontextionary.LoadVectorFromDisk(
		config.Contextionary.KNNFile, config.Contextionary.IDXFile)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).
			Error("could not load contextionary")
		logger.Exit(1)
	}

	logger.Debug("contextionary loaded")
	return mmapedContextionary
}

func connectToNetwork(logger *logrus.Logger, config config.Config) network.Network {
	if config.Network == nil {
		logger.Info("No network configured. Not Joining one.")
		return libnetworkFake.FakeNetwork{}
	}

	genesisURL := strfmt.URI(config.Network.GenesisURL)
	publicURL := strfmt.URI(config.Network.PublicURL)
	peerName := config.Network.PeerName

	logger.
		WithField("peer_name", peerName).
		WithField("genesis_url", genesisURL).
		Info("Network configured. Attempting to join.")
	newnet, err := libnetworkP2P.BootstrapNetwork(logger, genesisURL, publicURL, peerName)
	if err != nil {
		logger.WithField("action", "startup").
			WithError(err).
			Error("could not connect to network")
		logger.Exit(1)
	}

	return newnet
}
