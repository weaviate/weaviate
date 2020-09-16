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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network"
	libnetworkFake "github.com/semi-technologies/weaviate/usecases/network/fake"
	libnetworkP2P "github.com/semi-technologies/weaviate/usecases/network/p2p"
	"github.com/semi-technologies/weaviate/usecases/traverser"
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

func makeUpdateSchemaCall(logger logrus.FieldLogger, appState *state.State, traverser *traverser.Traverser) func(schema.Schema) {
	return func(updatedSchema schema.Schema) {
		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.

		gql, err := rebuildGraphQL(
			updatedSchema,
			logger,
			appState.Network,
			appState.ServerConfig.Config,
			traverser,
		)
		if err != nil {
			logger.WithField("action", "graphql_rebuild").
				WithError(err).Error("could not (re)build graphql provider")
		}
		appState.GraphQL = gql
	}
}

func rebuildGraphQL(updatedSchema schema.Schema, logger logrus.FieldLogger,
	network network.Network, config config.Config,
	traverser *traverser.Traverser) (graphql.GraphQL, error) {
	peers, err := network.ListPeers()
	if err != nil {
		return nil, fmt.Errorf("could not list network peers to regenerate schema: %v", err)
	}

	updatedGraphQL, err := graphql.Build(&updatedSchema, peers, traverser, logger, config)
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
