//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/utils"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authentication/anonymous"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/traverser"
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
		if appState.ServerConfig.Config.DisableGraphQL {
			return
		}

		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.

		gql, err := rebuildGraphQL(
			updatedSchema,
			logger,
			appState.ServerConfig.Config,
			traverser,
			appState.Modules,
		)
		if err != nil && err != utils.ErrEmptySchema {
			logger.WithField("action", "graphql_rebuild").
				WithError(err).Error("could not (re)build graphql provider")
		}
		appState.SetGraphQL(gql)
	}
}

func rebuildGraphQL(updatedSchema schema.Schema, logger logrus.FieldLogger,
	config config.Config, traverser *traverser.Traverser, modulesProvider *modules.Provider,
) (graphql.GraphQL, error) {
	updatedGraphQL, err := graphql.Build(&updatedSchema, traverser, logger, config, modulesProvider)
	if err != nil {
		return nil, err
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

func configureAPIKey(appState *state.State) *apikey.Client {
	c, err := apikey.New(appState.ServerConfig.Config)
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
	return time.Until(dl).String()
}
