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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/utils"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authentication/anonymous"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
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

func makeUpdateSchemaCall(appState *state.State) func(schema.Schema) {
	return func(updatedSchema schema.Schema) {
		if appState.ServerConfig.Config.DisableGraphQL {
			return
		}

		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.

		gql, err := rebuildGraphQL(
			updatedSchema,
			appState.Logger,
			appState.ServerConfig.Config,
			appState.Traverser,
			appState.Modules,
			appState.Authorizer,
		)
		if err != nil && !errors.Is(err, utils.ErrEmptySchema) {
			appState.Logger.WithField("action", "graphql_rebuild").
				WithError(err).Error("could not (re)build graphql provider")
		}
		appState.SetGraphQL(gql)
	}
}

func rebuildGraphQL(updatedSchema schema.Schema, logger logrus.FieldLogger,
	config config.Config, traverser *traverser.Traverser, modulesProvider *modules.Provider, authorizer authorization.Authorizer,
) (graphql.GraphQL, error) {
	updatedGraphQL, err := graphql.Build(&updatedSchema, traverser, logger, config, modulesProvider, authorizer)
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

func configureAPIKey(appState *state.State) *apikey.ApiKey {
	c, err := apikey.New(appState.ServerConfig.Config, appState.Logger)
	if err != nil {
		appState.Logger.WithField("action", "api_keys_init").WithError(err).Fatal("apikey client could not start up")
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

func configureAuthorizer(appState *state.State) error {
	if appState.ServerConfig.Config.Authorization.Rbac.Enabled {
		// if rbac enforcer enabled, start forcing all requests using the casbin enforcer
		rbacController, err := rbac.New(
			filepath.Join(appState.ServerConfig.Config.Persistence.DataPath, config.DefaultRaftDir),
			appState.ServerConfig.Config.Authorization.Rbac, appState.ServerConfig.Config.Authentication,
			appState.Logger)
		if err != nil {
			return fmt.Errorf("can't init casbin %w", err)
		}

		appState.AuthzController = rbacController
		appState.AuthzSnapshotter = rbacController
		appState.RBAC = rbacController
		appState.Authorizer = rbacController
	} else if appState.ServerConfig.Config.Authorization.AdminList.Enabled {
		appState.Authorizer = adminlist.New(appState.ServerConfig.Config.Authorization.AdminList)
	} else {
		appState.Authorizer = &authorization.DummyAuthorizer{}
	}

	if appState.ServerConfig.Config.Authorization.Rbac.Enabled && appState.RBAC == nil {
		// this in general shall not happen, it's to catch cases were RBAC expected but we weren't able
		// to assign it.
		return fmt.Errorf("RBAC is expected to be enabled, but the controller wasn't initialized")
	}

	return nil
}

func timeTillDeadline(ctx context.Context) string {
	dl, _ := ctx.Deadline()
	return time.Until(dl).String()
}
