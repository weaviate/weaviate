//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/authz"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_DummyAuthorizer(t *testing.T) {
	t.Run("when no authz is configured", func(t *testing.T) {
		authorizer := authorization.DummyAuthorizer{}

		t.Run("any request is allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), nil, "delete", "the/world")
			assert.Nil(t, err)
		})
	})
}

func Test_AdminListAuthorizer(t *testing.T) {
	t.Run("when adminlist is configured", func(t *testing.T) {
		cfg := config.Config{
			Authorization: config.Authorization{
				AdminList: adminlist.Config{
					Enabled: true,
					Users:   []string{"user1"},
				},
			},
		}

		authorizer := adminlist.New(cfg.Authorization.AdminList)
		t.Run("admin requests are allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), &models.Principal{Username: "user1"}, "delete", "the/world")
			assert.Nil(t, err)
		})

		t.Run("non admin requests are allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), &models.Principal{Username: "user2"}, "delete", "the/world")
			assert.NotNil(t, err)
		})
	})
}

func TestConfigureAuthorizer(t *testing.T) {
	tests := []struct {
		name           string
		authz          config.Authorization
		wantAuthorizer func(t *testing.T, appState *state.State)
	}{
		{
			name:  "RBAC enabled",
			authz: config.Authorization{Rbac: rbacconf.Config{Enabled: true}},
			wantAuthorizer: func(t *testing.T, appState *state.State) {
				assert.Equal(t, appState.RBAC, appState.Authorizer)
			},
		},
		{
			name:  "adminlist enabled",
			authz: config.Authorization{AdminList: adminlist.Config{Enabled: true}},
			wantAuthorizer: func(t *testing.T, appState *state.State) {
				assert.IsType(t, &adminlist.Authorizer{}, appState.Authorizer)
			},
		},
		{
			name: "no authorization configured",
			wantAuthorizer: func(t *testing.T, appState *state.State) {
				assert.IsType(t, &authorization.DummyAuthorizer{}, appState.Authorizer)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			appState := &state.State{
				Logger: logger,
				ServerConfig: &config.WeaviateConfig{Config: config.Config{
					Persistence:   config.Persistence{DataPath: t.TempDir()},
					Authorization: tt.authz,
				}},
			}
			require.NoError(t, configureAuthorizer(appState))

			require.NotNil(t, appState.RBAC, "the manager is built whatever the config")
			tt.wantAuthorizer(t, appState)

			raft := authz.NewMockControllerAndGetUsers(t)
			roleAPI := roleAPIController(tt.authz.Rbac.Enabled, raft)
			if tt.authz.Rbac.Enabled {
				assert.Same(t, raft, roleAPI)
			} else {
				inert, ok := roleAPI.(authz.InertController)
				require.True(t, ok, "RBAC disabled selects the inert controller")
				assert.Same(t, raft, inert.Users, "user lookups go to raft")
			}
		})
	}
}

func TestConfigureAuthorizerRefusesNamespacesWithoutController(t *testing.T) {
	logger, _ := test.NewNullLogger()
	appState := &state.State{
		Logger: logger,
		ServerConfig: &config.WeaviateConfig{Config: config.Config{
			Namespaces: config.Namespaces{Enabled: true},
			Authorization: config.Authorization{
				Rbac: rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
			},
		}},
	}

	require.ErrorContains(t, configureAuthorizer(appState), "requires a namespace controller")
}
