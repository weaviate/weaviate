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
	"os"
	"path/filepath"
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
	rbacAuthorizer := func(t *testing.T, appState *state.State) {
		assert.Equal(t, appState.RBAC, appState.Authorizer)
	}
	tests := []struct {
		name  string
		authz config.Authorization
		// legacyPolicy, when set, is planted as <DataPath>/raft/rbac/policy.csv.
		legacyPolicy   string
		wantAuthorizer func(t *testing.T, appState *state.State)
	}{
		{
			name:           "RBAC enabled",
			authz:          config.Authorization{Rbac: rbacconf.Config{Enabled: true}},
			wantAuthorizer: rbacAuthorizer,
		},
		{
			name:  "RBAC enabled seeds the legacy policy file under the RAFT dir",
			authz: config.Authorization{Rbac: rbacconf.Config{Enabled: true}},
			legacyPolicy: "p, role:custom, schema/collections/Movies/shards/#, R, schema\n" +
				"g, db:wv_internal_empty, role:custom\n",
			wantAuthorizer: rbacAuthorizer,
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
			dataPath := t.TempDir()
			if tt.legacyPolicy != "" {
				legacyDir := filepath.Join(dataPath, "raft", "rbac")
				require.NoError(t, os.MkdirAll(legacyDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "policy.csv"), []byte(tt.legacyPolicy), 0o644))
				// A version of 1.30 or later skips the <1.30 rewrite, so the rows load as written.
				require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "version"), []byte("1.30.0"), 0o644))
			}
			appState := &state.State{
				Logger: logger,
				ServerConfig: &config.WeaviateConfig{Config: config.Config{
					Persistence:   config.Persistence{DataPath: dataPath},
					Authorization: tt.authz,
				}},
			}
			require.NoError(t, configureAuthorizer(appState))

			require.NotNil(t, appState.RBAC, "the manager is built whatever the config")
			tt.wantAuthorizer(t, appState)
			if tt.legacyPolicy != "" {
				roles, err := appState.RBAC.GetRoles()
				require.NoError(t, err)
				_, seeded := roles["custom"]
				assert.True(t, seeded, "the role from the legacy policy file is seeded")
			}

			// assert.Nil also passes a typed nil pointer inside the interface, so
			// the disabled case compares the interfaces with nil directly.
			snapshotter, roleLister := rbacBackupCapabilities(tt.authz.Rbac.Enabled, appState.RBAC)
			if tt.authz.Rbac.Enabled {
				assert.Same(t, appState.RBAC, snapshotter)
				assert.Same(t, appState.RBAC, roleLister)
			} else {
				assert.True(t, snapshotter == nil, "RBAC disabled leaves the snapshotter untyped nil")
				assert.True(t, roleLister == nil, "RBAC disabled leaves the role lister untyped nil")
			}

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
