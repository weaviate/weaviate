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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
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

func TestConfigureAuthorizer_NamespaceRequirements(t *testing.T) {
	tests := []struct {
		name           string
		namespaces     bool
		rbac           bool
		adminList      bool
		omitController bool
		wantErr        string
	}{
		{
			name:       "rbac with namespaces on is the rbac controller",
			namespaces: true,
			rbac:       true,
		},
		{
			name:      "adminlist without namespaces is allowed",
			adminList: true,
		},
		{
			name:           "rbac with namespaces on but no controller is refused",
			namespaces:     true,
			rbac:           true,
			omitController: true,
			wantErr:        "requires a namespace controller",
		},
		{
			name:           "rbac without namespaces needs no controller",
			rbac:           true,
			omitController: true,
		},
		{
			name:           "adminlist with namespaces on and no controller is refused",
			namespaces:     true,
			adminList:      true,
			omitController: true,
			wantErr:        "requires a namespace controller",
		},
		{
			name:           "no authorizer with namespaces on and no controller is refused",
			namespaces:     true,
			omitController: true,
			wantErr:        "requires a namespace controller",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			appState := &state.State{
				Logger: logger,
				ServerConfig: &config.WeaviateConfig{Config: config.Config{
					Namespaces:  config.Namespaces{Enabled: tt.namespaces},
					Persistence: config.Persistence{DataPath: filepath.Join(t.TempDir(), "data")},
					Authorization: config.Authorization{
						Rbac:      rbacconf.Config{Enabled: tt.rbac, RootUsers: []string{"root"}},
						AdminList: adminlist.Config{Enabled: tt.adminList, Users: []string{"admin"}},
					},
				}},
			}

			if !tt.omitController {
				appState.NamespacesController = usecasesNamespaces.NewController(logger)
			}

			err := configureAuthorizer(appState)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.rbac {
				require.Same(t, appState.RBAC, appState.Authorizer)
				require.Same(t, appState.RBAC, appState.AuthzController)
				return
			}
			require.IsType(t, &adminlist.Authorizer{}, appState.Authorizer)
		})
	}
}

// TestConfigureAuthorizer_NoControllerLeavesTheListerNil pins that a missing
// controller reaches the RBAC manager as a nil lister, not as an interface holding
// a nil pointer. Snapshot is where the two differ, because it looks up every
// namespace its role names refer to.
func TestConfigureAuthorizer_NoControllerLeavesTheListerNil(t *testing.T) {
	logger, _ := test.NewNullLogger()
	appState := &state.State{
		Logger: logger,
		ServerConfig: &config.WeaviateConfig{Config: config.Config{
			Persistence: config.Persistence{DataPath: filepath.Join(t.TempDir(), "data")},
			Authorization: config.Authorization{
				Rbac: rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
			},
		}},
	}

	require.NoError(t, configureAuthorizer(appState))

	// The role name carries the namespace that drives the lookup. Without one the
	// snapshot never consults the lister and the assertion below proves nothing.
	require.NoError(t, appState.RBAC.CreateRolesPermissions(map[string][]authorization.Policy{
		"alpha:editor": {{
			Resource: "data/collections/alpha:Movies/shards/*/objects/*",
			Verb:     authorization.READ,
			Domain:   authorization.DataDomain,
		}},
	}))

	_, err := appState.RBAC.Snapshot()
	require.NoError(t, err)
}
