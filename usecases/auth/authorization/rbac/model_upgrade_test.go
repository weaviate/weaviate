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

package rbac

import (
	"slices"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestUpdateGroupings(t *testing.T) {
	tests := []struct {
		name               string
		rolesToAdd         []string
		assignments        map[string]string
		expectedAfterWards map[string]string
		authNconf          config.Authentication
	}{
		{
			name:               "only internal - will only be added as db",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"user:" + conv.InternalPlaceHolder: "role:test"},
			expectedAfterWards: map[string]string{"db:" + conv.InternalPlaceHolder: "role:test"},
			authNconf:          config.Authentication{OIDC: config.OIDC{Enabled: true}},
		},
		{
			name:               "only oidc enabled - normal user will only be added as oidc",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"user:something": "role:test"},
			expectedAfterWards: map[string]string{"oidc:something": "role:test"},
			authNconf:          config.Authentication{OIDC: config.OIDC{Enabled: true}},
		},
		{
			name:               "only db enabled - normal user will only be added as db",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"user:something": "role:test"},
			expectedAfterWards: map[string]string{"db:something": "role:test"},
			authNconf:          config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"something"}}},
		},
		{
			name:               "both enabled - normal user will be added for both",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"user:something": "role:test"},
			expectedAfterWards: map[string]string{"db:something": "role:test", "oidc:something": "role:test"},
			authNconf:          config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"something"}}, OIDC: config.OIDC{Enabled: true}},
		},
		{
			name:               "both enabled but user is not added to api key list- normal user will be added for both",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"user:something": "role:test"},
			expectedAfterWards: map[string]string{"oidc:something": "role:test"},
			authNconf:          config.Authentication{APIKey: config.StaticAPIKey{Enabled: true}, OIDC: config.OIDC{Enabled: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := model.NewModelFromString(MODEL)
			require.NoError(t, err)

			enforcer, err := casbin.NewSyncedCachedEnforcer(m)
			require.NoError(t, err)

			for _, role := range tt.rolesToAdd {
				_, err := enforcer.AddNamedPolicy("p", role, "*", "R", "*")
				require.NoError(t, err)

			}

			for user, role := range tt.assignments {
				_, err := enforcer.AddRoleForUser(user, role)
				require.NoError(t, err)
			}

			require.NoError(t, upgradeGroupingsFrom129(enforcer, tt.authNconf))
			roles, _ := enforcer.GetAllSubjects()
			require.Len(t, roles, len(tt.rolesToAdd))
			for user, role := range tt.expectedAfterWards {
				users, err := enforcer.GetUsersForRole(role)
				require.NoError(t, err)
				require.True(t, slices.Contains(users, user))
			}
		})
	}
}
