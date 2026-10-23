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

package rbac

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestResetForReplay(t *testing.T) {
	authN := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"root-user", "bob"}}}
	newManager := func(t *testing.T) (*Manager, *test.Hook) {
		t.Helper()
		logger, hook := test.NewNullLogger()
		m, err := New(t.TempDir(), rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, authN, false, nil, logger)
		require.NoError(t, err)
		return m, hook
	}
	warnings := func(hook *test.Hook) []*logrus.Entry {
		var out []*logrus.Entry
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel {
				out = append(out, e)
			}
		}
		return out
	}

	t.Run("custom roles are dropped and logged, predefined roles and static assignments kept", func(t *testing.T) {
		m, hook := newManager(t)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
			"zeta":  {{Resource: authorization.Cluster(), Verb: authorization.READ, Domain: authorization.ClusterDomain}},
			"alpha": {{Resource: authorization.Cluster(), Verb: authorization.READ, Domain: authorization.ClusterDomain}},
		}))
		require.NoError(t, m.AddRolesForUser("db:bob", []string{"alpha"}))

		require.NoError(t, m.ResetForReplay())

		roles, err := m.GetRoles()
		require.NoError(t, err)
		for _, builtIn := range authorization.BuiltInRoles {
			assert.Contains(t, roles, builtIn)
		}
		assert.NotContains(t, roles, "alpha")
		assert.NotContains(t, roles, "zeta")
		bobRoles, err := m.casbin.GetRolesForUser("db:bob")
		require.NoError(t, err)
		assert.Empty(t, bobRoles)
		rootRoles, err := m.casbin.GetRolesForUser("db:root-user")
		require.NoError(t, err)
		assert.Contains(t, rootRoles, conv.PrefixRoleName(authorization.Root))

		w := warnings(hook)
		require.Len(t, w, 1)
		assert.Equal(t, []string{"alpha", "zeta"}, w[0].Data["roles"])
	})

	t.Run("a store holding only predefined roles logs nothing", func(t *testing.T) {
		m, hook := newManager(t)
		require.NoError(t, m.ResetForReplay())
		assert.Empty(t, warnings(hook))
	})

	t.Run("RBAC disabled is a no-op", func(t *testing.T) {
		m, err := New(t.TempDir(), rbacconf.Config{}, authN, false, nil, logrus.New())
		require.NoError(t, err)
		require.NoError(t, m.ResetForReplay())
	})
}
