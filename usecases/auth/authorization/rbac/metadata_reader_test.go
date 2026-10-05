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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// metadataReaderPolicyRows returns the casbin rows applyPredefinedRoles
// registers for the metadata reader role.
func metadataReaderPolicyRows(t *testing.T, namespacesEnabled bool) [][]string {
	t.Helper()
	policies, err := conv.PermissionToPolicies(authorization.BuiltInPermissionsFor(namespacesEnabled)[authorization.MetadataReader]...)
	require.NoError(t, err)
	rows := make([][]string, 0, len(policies))
	for _, p := range policies {
		rows = append(rows, []string{conv.PrefixRoleName(authorization.MetadataReader), p.Resource, p.Verb, p.Domain})
	}
	return rows
}

func setupMetadataReaderTestManager(t *testing.T, groups ...string) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	conf := rbacconf.Config{Enabled: true, MetadataGroups: groups}
	require.NoError(t, conf.Validate())
	m, err := New(filepath.Join(t.TempDir(), "policy.csv"), conf,
		config.Authentication{OIDC: config.OIDC{Enabled: true}}, false, nil, logger)
	require.NoError(t, err)
	return m
}

func TestMetadataReaderHasNoWildcardPolicy(t *testing.T) {
	m := setupMetadataReaderTestManager(t)
	policies, err := m.casbin.GetFilteredPolicy(0, conv.PrefixRoleName(authorization.MetadataReader))
	require.NoError(t, err)
	require.NotEmpty(t, policies)
	for _, p := range policies {
		assert.NotEqual(t, "*", p[1], "metadata reader must not hold a wildcard resource: %v", p)
		assert.Equal(t, authorization.READ, p[2], "metadata reader must be read-only: %v", p)
		assert.NotEqual(t, authorization.DataDomain, p[3], "metadata reader must not hold data policies: %v", p)
	}
}

func TestMetadataReaderGroupPermissions(t *testing.T) {
	const group = "infra-access"
	m := setupMetadataReaderTestManager(t, group)
	staff := &models.Principal{Username: "staff", UserType: models.UserTypeInputOidc, Groups: []string{group}}
	outsider := &models.Principal{Username: "other", UserType: models.UserTypeInputOidc, Groups: []string{"other-group"}}

	allowed := map[string]string{
		"collection config": authorization.CollectionsMetadata("Movies")[0],
		"tenants":           authorization.ShardsMetadata("Movies", "tenant1")[0],
		"nodes verbose":     authorization.Nodes(verbosity.OutputVerbose, "Movies")[0],
		"cluster":           authorization.Cluster(),
		"aliases":           authorization.Aliases("Movies", "MoviesAlias")[0],
		"replication":       authorization.Replications("Movies", "shard1"),
	}
	for name, resource := range allowed {
		t.Run("allowed/"+name, func(t *testing.T) {
			ok, err := m.checkPermissions(staff, resource, authorization.READ)
			require.NoError(t, err)
			assert.True(t, ok)

			ok, err = m.checkPermissions(staff, resource, authorization.UPDATE)
			require.NoError(t, err)
			assert.False(t, ok, "metadata reader must not write")

			ok, err = m.checkPermissions(outsider, resource, authorization.READ)
			require.NoError(t, err)
			assert.False(t, ok, "only the configured group holds the role")
		})
	}

	denied := map[string]string{
		"objects":     authorization.Objects("Movies", "shard1"),
		"shards data": authorization.ShardsData("Movies", "shard1")[0],
		"backups":     authorization.Backups("Movies")[0],
		"users":       authorization.Users("someone")[0],
		"roles":       authorization.Roles("admin")[0],
		"mcp":         authorization.Mcp(),
	}
	for name, resource := range denied {
		t.Run("denied/"+name, func(t *testing.T) {
			ok, err := m.checkPermissions(staff, resource, authorization.READ)
			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}

func TestMetadataReaderBindingResetFromConfig(t *testing.T) {
	m := setupMetadataReaderTestManager(t, "infra-access")
	// a binding added outside config does not survive a re-apply
	_, err := m.casbin.AddRoleForUser(conv.PrefixGroupName("sneaky"), conv.PrefixRoleName(authorization.MetadataReader))
	require.NoError(t, err)

	require.NoError(t, applyPredefinedRoles(m.casbin, rbacconf.Config{Enabled: true, MetadataGroups: []string{"infra-access"}},
		config.Authentication{OIDC: config.OIDC{Enabled: true}}, false))

	subjects, err := m.casbin.GetUsersForRole(conv.PrefixRoleName(authorization.MetadataReader))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{conv.PrefixGroupName("infra-access")}, subjects)
}
