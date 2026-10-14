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
	"github.com/weaviate/weaviate/usecases/auth/authentication"
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

// metadataReaderModes runs a metadata reader test with namespaces off and on.
// applyPredefinedRoles picks its wildcard roles per mode, so a regression in
// either branch must fail here.
var metadataReaderModes = []struct {
	name              string
	namespacesEnabled bool
}{
	{name: "namespaces off", namespacesEnabled: false},
	{name: "namespaces on", namespacesEnabled: true},
}

func setupMetadataReaderTestManager(t *testing.T, namespacesEnabled bool, groups ...string) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	conf := rbacconf.Config{Enabled: true, MetadataGroups: groups}
	require.NoError(t, conf.Validate())
	m, err := New(filepath.Join(t.TempDir(), "policy.csv"), conf,
		config.Authentication{OIDC: config.OIDC{Enabled: true}}, namespacesEnabled, nil, logger)
	require.NoError(t, err)
	return m
}

// metadataReaderPrincipal is a caller in groups. With namespaces on it is a
// global operator, since nodes, cluster and replicate are operator-only there.
func metadataReaderPrincipal(name string, namespacesEnabled bool, groups ...string) *models.Principal {
	return &models.Principal{Username: name, UserType: models.UserTypeInputOidc, Groups: groups, IsGlobalOperator: namespacesEnabled}
}

func TestMetadataReaderHasNoWildcardPolicy(t *testing.T) {
	for _, mode := range metadataReaderModes {
		t.Run(mode.name, func(t *testing.T) {
			m := setupMetadataReaderTestManager(t, mode.namespacesEnabled)
			policies, err := m.casbin.GetFilteredPolicy(0, conv.PrefixRoleName(authorization.MetadataReader))
			require.NoError(t, err)
			require.NotEmpty(t, policies)
			for _, p := range policies {
				assert.NotEqual(t, "*", p[1], "metadata reader must not hold a wildcard resource: %v", p)
				// an empty verb regex-matches every action
				assert.NotEmpty(t, p[2], "metadata reader must not hold an empty verb: %v", p)
				// roles carry a scope suffix (R_ALL); every verb must still be a plain read
				assert.Contains(t, []string{authorization.READ, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL)}, p[2], "metadata reader must be read-only: %v", p)
				assert.NotEqual(t, authorization.DataDomain, p[3], "metadata reader must not hold data policies: %v", p)
			}
		})
	}
}

func TestMetadataReaderGroupPermissions(t *testing.T) {
	const group = "infra-access"
	allowed := map[string]string{
		"collection config": authorization.CollectionsMetadata("Movies")[0],
		"tenants":           authorization.ShardsMetadata("Movies", "tenant1")[0],
		"nodes verbose":     authorization.Nodes(verbosity.OutputVerbose, "Movies")[0],
		// verbosity is a discriminator in the resource path: the default minimal
		// read must not 403 just because the role was granted verbose
		"nodes minimal": authorization.Nodes(verbosity.OutputMinimal)[0],
		"cluster":       authorization.Cluster(),
		"aliases":       authorization.Aliases("Movies", "MoviesAlias")[0],
		"replication":   authorization.Replications("Movies", "shard1"),
		"users":         authorization.Users("someone")[0],
		"roles":         authorization.Roles("admin")[0],
		"groups":        authorization.Groups(authentication.AuthTypeOIDC, "some-group")[0],
		"backups":       authorization.Backups("Movies")[0],
	}
	denied := []struct {
		name, resource, verb string
	}{
		{"read objects", authorization.Objects("Movies", "shard1"), authorization.READ},
		{"delete objects", authorization.Objects("Movies", "shard1"), authorization.DELETE},
		{"create objects", authorization.Objects("Movies", "shard1"), authorization.CREATE},
		{"read shards data", authorization.ShardsData("Movies", "shard1")[0], authorization.READ},
		{"create collection", authorization.CollectionsMetadata("Movies")[0], authorization.CREATE},
		{"delete collection", authorization.CollectionsMetadata("Movies")[0], authorization.DELETE},
		{"create backups", authorization.Backups("Movies")[0], authorization.CREATE},
		{"cancel backups", authorization.Backups("Movies")[0], authorization.DELETE},
		{"read mcp", authorization.Mcp(), authorization.READ},
	}

	for _, mode := range metadataReaderModes {
		t.Run(mode.name, func(t *testing.T) {
			m := setupMetadataReaderTestManager(t, mode.namespacesEnabled, group)
			staff := metadataReaderPrincipal("staff", mode.namespacesEnabled, group)
			outsider := metadataReaderPrincipal("other", mode.namespacesEnabled, "other-group")

			for name, resource := range allowed {
				t.Run("allowed/"+name, func(t *testing.T) {
					read, update := authorization.READ, authorization.UPDATE
					if name == "roles" {
						// the roles handler authorizes reads as READ with scope ALL
						read = authorization.VerbWithScope(read, authorization.ROLE_SCOPE_ALL)
						update = authorization.VerbWithScope(update, authorization.ROLE_SCOPE_ALL)
					}
					ok, err := m.checkPermissions(staff, resource, read)
					require.NoError(t, err)
					assert.True(t, ok)

					ok, err = m.checkPermissions(staff, resource, update)
					require.NoError(t, err)
					assert.False(t, ok, "metadata reader must not write")

					ok, err = m.checkPermissions(outsider, resource, read)
					require.NoError(t, err)
					assert.False(t, ok, "only the configured group holds the role")
				})
			}

			for _, d := range denied {
				t.Run("denied/"+d.name, func(t *testing.T) {
					ok, err := m.checkPermissions(staff, d.resource, d.verb)
					require.NoError(t, err)
					assert.False(t, ok)
				})
			}
		})
	}
}

func TestMetadataReaderBindingResetFromConfig(t *testing.T) {
	for _, mode := range metadataReaderModes {
		t.Run(mode.name, func(t *testing.T) {
			m := setupMetadataReaderTestManager(t, mode.namespacesEnabled, "infra-access")
			// a binding added outside config does not survive a re-apply
			_, err := m.casbin.AddRoleForUser(conv.PrefixGroupName("sneaky"), conv.PrefixRoleName(authorization.MetadataReader))
			require.NoError(t, err)

			require.NoError(t, applyPredefinedRoles(m.casbin, rbacconf.Config{Enabled: true, MetadataGroups: []string{"infra-access"}},
				config.Authentication{OIDC: config.OIDC{Enabled: true}}, mode.namespacesEnabled))

			subjects, err := m.casbin.GetUsersForRole(conv.PrefixRoleName(authorization.MetadataReader))
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{conv.PrefixGroupName("infra-access")}, subjects)
		})
	}
}

func TestAddWildcardPoliciesFailsClosed(t *testing.T) {
	m := setupMetadataReaderTestManager(t, false)
	err := addWildcardPolicies(m.casbin, []string{authorization.Root, "role-without-verb"}, conv.BuiltInWildcardVerb)
	require.ErrorContains(t, err, `built-in role "role-without-verb" has no wildcard verb`)

	policies, err := m.casbin.GetFilteredPolicy(0, conv.PrefixRoleName("role-without-verb"))
	require.NoError(t, err)
	assert.Empty(t, policies, "no policy may be registered for a role without a wildcard verb")
}
