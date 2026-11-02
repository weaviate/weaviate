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

package authz

import (
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestAliasPatternCheckedAsStored checks that create and add refuse an alias
// pattern that stops compiling once conv uppercases it, and checkLookup doesn't.
func TestAliasPatternCheckedAsStored(t *testing.T) {
	perm := &models.Permission{Action: String(authorization.ReadAliases), Aliases: &models.PermissionAliases{Collection: String("*"), Alias: String("[[:alpha:]]+")}}
	wantErr := "'[[:alpha:]]+' is not a valid alias name"
	require.NoError(t, validatePermissions(false, checkLookup, perm))

	t.Run("create role", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		h := &authZHandlers{authorizer: authorization.NewMockAuthorizer(t), controller: NewMockControllerAndGetUsers(t), logger: logger}
		res := h.createRole(authz.CreateRoleParams{
			HTTPRequest: req,
			Body:        &models.Role{Name: String("newRole"), Permissions: []*models.Permission{perm}},
		}, &models.Principal{Username: "user1"})
		parsed, ok := res.(*authz.CreateRoleUnprocessableEntity)
		require.True(t, ok, "got %T", res)
		assert.Contains(t, parsed.Payload.Error[0].Message, wantErr)
	})

	t.Run("add permissions", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		h := &authZHandlers{authorizer: authorization.NewMockAuthorizer(t), controller: NewMockControllerAndGetUsers(t), logger: logger}
		res := h.addPermissions(authz.AddPermissionsParams{
			ID:          "someRole",
			HTTPRequest: req,
			Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{perm}},
		}, &models.Principal{Username: "user1"})
		parsed, ok := res.(*authz.AddPermissionsBadRequest)
		require.True(t, ok, "got %T", res)
		assert.Contains(t, parsed.Payload.Error[0].Message, wantErr)
	})
}

// TestValidatePermissionsAliasRule checks that create and add apply the
// class-name rule to the uppercased alias pattern and that lookups skip it.
func TestValidatePermissionsAliasRule(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		aliases           *models.PermissionAliases
		wantErr           string
	}{
		{name: "no aliases"},
		{name: "no alias", aliases: &models.PermissionAliases{Collection: String("*")}},
		{name: "wildcard", aliases: &models.PermissionAliases{Alias: String("*")}},
		{name: "lowercase first letter", aliases: &models.PermissionAliases{Alias: String("my_alias")}},
		{name: "prefix pattern", aliases: &models.PermissionAliases{Alias: String("Alias*")}},
		{name: "alternation", aliases: &models.PermissionAliases{Alias: String("Movies|Books")}},
		{name: "non-ASCII first letter", aliases: &models.PermissionAliases{Alias: String("ålias.*")}, wantErr: "'ålias.*' is not a valid alias name"},
		{name: "non-ASCII letter after the first", aliases: &models.PermissionAliases{Alias: String("Alïas")}, wantErr: "not a valid alias name"},
		{name: "long s, whose uppercase is ASCII S", aliases: &models.PermissionAliases{Alias: String("ſlias.*")}, wantErr: "not a valid alias name"},
		{name: "dotless i, whose uppercase is ASCII I", aliases: &models.PermissionAliases{Alias: String("ıtems")}, wantErr: "not a valid alias name"},
		{name: "starts with a regex operator", aliases: &models.PermissionAliases{Alias: String(".*_v2")}, wantErr: "not a valid alias name"},
		{name: "colon with namespaces off", aliases: &models.PermissionAliases{Alias: String("my:alias")}, wantErr: "not a valid alias name"},
		{name: "empty", aliases: &models.PermissionAliases{Alias: String("")}, wantErr: "not a valid alias name"},
		{name: "qualified with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: String("customer1:films*")}},
		{name: "empty namespace with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: String(":films")}, wantErr: "not a valid alias name"},
		{name: "qualified non-ASCII alias with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: String("customer1:\u00e5lias")}, wantErr: "not a valid alias name"},
		{name: "POSIX class with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: String("[[:alpha:]]+")}, wantErr: "not a valid alias name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perm := &models.Permission{Aliases: tt.aliases}
			for _, check := range []permissionCheck{checkCreate, checkAdd} {
				err := validatePermissions(tt.namespacesEnabled, check, perm)
				if tt.wantErr == "" {
					require.NoError(t, err)
					continue
				}
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.NoError(t, validatePermissions(tt.namespacesEnabled, checkLookup, perm))
		})
	}
}

// rbacController runs the handler against a real rbac.Manager, which stores
// policies in casbin the way the cluster applies them.
type rbacController struct{ *rbac.Manager }

func (rbacController) GetUsers(...string) (map[string]apikey.UserView, error) { return nil, nil }

// TestLookupsReachUnreadableAliasRow checks that has-permission finds and
// remove-permissions deletes "Ã\xa5lias.*", the row conv stores for "ålias.*".
// JSON can't carry those bytes, so conv rebuilds them from the pattern as sent.
func TestLookupsReachUnreadableAliasRow(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
		config.Authentication{OIDC: config.OIDC{Enabled: true}}, false, nil, logger)
	require.NoError(t, err)

	stored := authorization.Policy{Resource: "aliases/collections/.*/aliases/\u00c3\xa5lias..*", Verb: authorization.READ, Domain: authorization.AliasesDomain}
	other := authorization.Policy{Resource: "aliases/collections/.*/aliases/Other", Verb: authorization.READ, Domain: authorization.AliasesDomain}
	require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"legacy": {stored, other}}))

	principal := &models.Principal{Username: "user1"}
	authorizer := authorization.NewMockAuthorizer(t)
	for _, verb := range []string{authorization.READ, authorization.UPDATE} {
		authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_ALL), authorization.Roles("legacy")[0]).Return(nil)
	}
	h := &authZHandlers{authorizer: authorizer, controller: rbacController{m}, logger: logger}
	perm := &models.Permission{
		Action:  String(authorization.ReadAliases),
		Aliases: &models.PermissionAliases{Collection: String("*"), Alias: String("\u00e5lias.*")},
	}

	res := h.hasPermission(authz.HasPermissionParams{ID: "legacy", HTTPRequest: req, Body: perm}, principal)
	parsed, ok := res.(*authz.HasPermissionOK)
	require.True(t, ok, "got %T", res)
	assert.True(t, parsed.Payload, "has-permission must find the unreadable row")

	res = h.removePermissions(authz.RemovePermissionsParams{
		ID:          "legacy",
		HTTPRequest: req,
		Body:        authz.RemovePermissionsBody{Permissions: []*models.Permission{perm}},
	}, principal)
	require.IsType(t, &authz.RemovePermissionsOK{}, res)

	has, err := m.HasPermission("legacy", &stored)
	require.NoError(t, err)
	assert.False(t, has, "the unreadable row must be gone")
	has, err = m.HasPermission("legacy", &other)
	require.NoError(t, err)
	assert.True(t, has, "the role's other rows must stay")
}
