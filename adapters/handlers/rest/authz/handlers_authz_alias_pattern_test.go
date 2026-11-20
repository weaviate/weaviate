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
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestPatternCheckedAsStored checks that create and add refuse a pattern that
// stops compiling once conv stores it, and conv.CheckLookup doesn't, so a row already
// stored stays removable.
func TestPatternCheckedAsStored(t *testing.T) {
	tests := []struct {
		name    string
		perm    *models.Permission
		wantErr string
	}{
		{
			name:    "alias uppercased",
			perm:    &models.Permission{Action: String(authorization.ReadAliases), Aliases: &models.PermissionAliases{Collection: String("*"), Alias: String("[[:alpha:]]+")}},
			wantErr: "'[[:alpha:]]+' is not a valid alias name",
		},
		{
			// conv stores X|\Q as (X|\Q), and \Q quotes the closing ')'.
			name:    "alternation wrapped in a group",
			perm:    &models.Permission{Action: String(authorization.ReadData), Data: &models.PermissionData{Collection: String(`X|\Q`)}},
			wantErr: "is not a valid pattern: error parsing regexp: missing closing )",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, conv.ValidatePermissions(false, conv.CheckLookup, tt.perm))

			t.Run("create role", func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				h := &authZHandlers{authorizer: authorization.NewMockAuthorizer(t), controller: NewMockControllerAndGetUsers(t), logger: logger}
				res := h.createRole(authz.CreateRoleParams{
					HTTPRequest: req,
					Body:        &models.Role{Name: String("newRole"), Permissions: []*models.Permission{tt.perm}},
				}, &models.Principal{Username: "user1"})
				parsed, ok := res.(*authz.CreateRoleUnprocessableEntity)
				require.True(t, ok, "got %T", res)
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.wantErr)
			})

			t.Run("add permissions", func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				h := &authZHandlers{authorizer: authorization.NewMockAuthorizer(t), controller: NewMockControllerAndGetUsers(t), logger: logger}
				res := h.addPermissions(authz.AddPermissionsParams{
					ID:          "someRole",
					HTTPRequest: req,
					Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{tt.perm}},
				}, &models.Principal{Username: "user1"})
				parsed, ok := res.(*authz.AddPermissionsBadRequest)
				require.True(t, ok, "got %T", res)
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.wantErr)
			})
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
