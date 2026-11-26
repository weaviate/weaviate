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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/config"
)

// unstorableValues are values conv.ValidateStorableValue refuses. The tab stands
// in for every control character other than a line break.
var unstorableValues = []struct {
	name  string
	value string
}{
	// A comma adds a field to the stored row, and a p row with an extra field
	// fails every load.
	{name: "comma", value: "JFxylh,(select*from(select(sleep(20)))a)"},
	{name: "quote", value: `a"b`},
	{name: "LF", value: "a\nb"},
	{name: "CRLF", value: "a\r\nb"},
	{name: "tab", value: "a\tb"},
}

// unstorablePermissions returns one permission per free-form target, each set to value.
func unstorablePermissions(value string) map[string]*models.Permission {
	return map[string]*models.Permission{
		"namespace": {Action: String(authorization.ManageNamespaces), Namespaces: &models.PermissionNamespaces{Namespace: String(value)}},
		"users":     {Action: String(authorization.ReadUsers), Users: &models.PermissionUsers{Users: String(value)}},
		"group":     {Action: String(authorization.ReadGroups), Groups: &models.PermissionGroups{Group: String(value), GroupType: models.GroupTypeOidc}},
		"role":      {Action: String(authorization.ReadRoles), Roles: &models.PermissionRoles{Role: String(value)}},
		"alias":     {Action: String(authorization.ReadAliases), Aliases: &models.PermissionAliases{Alias: String(value)}},
		"shard":     {Action: String(authorization.ReadReplicate), Replicate: &models.PermissionReplicate{Shard: String(value)}},
	}
}

// assertRefusalLogged checks the warning that names who sent a refused value,
// since the refusal comes before Authorize writes its audit line.
func assertRefusalLogged(t *testing.T, hook *test.Hook, action, user string) {
	t.Helper()
	entry := hook.LastEntry()
	require.NotNil(t, entry, "the refusal must be logged")
	assert.Equal(t, logrus.WarnLevel, entry.Level)
	assert.True(t, strings.HasPrefix(entry.Message, "refused "), entry.Message)
	assert.Equal(t, action, entry.Data["action"])
	assert.Equal(t, authorization.ComponentName, entry.Data["component"])
	assert.Equal(t, user, entry.Data["user"])
	assert.Less(t, len(entry.Message), 1024, "the log line must not grow with the request")
}

// The strict mocks fail the test on any call, so each rejection happens before
// authorization and before anything is written. conv already refuses a line
// break in a group target with a 400, before the storable check runs. The
// class-name rule refuses each such value in an alias target first.
func TestCreateRoleRejectsUnstorableValues(t *testing.T) {
	for _, v := range unstorableValues {
		for field, perm := range unstorablePermissions(v.value) {
			t.Run(v.name+"/"+field, func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				h := &authZHandlers{
					authorizer: authorization.NewMockAuthorizer(t),
					controller: NewMockControllerAndGetUsers(t),
					logger:     logger,
				}
				res := h.createRole(authz.CreateRoleParams{
					HTTPRequest: req,
					Body:        &models.Role{Name: String("newRole"), Permissions: []*models.Permission{perm}},
				}, &models.Principal{Username: "user1"})
				if field == "group" && strings.Contains(v.value, "\n") {
					_, ok := res.(*authz.CreateRoleBadRequest)
					assert.True(t, ok, "got %T", res)
					return
				}
				parsed, ok := res.(*authz.CreateRoleUnprocessableEntity)
				require.True(t, ok, "got %T", res)
				if field == "alias" {
					assert.Contains(t, parsed.Payload.Error[0].Message, "not a valid alias name")
					return
				}
				assertRefusalLogged(t, hook, "create_role", "user1")
			})
		}
	}
}

func TestAddPermissionsRejectsUnstorableValues(t *testing.T) {
	for _, v := range unstorableValues {
		for field, perm := range unstorablePermissions(v.value) {
			t.Run(v.name+"/"+field, func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				h := &authZHandlers{
					authorizer: authorization.NewMockAuthorizer(t),
					controller: NewMockControllerAndGetUsers(t),
					logger:     logger,
				}
				res := h.addPermissions(authz.AddPermissionsParams{
					ID:          "someRole",
					HTTPRequest: req,
					Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{perm}},
				}, &models.Principal{Username: "user1"})
				parsed, ok := res.(*authz.AddPermissionsBadRequest)
				require.True(t, ok, "got %T", res)
				if field == "group" && strings.Contains(v.value, "\n") {
					return
				}
				if field == "alias" {
					assert.Contains(t, parsed.Payload.Error[0].Message, "not a valid alias name")
					return
				}
				assertRefusalLogged(t, hook, "add_permissions", "user1")
			})
		}
	}

	t.Run("storable permission before an unstorable one", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		h := &authZHandlers{
			authorizer: authorization.NewMockAuthorizer(t),
			controller: NewMockControllerAndGetUsers(t),
			logger:     logger,
		}
		res := h.addPermissions(authz.AddPermissionsParams{
			ID:          "someRole",
			HTTPRequest: req,
			Body: authz.AddPermissionsBody{Permissions: []*models.Permission{
				unstorablePermissions("fine")["users"], unstorablePermissions("a,b")["namespace"],
			}},
		}, &models.Principal{Username: "user1"})
		_, ok := res.(*authz.AddPermissionsBadRequest)
		assert.True(t, ok, "got %T", res)
		assertRefusalLogged(t, hook, "add_permissions", "user1")
	})
}

// TestLongResourceIsStorable accepts a permission whose resource, which joins
// several targets, is longer than maxTargetLength though each target fits.
func TestLongResourceIsStorable(t *testing.T) {
	perm := &models.Permission{
		Action: String(authorization.ReadData),
		Data: &models.PermissionData{
			Collection: String("C" + strings.Repeat("a", 254)),
			Tenant:     String(strings.Repeat("t", 64)),
		},
	}
	policies, err := conv.RolesToPolicies(&models.Role{Name: String("longRole"), Permissions: []*models.Permission{perm}})
	require.NoError(t, err)
	require.Greater(t, len(policies["longRole"][0].Resource), maxTargetLength, "the resource must be longer than any one target")

	t.Run("create role", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		controller := NewMockControllerAndGetUsers(t)
		logger, _ := test.NewNullLogger()
		authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil)
		controller.On("CreateRolesPermissions", policies).Return(nil)

		h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger}
		res := h.createRole(authz.CreateRoleParams{
			HTTPRequest: req,
			Body:        &models.Role{Name: String("longRole"), Permissions: []*models.Permission{perm}},
		}, &models.Principal{Username: "user1"})
		_, ok := res.(*authz.CreateRoleCreated)
		assert.True(t, ok, "got %T", res)
	})

	t.Run("add permissions", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		controller := NewMockControllerAndGetUsers(t)
		logger, _ := test.NewNullLogger()
		authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		controller.On("GetRoles", "longRole").Return(map[string][]authorization.Policy{"longRole": {}}, nil)
		controller.On("UpdateRolesPermissions", policies).Return(nil)

		h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger}
		res := h.addPermissions(authz.AddPermissionsParams{
			ID:          "longRole",
			HTTPRequest: req,
			Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{perm}},
		}, &models.Principal{Username: "user1"})
		_, ok := res.(*authz.AddPermissionsOK)
		assert.True(t, ok, "got %T", res)
	})
}

func unstorableIDs() []struct{ name, id string } {
	ids := []struct{ name, id string }{
		{name: "one byte over the 256-byte limit", id: strings.Repeat("a", 257)},
		{name: "far too long", id: strings.Repeat("a", 70*1024)},
		// casbin reads the first two fields of "g, oidc:x, role:admin, role:<assigned>".
		{name: "comma grants admin", id: "x, role:admin"},
		{name: "LF injects a row", id: "x, role:viewer\ng, db:attacker, role:admin\ng, oidc:y"},
		// Only a path parameter can carry this; a JSON body arrives as valid UTF-8.
		{name: "invalid UTF-8", id: "a\xffb"},
	}
	for _, v := range unstorableValues {
		ids = append(ids, struct{ name, id string }{name: v.name, id: v.value})
	}
	return ids
}

func TestAssignRoleToUserRejectsUnstorableID(t *testing.T) {
	for _, tt := range unstorableIDs() {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			h := &authZHandlers{
				authorizer:  authorization.NewMockAuthorizer(t),
				controller:  NewMockControllerAndGetUsers(t),
				oidcConfigs: config.OIDC{Enabled: true},
				logger:      logger,
			}
			res := h.assignRoleToUser(authz.AssignRoleToUserParams{
				ID:          tt.id,
				HTTPRequest: req,
				Body:        authz.AssignRoleToUserBody{Roles: []string{"testRole"}, UserType: models.UserTypeInputOidc},
			}, &models.Principal{Username: "user1"})
			parsed, ok := res.(*authz.AssignRoleToUserBadRequest)
			require.True(t, ok, "got %T", res)
			assert.Contains(t, parsed.Payload.Error[0].Message, "user id")
			assertRefusalLogged(t, hook, "assign_roles", "user1")
		})
	}
}

func TestAssignRoleToGroupRejectsUnstorableID(t *testing.T) {
	for _, tt := range unstorableIDs() {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			h := &authZHandlers{
				authorizer: authorization.NewMockAuthorizer(t),
				controller: NewMockControllerAndGetUsers(t),
				logger:     logger,
			}
			res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
				ID:          tt.id,
				HTTPRequest: req,
				Body:        authz.AssignRoleToGroupBody{Roles: []string{"testRole"}, GroupType: models.GroupTypeOidc},
			}, &models.Principal{Username: "user1"})
			parsed, ok := res.(*authz.AssignRoleToGroupBadRequest)
			require.True(t, ok, "got %T", res)
			assert.Contains(t, parsed.Payload.Error[0].Message, "group id")
			assertRefusalLogged(t, hook, "assign_roles", "user1")
		})
	}

	// With RBAC off and anonymous access on, no principal reaches the handler.
	t.Run("anonymous caller", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		h := &authZHandlers{
			authorizer: authorization.NewMockAuthorizer(t),
			controller: NewMockControllerAndGetUsers(t),
			logger:     logger,
		}
		res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
			ID:          "a,b",
			HTTPRequest: req,
			Body:        authz.AssignRoleToGroupBody{Roles: []string{"testRole"}, GroupType: models.GroupTypeOidc},
		}, nil)
		_, ok := res.(*authz.AssignRoleToGroupBadRequest)
		require.True(t, ok, "got %T", res)
		entry := hook.LastEntry()
		require.NotNil(t, entry, "the refusal must be logged")
		assert.NotContains(t, entry.Data, "user")
	})
}

// TestRevokeRoleFromGroupAcceptsUnstorableID checks revoke accepts an id that
// assign refuses, so a node holding such a row in memory can remove it.
func TestRevokeRoleFromGroupAcceptsUnstorableID(t *testing.T) {
	principal := &models.Principal{Username: "root-user"}
	params := authz.RevokeRoleFromGroupParams{
		ID:          "a,b",
		HTTPRequest: req,
		Body:        authz.RevokeRoleFromGroupBody{Roles: []string{"testRole"}, GroupType: models.GroupTypeOidc},
	}
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()
	authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthTypeOIDC, params.ID)[0]).Return(nil)
	controller.On("GetRoles", "testRole").Return(map[string][]authorization.Policy{"testRole": {}}, nil)
	controller.On("RevokeRolesForUser", conv.PrefixGroupName(params.ID), "testRole").Return(nil)

	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger}
	res := h.revokeRoleFromGroup(params, principal)
	_, ok := res.(*authz.RevokeRoleFromGroupOK)
	assert.True(t, ok, "got %T", res)
}

func TestRevokeRoleFromUserAcceptsUnstorableID(t *testing.T) {
	principal := &models.Principal{Username: "root-user"}
	params := authz.RevokeRoleFromUserParams{
		ID:          "a,b",
		HTTPRequest: req,
		Body:        authz.RevokeRoleFromUserBody{Roles: []string{"testRole"}, UserType: models.UserTypeInputOidc},
	}
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()
	authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(params.ID)[0]).Return(nil)
	controller.On("GetRoles", "testRole").Return(map[string][]authorization.Policy{"testRole": {}}, nil)
	controller.On("RevokeRolesForUser", conv.UserNameWithTypeFromId(params.ID, authentication.AuthTypeOIDC), "testRole").Return(nil)

	h := &authZHandlers{authorizer: authorizer, controller: controller, oidcConfigs: config.OIDC{Enabled: true}, logger: logger}
	res := h.revokeRoleFromUser(params, principal)
	_, ok := res.(*authz.RevokeRoleFromUserOK)
	assert.True(t, ok, "got %T", res)
}
