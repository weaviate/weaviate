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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

var fullRoles = true

// legacyHeaders are the header values that select the legacy view. An empty
// value sends no header.
var legacyHeaders = []struct {
	name   string
	header string
}{
	{name: "no header", header: ""},
	{name: "header false", header: "false"},
}

func withHeader(value string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/authz/roles", nil)
	if value != "" {
		r.Header.Set(conv.AcceptBackupPrincipalsHeader, value)
	}
	return r
}

var backupsCaller = &models.Principal{Username: "caller", UserType: models.UserTypeInputDb}

// backupsAuthorizer allows every check that deny does not match and records
// each Authorize call as "<verb> <resources>".
type backupsAuthorizer struct {
	deny       func(verb, resource string) bool
	authorized []string
}

func (a *backupsAuthorizer) check(principal *models.Principal, verb string, resources []string) error {
	for _, r := range resources {
		if a.deny != nil && a.deny(verb, r) {
			return authzerrors.NewForbidden(principal, verb, r)
		}
	}
	return nil
}

func (a *backupsAuthorizer) Authorize(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.authorized = append(a.authorized, verb+" "+strings.Join(resources, ","))
	return a.check(principal, verb, resources)
}

func (a *backupsAuthorizer) AuthorizeAndRequireActiveNamespace(_ context.Context, principal *models.Principal, verb string, _ string, resources ...string) error {
	return a.check(principal, verb, resources)
}

func (a *backupsAuthorizer) AuthorizeSilent(_ context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.check(principal, verb, resources)
}

func (a *backupsAuthorizer) FilterAuthorizedResources(_ context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	var allowed []string
	for _, r := range resources {
		if a.check(principal, verb, []string{r}) == nil {
			allowed = append(allowed, r)
		}
	}
	return allowed, nil
}

// matchScopeCaller manages roleName only at MATCH scope and lacks every
// resource in lacking.
func matchScopeCaller(roleName string, lacking ...string) *backupsAuthorizer {
	return &backupsAuthorizer{deny: func(verb, resource string) bool {
		if resource == authorization.Roles(roleName)[0] && strings.HasSuffix(verb, "_"+authorization.ROLE_SCOPE_ALL) {
			return true
		}
		return slices.Contains(lacking, resource)
	}}
}

func backupsPermission(action string, target *models.PermissionBackups) *models.Permission {
	return &models.Permission{Action: &action, Backups: target}
}

func backupsCollection(action, collection string) *models.Permission {
	return backupsPermission(action, &models.PermissionBackups{Collection: &collection})
}

func toPolicies(t *testing.T, perms ...*models.Permission) []authorization.Policy {
	t.Helper()
	ptrs, err := conv.PermissionToPolicies(perms...)
	require.NoError(t, err)
	out := make([]authorization.Policy, len(ptrs))
	for i, p := range ptrs {
		out[i] = *p
	}
	return out
}

// backupsGrants names each backups permission as "<action> <target>=<id>".
func backupsGrants(perms []*models.Permission) []string {
	var out []string
	for _, p := range perms {
		if p.Action == nil || !strings.HasSuffix(*p.Action, "_backups") || p.Backups == nil {
			continue
		}
		switch b := p.Backups; {
		case b.User != nil:
			out = append(out, *p.Action+" user="+*b.User)
		case b.Role != nil:
			out = append(out, *p.Action+" role="+*b.Role)
		case b.Collection != nil:
			out = append(out, *p.Action+" collection="+*b.Collection)
		}
	}
	return out
}

func grantsByRole(roles []*models.Role) map[string][]string {
	out := map[string][]string{}
	for _, r := range roles {
		out[*r.Name] = backupsGrants(r.Permissions)
	}
	return out
}

// storedRoles returns the GetRoles result that the rbac manager builds from
// the rows of custom and builtIns. A built-in role displays
// BuiltInPermissionsFor, whatever its row holds.
func storedRoles(t *testing.T, namespacesEnabled bool, custom map[string][]authorization.Policy, builtIns ...string) map[string][]authorization.Policy {
	t.Helper()
	var rows [][]string
	for name, policies := range custom {
		for _, p := range policies {
			rows = append(rows, []string{conv.PrefixRoleName(name), p.Resource, p.Verb, p.Domain})
		}
	}
	for _, name := range builtIns {
		rows = append(rows, []string{conv.PrefixRoleName(name), "*", authorization.READ, "*"})
	}
	logger, _ := test.NewNullLogger()
	roles, err := conv.CasbinPolicies(logger, namespacesEnabled, rows)
	require.NoError(t, err)
	return roles
}

// backupsHandler serves roles from every role lookup: the list, a lookup by
// name, user "target" (a static API-key user) and OIDC group "group".
func backupsHandler(t *testing.T, namespacesEnabled bool, authorizer authorization.Authorizer, roles map[string][]authorization.Policy) *authZHandlers {
	t.Helper()
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles").Return(roles, nil).Maybe()
	controller.On("GetRoles", mock.Anything).Return(func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		for _, name := range names {
			if p, ok := roles[name]; ok {
				out[name] = p
			}
		}
		return out
	}, nil).Maybe()
	controller.On("GetRolesForUserOrGroup", "target", authentication.AuthTypeDb, false).Return(roles, nil).Maybe()
	controller.On("GetRolesForUserOrGroup", "target", authentication.AuthTypeOIDC, false).Return(map[string][]authorization.Policy{}, nil).Maybe()
	controller.On("GetRolesForUserOrGroup", "group", authentication.AuthTypeOIDC, true).Return(roles, nil).Maybe()
	controller.On("AddRolesForUser", mock.Anything, mock.Anything).Return(nil).Maybe()

	logger, _ := test.NewNullLogger()
	return &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		apiKeysConfigs:    config.StaticAPIKey{Enabled: true, Users: []string{"target"}},
		rbacconfig:        rbacconf.Config{Enabled: true, RootUsers: []string{backupsCaller.Username}},
		namespacesEnabled: namespacesEnabled,
	}
}

// roleReadEndpoints are the authz operations that return role permissions.
// Each read returns every role the handler serves.
var roleReadEndpoints = []struct {
	name string
	read func(t *testing.T, h *authZHandlers, roleNames []string, header string) []*models.Role
}{
	{
		name: "getRoles",
		read: func(t *testing.T, h *authZHandlers, _ []string, header string) []*models.Role {
			res := h.getRoles(authz.GetRolesParams{HTTPRequest: withHeader(header)}, backupsCaller)
			parsed, ok := res.(*authz.GetRolesOK)
			require.True(t, ok, "got %T", res)
			return parsed.Payload
		},
	},
	{
		name: "getRole",
		read: func(t *testing.T, h *authZHandlers, roleNames []string, header string) []*models.Role {
			var out []*models.Role
			for _, name := range roleNames {
				res := h.getRole(authz.GetRoleParams{HTTPRequest: withHeader(header), ID: name}, backupsCaller)
				parsed, ok := res.(*authz.GetRoleOK)
				require.True(t, ok, "%s: got %T", name, res)
				out = append(out, parsed.Payload)
			}
			return out
		},
	},
	{
		name: "getRolesForUser with full roles",
		read: func(t *testing.T, h *authZHandlers, _ []string, header string) []*models.Role {
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				HTTPRequest: withHeader(header), ID: "target", UserType: string(models.UserTypeInputDb),
				IncludeFullRoles: &fullRoles,
			}, backupsCaller)
			parsed, ok := res.(*authz.GetRolesForUserOK)
			require.True(t, ok, "got %T", res)
			return parsed.Payload
		},
	},
	{
		name: "getRolesForGroup with full roles",
		read: func(t *testing.T, h *authZHandlers, _ []string, header string) []*models.Role {
			res := h.getRolesForGroup(authz.GetRolesForGroupParams{
				HTTPRequest: withHeader(header), ID: "group", GroupType: string(models.GroupTypeOidc),
				IncludeFullRoles: &fullRoles,
			}, backupsCaller)
			parsed, ok := res.(*authz.GetRolesForGroupOK)
			require.True(t, ok, "got %T", res)
			return parsed.Payload
		},
	},
	{
		name: "getRolesForUserDeprecated",
		read: func(t *testing.T, h *authZHandlers, _ []string, header string) []*models.Role {
			res := h.getRolesForUserDeprecated(authz.GetRolesForUserDeprecatedParams{
				HTTPRequest: withHeader(header), ID: "target",
			}, backupsCaller)
			parsed, ok := res.(*authz.GetRolesForUserDeprecatedOK)
			require.True(t, ok, "got %T", res)
			return parsed.Payload
		},
	},
}

func TestBackupPrincipalsReadView(t *testing.T) {
	custom := map[string][]authorization.Policy{"backup-role": toPolicies(t,
		backupsCollection(authorization.ManageBackups, "Movies"),
		backupsPermission(authorization.ManageBackups, authorization.AllBackupUsers),
		backupsPermission(authorization.ManageBackups, &models.PermissionBackups{Role: authorization.String("editor")}),
	)}
	builtIns := []string{authorization.Admin, authorization.Root, authorization.MetadataReader, authorization.Viewer, authorization.ReadOnly}
	roles := storedRoles(t, false, custom, builtIns...)
	roleNames := append([]string{"backup-role"}, builtIns...)

	for _, endpoint := range roleReadEndpoints {
		for _, legacy := range legacyHeaders {
			t.Run(endpoint.name+", "+legacy.name+": users and roles grants are omitted", func(t *testing.T) {
				h := backupsHandler(t, false, &backupsAuthorizer{}, roles)
				got := grantsByRole(endpoint.read(t, h, roleNames, legacy.header))
				require.Len(t, got, len(roleNames))
				for name, grants := range got {
					for _, g := range grants {
						assert.NotContains(t, g, " user=", name)
						assert.NotContains(t, g, " role=", name)
					}
				}
				assert.Equal(t, []string{"manage_backups collection=Movies"}, got["backup-role"])
				assert.Equal(t, []string{"read_backups collection=*"}, got[authorization.Viewer])
			})
		}

		t.Run(endpoint.name+", header true: users and roles grants are returned", func(t *testing.T) {
			h := backupsHandler(t, false, &backupsAuthorizer{}, roles)
			got := grantsByRole(endpoint.read(t, h, roleNames, "true"))
			assert.ElementsMatch(t, []string{
				"manage_backups collection=Movies", "manage_backups user=*", "manage_backups role=editor",
			}, got["backup-role"])
			assert.ElementsMatch(t, []string{
				"read_backups collection=*", "read_backups user=*", "read_backups role=*",
			}, got[authorization.Viewer])
			for role, action := range map[string]string{
				authorization.Admin:          authorization.ManageBackups,
				authorization.Root:           authorization.ManageBackups,
				authorization.MetadataReader: authorization.ReadBackups,
				authorization.ReadOnly:       authorization.ReadBackups,
			} {
				assert.Contains(t, got[role], action+" user=*", role)
				assert.Contains(t, got[role], action+" role=*", role)
			}
		})
	}
}

func TestLegacyResponseSatisfiesReleasedClients(t *testing.T) {
	custom := map[string][]authorization.Policy{"backup-role": toPolicies(t,
		backupsCollection(authorization.ManageBackups, "Movies"),
		backupsPermission(authorization.ManageBackups, authorization.AllBackupUsers),
		backupsPermission(authorization.ManageBackups, authorization.AllBackupRoles),
	)}
	roles := storedRoles(t, false, custom, authorization.Admin)
	roleNames := []string{"backup-role", authorization.Admin}

	for _, endpoint := range roleReadEndpoints[:2] {
		t.Run(endpoint.name+": every backups object has a collection key", func(t *testing.T) {
			h := backupsHandler(t, false, &backupsAuthorizer{}, roles)
			body, err := json.Marshal(endpoint.read(t, h, roleNames, ""))
			require.NoError(t, err)

			var decoded []map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			require.Len(t, decoded, len(roleNames))
			seen := 0
			for _, role := range decoded {
				perms, _ := role["permissions"].([]any)
				for _, p := range perms {
					backups, ok := p.(map[string]any)["backups"]
					if !ok {
						continue
					}
					seen++
					obj, ok := backups.(map[string]any)
					require.True(t, ok, "role %v: backups is %T", role["name"], backups)
					assert.Contains(t, obj, "collection", "role %v: %v", role["name"], obj)
				}
			}
			assert.NotZero(t, seen)
		})
	}
}

// backupsWrites records what a write handler sent to the raft boundary.
type backupsWrites struct {
	created, updated map[string][]authorization.Policy
	removed          []authorization.Policy
	removeCalls      int
}

// writeHandler serves stored as the roles a write reads, with no other roles.
func writeHandler(t *testing.T, authorizer authorization.Authorizer, stored map[string][]authorization.Policy) (*authZHandlers, *backupsWrites) {
	t.Helper()
	writes := &backupsWrites{}
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil).Maybe()
	controller.On("GetRoles", mock.Anything).Return(func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		if p, ok := stored[names[0]]; ok {
			out[names[0]] = p
		}
		return out
	}, nil).Maybe()
	controller.On("CreateRolesPermissions", mock.Anything).Run(func(args mock.Arguments) {
		writes.created = args.Get(0).(map[string][]authorization.Policy)
	}).Return(nil).Maybe()
	controller.On("UpdateRolesPermissions", mock.Anything).Run(func(args mock.Arguments) {
		writes.updated = args.Get(0).(map[string][]authorization.Policy)
	}).Return(nil).Maybe()
	controller.On("RemovePermissions", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		writes.removeCalls++
		writes.removed = nil
		for _, p := range args.Get(1).([]*authorization.Policy) {
			writes.removed = append(writes.removed, *p)
		}
	}).Return(nil).Maybe()

	logger, _ := test.NewNullLogger()
	return &authZHandlers{authorizer: authorizer, controller: controller, logger: logger}, writes
}

func countCalls(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}

func TestLegacyBackupPrincipalWrites(t *testing.T) {
	const roleName = "backup-role"
	manageMovies := backupsCollection(authorization.ManageBackups, "Movies")
	manageBooks := backupsCollection(authorization.ManageBackups, "Books")
	readBooks := backupsCollection(authorization.ReadBackups, "Books")
	manageUsers := backupsPermission(authorization.ManageBackups, authorization.AllBackupUsers)
	readSchema := &models.Permission{Action: authorization.String(authorization.ReadCollections), Collections: authorization.AllCollections}

	policy := func(p *models.Permission) authorization.Policy { return toPolicies(t, p)[0] }
	wildcards := func(p *models.Permission) []authorization.Policy { return conv.BackupPrincipalGrants(policy(p)) }
	manageWildcards := wildcards(manageMovies)
	readWildcards := wildcards(readBooks)
	allowAll := func() *backupsAuthorizer { return &backupsAuthorizer{} }
	updateAll := authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL) + " " + authorization.Roles(roleName)[0]

	create := func(h *authZHandlers, header string, perms ...*models.Permission) middleware.Responder {
		return h.createRole(authz.CreateRoleParams{
			HTTPRequest: withHeader(header),
			Body:        &models.Role{Name: authorization.String(roleName), Permissions: perms},
		}, backupsCaller)
	}
	add := func(h *authZHandlers, header string, perms ...*models.Permission) middleware.Responder {
		return h.addPermissions(authz.AddPermissionsParams{
			HTTPRequest: withHeader(header), ID: roleName,
			Body: authz.AddPermissionsBody{Permissions: perms},
		}, backupsCaller)
	}
	remove := func(h *authZHandlers, header string, perms ...*models.Permission) middleware.Responder {
		return h.removePermissions(authz.RemovePermissionsParams{
			HTTPRequest: withHeader(header), ID: roleName,
			Body: authz.RemovePermissionsBody{Permissions: perms},
		}, backupsCaller)
	}

	for _, legacy := range legacyHeaders {
		t.Run(legacy.name+": create derives both wildcards per verb", func(t *testing.T) {
			h, writes := writeHandler(t, allowAll(), nil)
			res := create(h, legacy.header, manageMovies, readBooks, manageUsers)
			require.IsType(t, &authz.CreateRoleCreated{}, res)
			assert.ElementsMatch(t, append(toPolicies(t, manageMovies, readBooks, manageUsers), manageWildcards[1], readWildcards[0], readWildcards[1]),
				writes.created[roleName])
		})

		t.Run(legacy.name+": create derives nothing from a users backup grant", func(t *testing.T) {
			alice := "alice"
			manageAlice := backupsPermission(authorization.ManageBackups, &models.PermissionBackups{User: &alice})
			h, writes := writeHandler(t, allowAll(), nil)
			res := create(h, legacy.header, manageAlice, readSchema)
			require.IsType(t, &authz.CreateRoleCreated{}, res)
			assert.Equal(t, toPolicies(t, manageAlice, readSchema), writes.created[roleName])
		})

		t.Run(legacy.name+": add derives both wildcards", func(t *testing.T) {
			h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: toPolicies(t, readSchema)})
			res := add(h, legacy.header, manageMovies)
			require.IsType(t, &authz.AddPermissionsOK{}, res)
			assert.ElementsMatch(t, append(toPolicies(t, manageMovies), manageWildcards...), writes.updated[roleName])
		})

		t.Run(legacy.name+": removing the last collections grant for a verb removes its stored wildcards", func(t *testing.T) {
			auth := allowAll()
			stored := append(toPolicies(t, manageMovies, readSchema), manageWildcards...)
			h, writes := writeHandler(t, auth, map[string][]authorization.Policy{roleName: stored})
			res := remove(h, legacy.header, manageMovies)
			require.IsType(t, &authz.RemovePermissionsOK{}, res)
			assert.ElementsMatch(t, append(toPolicies(t, manageMovies), manageWildcards...), writes.removed)
			assert.Equal(t, 2, countCalls(auth.authorized, updateAll), "the extras are authorized")
		})
	}

	t.Run("header true: create and add store exactly the request", func(t *testing.T) {
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: toPolicies(t, readSchema)})
		require.IsType(t, &authz.CreateRoleCreated{}, create(h, "true", manageMovies, readBooks))
		assert.Equal(t, toPolicies(t, manageMovies, readBooks), writes.created[roleName])
		require.IsType(t, &authz.AddPermissionsOK{}, add(h, "true", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.updated[roleName])
	})

	t.Run("removing every collections grant for a verb at once removes its wildcards", func(t *testing.T) {
		stored := append(toPolicies(t, manageMovies, manageBooks), manageWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies, manageBooks))
		assert.ElementsMatch(t, stored, writes.removed)
	})

	t.Run("another stored same-verb collections grant keeps the wildcards", func(t *testing.T) {
		stored := append(toPolicies(t, manageMovies, manageBooks), manageWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
	})

	t.Run("a different-verb collections grant does not keep them", func(t *testing.T) {
		stored := append(append(toPolicies(t, manageMovies, readBooks), manageWildcards...), readWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies))
		assert.ElementsMatch(t, append(toPolicies(t, manageMovies), manageWildcards...), writes.removed)
	})

	t.Run("removing an unstored collections grant removes nothing extra", func(t *testing.T) {
		stored := append(toPolicies(t, readSchema), manageWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
	})

	t.Run("a role storing no wildcards gets no extras and no extra authorization", func(t *testing.T) {
		auth := allowAll()
		h, writes := writeHandler(t, auth, map[string][]authorization.Policy{roleName: toPolicies(t, manageMovies, readSchema)})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
		assert.Equal(t, 1, countCalls(auth.authorized, updateAll))
	})

	t.Run("header true: remove removes exactly the request", func(t *testing.T) {
		stored := append(toPolicies(t, manageMovies), manageWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "true", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
	})

	unmigrated := map[string][]authorization.Policy{roleName: toPolicies(t, readSchema)}
	migrated := map[string][]authorization.Policy{roleName: append(toPolicies(t, manageMovies), manageWildcards...)}

	for _, missing := range []struct{ name, resource string }{
		{name: "the users wildcard", resource: conv.CasbinBackupUsers("*")},
		{name: "the roles wildcard", resource: conv.CasbinBackupRoles("*")},
	} {
		for _, legacy := range legacyHeaders {
			t.Run(legacy.name+": a match-scope caller without "+missing.name+" is refused a legacy create", func(t *testing.T) {
				h, writes := writeHandler(t, matchScopeCaller(roleName, missing.resource), nil)
				res := create(h, legacy.header, manageMovies)
				forbidden, ok := res.(*authz.CreateRoleForbidden)
				require.True(t, ok, "got %T", res)
				assert.Contains(t, forbidden.Payload.Error[0].Message, missing.resource)
				assert.Nil(t, writes.created)
			})

			t.Run(legacy.name+": a match-scope caller without "+missing.name+" is refused a legacy add", func(t *testing.T) {
				h, writes := writeHandler(t, matchScopeCaller(roleName, missing.resource), unmigrated)
				res := add(h, legacy.header, manageMovies)
				forbidden, ok := res.(*authz.AddPermissionsForbidden)
				require.True(t, ok, "got %T", res)
				assert.Contains(t, forbidden.Payload.Error[0].Message, missing.resource)
				assert.Nil(t, writes.updated)
			})

			t.Run(legacy.name+": a match-scope caller without "+missing.name+" is refused a legacy remove of a migrated role's last collections grant", func(t *testing.T) {
				h, writes := writeHandler(t, matchScopeCaller(roleName, missing.resource), migrated)
				res := remove(h, legacy.header, manageMovies)
				forbidden, ok := res.(*authz.RemovePermissionsForbidden)
				require.True(t, ok, "got %T", res)
				assert.Contains(t, forbidden.Payload.Error[0].Message, missing.resource)
				assert.Zero(t, writes.removeCalls)
			})
		}

		t.Run("header true: a match-scope caller without "+missing.name+" may create, add and remove the request alone", func(t *testing.T) {
			h, writes := writeHandler(t, matchScopeCaller(roleName, missing.resource), nil)
			require.IsType(t, &authz.CreateRoleCreated{}, create(h, "true", manageMovies))
			assert.Equal(t, toPolicies(t, manageMovies), writes.created[roleName])

			h, writes = writeHandler(t, matchScopeCaller(roleName, missing.resource), unmigrated)
			require.IsType(t, &authz.AddPermissionsOK{}, add(h, "true", manageMovies))
			assert.Equal(t, toPolicies(t, manageMovies), writes.updated[roleName])

			h, writes = writeHandler(t, matchScopeCaller(roleName, missing.resource), migrated)
			require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "true", manageMovies))
			assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
		})
	}

	t.Run("two removes read from one snapshot each keep the wildcards", func(t *testing.T) {
		stored := append(toPolicies(t, manageMovies, manageBooks), manageWildcards...)
		h, writes := writeHandler(t, allowAll(), map[string][]authorization.Policy{roleName: stored})
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageMovies))
		assert.Equal(t, toPolicies(t, manageMovies), writes.removed)
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "", manageBooks))
		assert.Equal(t, toPolicies(t, manageBooks), writes.removed)

		// Once both removes apply, the role holds only the wildcards. A capable
		// read shows them, and a capable remove clears them.
		left := map[string][]authorization.Policy{roleName: manageWildcards}
		read := backupsHandler(t, false, allowAll(), storedRoles(t, false, left))
		parsed, ok := read.getRole(authz.GetRoleParams{HTTPRequest: withHeader("true"), ID: roleName}, backupsCaller).(*authz.GetRoleOK)
		require.True(t, ok)
		assert.ElementsMatch(t, []string{"manage_backups user=*", "manage_backups role=*"}, backupsGrants(parsed.Payload.Permissions))

		h, writes = writeHandler(t, allowAll(), left)
		require.IsType(t, &authz.RemovePermissionsOK{}, remove(h, "true",
			manageUsers, backupsPermission(authorization.ManageBackups, authorization.AllBackupRoles)))
		assert.ElementsMatch(t, manageWildcards, writes.removed)
	})
}

func TestBuiltInBackupPrincipalReadsGateAssignAndVisibility(t *testing.T) {
	lacksBackupUsers := &backupsAuthorizer{deny: func(_, resource string) bool { return resource == conv.CasbinBackupUsers("*") }}

	assignToUser := func(h *authZHandlers) any {
		return h.assignRoleToUser(authz.AssignRoleToUserParams{
			HTTPRequest: req, ID: "target",
			Body: authz.AssignRoleToUserBody{Roles: []string{authorization.Viewer}, UserType: models.UserTypeInputDb},
		}, backupsCaller)
	}
	assignToGroup := func(h *authZHandlers) any {
		return h.assignRoleToGroup(authz.AssignRoleToGroupParams{
			HTTPRequest: req, ID: "group",
			Body: authz.AssignRoleToGroupBody{Roles: []string{authorization.Viewer}, GroupType: models.GroupTypeOidc},
		}, backupsCaller)
	}

	t.Run("assigning viewer to a user needs both backup reads", func(t *testing.T) {
		roles := storedRoles(t, false, nil, authorization.Viewer)
		assert.IsType(t, &authz.AssignRoleToUserForbidden{}, assignToUser(backupsHandler(t, false, lacksBackupUsers, roles)))
		assert.IsType(t, &authz.AssignRoleToUserOK{}, assignToUser(backupsHandler(t, false, &backupsAuthorizer{}, roles)))
	})

	t.Run("assigning viewer to a group needs both backup reads", func(t *testing.T) {
		roles := storedRoles(t, false, nil, authorization.Viewer)
		assert.IsType(t, &authz.AssignRoleToGroupForbidden{}, assignToGroup(backupsHandler(t, false, lacksBackupUsers, roles)))
		assert.IsType(t, &authz.AssignRoleToGroupOK{}, assignToGroup(backupsHandler(t, false, &backupsAuthorizer{}, roles)))
	})

	t.Run("namespaces on, read-only is hidden from a non-ALL reader lacking the wildcards", func(t *testing.T) {
		readAll := authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL)
		notAllReader := func(lacking ...string) *backupsAuthorizer {
			return &backupsAuthorizer{deny: func(verb, resource string) bool {
				return (verb == readAll && resource == authorization.Roles()[0]) || slices.Contains(lacking, resource)
			}}
		}
		roles := storedRoles(t, true, nil, authorization.ReadOnly)
		names := func(a *backupsAuthorizer) []string {
			res := backupsHandler(t, true, a, roles).getRolesForUser(authz.GetRolesForUserParams{
				HTTPRequest: req, ID: "target", UserType: string(models.UserTypeInputDb),
			}, backupsCaller)
			parsed, ok := res.(*authz.GetRolesForUserOK)
			require.True(t, ok, "got %T", res)
			return roleNames(parsed.Payload)
		}
		assert.Empty(t, names(notAllReader(conv.CasbinBackupRoles("*"))))
		assert.Equal(t, []string{authorization.ReadOnly}, names(notAllReader()))
	})
}
