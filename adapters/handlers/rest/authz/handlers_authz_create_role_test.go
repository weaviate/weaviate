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
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestCreateRoleSuccess(t *testing.T) {
	type testCase struct {
		name      string
		principal *models.Principal
		params    authz.CreateRoleParams
	}

	tests := []testCase{
		{
			name:      "all are *",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
		},
		{
			name:      "collection checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{
								Collection: String("ABC"),
							},
						},
					},
				},
			},
		},
		{
			name:      "collection and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenants: &models.PermissionTenants{
								Collection: String("ABC"),
								Tenant:     String("Tenant1"),
							},
						},
					},
				},
			},
		},
		{
			name:      "* collections and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenants: &models.PermissionTenants{
								Tenant: String("Tenant1"),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil)
			controller.On("CreateRolesPermissions", mock.Anything).Return(nil)

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleCreated)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateRoleConflict(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{},
				},
			},
		},
	}
	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles("newRole")[0]).Return(nil)
	controller.On("GetRoles").Return(map[string][]authorization.Policy{"newRole": {}}, nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.createRole(params, principal)
	parsed, ok := res.(*authz.CreateRoleConflict)
	assert.True(t, ok)
	assert.Contains(t, parsed.Payload.Error[0].Message, fmt.Sprintf("role with name %s already exists", *params.Body.Name))
}

func TestCreateRoleBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "role name is required",
		},
		{
			name: "invalid role name",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("something/wrong"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "role name is invalid",
		},
		{
			name: "invalid permission",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_something"),
						},
					},
				},
			},
			expectedError: "invalid role",
		},
		{
			name: "cannot create role with the same name as builtin role",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: &authorization.BuiltInRoles[0],
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "you cannot create role with the same name as built-in role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			if tt.expectedError == "" {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil)
				controller.On("upsertRolesPermissions", mock.Anything).Return(tt.upsertErr)
			}

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.createRole(tt.params, nil)
			parsed, ok := res.(*authz.CreateRoleBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestCreateRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  errors.New("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestCreateRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "upsert roles permissions error",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			upsertErr:     errors.New("upsert error"),
			expectedError: "upsert error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(tt.params.Body)
			require.Nil(t, err)

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
			controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil)
			controller.On("CreateRolesPermissions", policies).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestCreateRoleUnprocessableRegexp(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	params := authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("/[a-z+/")},
				},
			},
		},
	}

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.createRole(params, &models.Principal{Username: "user1"})
	_, ok := res.(*authz.CreateRoleUnprocessableEntity)
	assert.True(t, ok)
	assert.Contains(t, res.(*authz.CreateRoleUnprocessableEntity).Payload.Error[0].Message, "role permissions are invalid")
}

func String(s string) *string {
	return &s
}

// collectionRole builds a CreateRoleParams granting create_collections on the
// given collection (empty collection means the `*` wildcard).
func collectionRole(name, collection string) authz.CreateRoleParams {
	perm := &models.Permission{
		Action:      String(authorization.CreateCollections),
		Collections: &models.PermissionCollections{},
	}
	if collection != "" {
		perm.Collections.Collection = String(collection)
	}
	return authz.CreateRoleParams{
		HTTPRequest: req,
		Body:        &models.Role{Name: String(name), Permissions: []*models.Permission{perm}},
	}
}

// TestCreateRoleOperatorReservedPrefix pins the reservation: only confined
// (namespaced) callers are blocked from operator_/global_ role names; global
// operators may create them and NS-disabled clusters treat them as ordinary names.
func TestCreateRoleOperatorReservedPrefix(t *testing.T) {
	tests := []struct {
		name              string
		principal         *models.Principal
		roleName          string
		namespacesEnabled bool
		wantResp          string
	}{
		{
			name:              "namespaced caller operator_ prefix rejected",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			roleName:          "operator_foo",
			namespacesEnabled: true,
			wantResp:          "422",
		},
		{
			name:              "namespaced caller global_ prefix rejected",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			roleName:          "global_foo",
			namespacesEnabled: true,
			wantResp:          "422",
		},
		{
			name:              "namespaced caller unreserved name allowed",
			principal:         &models.Principal{Username: "u", Namespace: "customer1"},
			roleName:          "billing-ro",
			namespacesEnabled: true,
			wantResp:          "created",
		},
		{
			name:              "global operator operator_ prefix allowed",
			principal:         &models.Principal{Username: "op", IsGlobalOperator: true},
			roleName:          "operator_foo",
			namespacesEnabled: true,
			wantResp:          "created",
		},
		{
			name:              "NS-disabled operator_ prefix allowed",
			principal:         &models.Principal{Username: "u"},
			roleName:          "operator_foo",
			namespacesEnabled: false,
			wantResp:          "created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil).Maybe()
			controller.On("CreateRolesPermissions", mock.Anything).Return(nil).Maybe()

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				schemaReader:      schemaReader,
				logger:            logger,
				namespacesEnabled: tt.namespacesEnabled,
			}
			res := h.createRole(collectionRole(tt.roleName, "Movies"), tt.principal)
			require.Equal(t, tt.wantResp, createRoleRespType(res))
		})
	}
}

func createRoleRespType(res middleware.Responder) string {
	switch res.(type) {
	case *authz.CreateRoleCreated:
		return "created"
	case *authz.CreateRoleUnprocessableEntity:
		return "422"
	case *authz.CreateRoleConflict:
		return "409"
	case *authz.CreateRoleBadRequest:
		return "400"
	case *authz.CreateRoleForbidden:
		return "403"
	default:
		return fmt.Sprintf("%T", res)
	}
}

func TestCreateRoleNamespaced(t *testing.T) {
	tests := []struct {
		name          string
		principal     *models.Principal
		params        authz.CreateRoleParams
		existingRoles []string
		wantResp      string
		wantStoredKey string // when "created", the role name CreateRolesPermissions must be keyed by
		wantResource  string // substring the stored policy resource must contain
	}{
		{
			name:          "namespaced short name and collection auto-qualify",
			principal:     &models.Principal{Username: "u", Namespace: "customer1"},
			params:        collectionRole("editor", "Movies"),
			wantResp:      "created",
			wantStoredKey: "customer1:editor",
			wantResource:  "customer1:Movies",
		},
		{
			name:      "namespaced foreign collection rejected",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			params:    collectionRole("editor", "customer2:Movies"),
			wantResp:  "422",
		},
		{
			name:      "namespaced own pre-qualified collection rejected",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			params:    collectionRole("editor", "customer1:Movies"),
			wantResp:  "422",
		},
		{
			// validateRoleName's regex forbids ':', so this is 400 not 422.
			name:      "namespaced colon in role name rejected",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			params:    collectionRole("customer1:editor", "Movies"),
			wantResp:  "400",
		},
		{
			name:      "namespaced built-in name rejected",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			params:    collectionRole("admin", "Movies"),
			wantResp:  "400",
		},
		{
			name:          "collision with global short name rejected",
			principal:     &models.Principal{Username: "u", Namespace: "customer1"},
			params:        collectionRole("editor", "Movies"),
			existingRoles: []string{"editor"},
			wantResp:      "409",
		},
		{
			name:          "collision with same-namespace role rejected",
			principal:     &models.Principal{Username: "u", Namespace: "customer1"},
			params:        collectionRole("editor", "Movies"),
			existingRoles: []string{"customer1:editor"},
			wantResp:      "409",
		},
		{
			name:          "same short name in another namespace allowed",
			principal:     &models.Principal{Username: "u", Namespace: "customer1"},
			params:        collectionRole("editor", "Movies"),
			existingRoles: []string{"customer2:editor"},
			wantResp:      "created",
			wantStoredKey: "customer1:editor",
			wantResource:  "customer1:Movies",
		},
		{
			name:          "global creator unprefixed name allowed",
			principal:     &models.Principal{Username: "op"},
			params:        collectionRole("editor", "Movies"),
			wantResp:      "created",
			wantStoredKey: "editor",
			wantResource:  "schema/collections/Movies",
		},
		{
			name:          "global creator colliding with existing local role rejected",
			principal:     &models.Principal{Username: "op"},
			params:        collectionRole("editor", "Movies"),
			existingRoles: []string{"customer1:editor"},
			wantResp:      "409",
		},
		{
			name:      "global creator qualified name rejected",
			principal: &models.Principal{Username: "op"},
			params:    collectionRole("customer1:editor", "Movies"),
			wantResp:  "400",
		},
		{
			name:      "global creator qualified policy still rejected (carve-out is namespaced-only)",
			principal: &models.Principal{Username: "op"},
			params:    collectionRole("editor", "customer1:Movies"),
			wantResp:  "422",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			// A namespaced creator authorizes at MATCH scope, then each policy is
			// checked ≤-effective; grant both so qualify/collision stay the focus.
			authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			all := map[string][]authorization.Policy{}
			for _, r := range tt.existingRoles {
				all[r] = []authorization.Policy{}
			}
			controller.On("GetRoles").Return(all, nil).Maybe()
			var stored map[string][]authorization.Policy
			controller.On("CreateRolesPermissions", mock.Anything).Run(func(args mock.Arguments) {
				stored = args.Get(0).(map[string][]authorization.Policy)
			}).Return(nil).Maybe()

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				schemaReader:      schemaReader,
				logger:            logger,
				namespacesEnabled: true,
			}
			res := h.createRole(tt.params, tt.principal)
			require.Equal(t, tt.wantResp, createRoleRespType(res))

			if tt.wantResp == "created" {
				require.NotNil(t, stored)
				require.Contains(t, stored, tt.wantStoredKey)
				found := false
				for _, p := range stored[tt.wantStoredKey] {
					if strings.Contains(p.Resource, tt.wantResource) {
						found = true
					}
				}
				assert.True(t, found, "stored policy resource should contain %q, got %+v", tt.wantResource, stored[tt.wantStoredKey])
			}
		})
	}
}

// TestCreateRoleNamespacedEffectiveDeny pins the per-permission ≤-effective
// MATCH check: a namespaced caller that may manage roles at MATCH scope but does
// not itself hold a submitted (own-namespace) permission gets 403, not a role.
// The table above grants every AuthorizeSilent, so the deny arm needs its own
// authorizer.
func TestCreateRoleNamespacedEffectiveDeny(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	// May manage roles at MATCH scope...
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH),
		mock.Anything).Return(nil)
	// ...but does not hold the data permission it is trying to grant.
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("forbidden"))
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil).Maybe()

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		namespacesEnabled: true,
	}

	principal := &models.Principal{Username: "u", Namespace: "customer1"}
	res := h.createRole(collectionRole("editor", "Movies"), principal)
	require.Equal(t, "403", createRoleRespType(res))
}
