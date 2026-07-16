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
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestGetRolesForUserSuccess(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	policies := []authorization.Policy{
		{
			Resource: authorization.Collections("ABC")[0],
			Verb:     authorization.READ,
			Domain:   authorization.SchemaDomain,
		},
	}
	userType := models.UserTypeInputDb
	returnedPolices := map[string][]authorization.Policy{
		"testRole": policies,
	}
	truep := true
	falseP := false
	tests := []struct {
		name        string
		params      authz.GetRolesForUserParams
		principal   *models.Principal
		expectAuthz bool
	}{
		{
			name: "success",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				HTTPRequest:      req,
				IncludeFullRoles: &truep,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success without roles",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success for own user",
			params: authz.GetRolesForUserParams{
				ID:               "user1",
				UserType:         string(userType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectAuthz {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)
				if *tt.params.IncludeFullRoles {
					authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(nil)
				}
			}
			controller.On("GetRolesForUserOrGroup", tt.params.ID, authentication.AuthTypeDb, false).Return(returnedPolices, nil)
			controller.On("GetUsers", tt.params.ID).Return(map[string]apikey.UserView{"testUser": {}}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			permissions, err := conv.PoliciesToPermission(policies...)
			assert.Nil(t, err)

			roles := []*models.Role{
				{
					Name:        String("testRole"),
					Permissions: permissions,
				},
			}
			expectedRoles := models.RolesListResponse(roles)
			if *tt.params.IncludeFullRoles {
				assert.Equal(t, expectedRoles, parsed.Payload)
			} else {
				assert.Nil(t, parsed.Payload[0].Permissions)
			}
			assert.Equal(t, *roles[0].Name, *parsed.Payload[0].Name)
		})
	}
}

func TestGetRolesForUserForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForUserParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}
	truep := true
	userType := models.UserTypeInputDb
	tests := []testCase{
		{
			name: "authorization error no access to role",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:     &models.Principal{Username: "user1", UserType: userType},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			returnedPolices := map[string][]authorization.Policy{
				"testRole": {
					{
						Resource: authorization.Collections("ABC")[0],
						Verb:     authorization.READ,
						Domain:   authorization.SchemaDomain,
					},
				},
			}
			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			}
			controller.On("GetRolesForUserOrGroup", tt.params.ID, authentication.AuthType(userType), false).Return(returnedPolices, nil)
			controller.On("GetUsers", tt.params.ID).Return(map[string]apikey.UserView{tt.params.ID: {}}, nil)
			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetRolesForUserInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForUserParams
		principal     *models.Principal
		getRolesErr   error
		expectedError string
	}

	userType := models.UserTypeInputDb
	tests := []testCase{
		{
			name: "internal server error",
			params: authz.GetRolesForUserParams{
				ID:          "testUser",
				UserType:    string(userType),
				HTTPRequest: req,
			},
			principal:     &models.Principal{Username: "user1", UserType: userType},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRolesForUserOrGroup", tt.params.ID, authentication.AuthType(userType), false).Return(nil, tt.getRolesErr)
			controller.On("GetUsers", tt.params.ID).Return(map[string]apikey.UserView{tt.params.ID: {}}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestSortRolesByName(t *testing.T) {
	tests := []struct {
		name     string
		input    []*models.Role
		expected []*models.Role
	}{
		{
			name: "already sorted",
			input: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
		},
		{
			name: "unsorted",
			input: []*models.Role{
				{Name: String("user")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
		},
		{
			name: "same name",
			input: []*models.Role{
				{Name: String("admin")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortByName(tt.input)
			assert.Equal(t, tt.expected, tt.input)
		})
	}
}

// TestGetRolesForUser_OIDCSubject pins the read-side subject encoding: a global
// caller reading a bare OIDC id looks itself up under ":carol", while a
// namespaced caller's bare id is qualified to "customer1:carol".
func TestGetRolesForUser_OIDCSubject(t *testing.T) {
	falseP := false
	roles := map[string][]authorization.Policy{
		"role1": {{Resource: authorization.Collections("X")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}},
	}

	tests := []struct {
		name             string
		userID           string
		principalNS      string
		isGlobalOperator bool
		authzKey         string // users/<id> resource (never namespace-prefixed)
		groupKey         string // GetRolesForUserOrGroup subject user-portion
	}{
		{
			name:             "global operator bare oidc id → namespace-prefixed subject",
			userID:           "carol",
			isGlobalOperator: true,
			authzKey:         "carol",
			groupKey:         ":carol",
		},
		{
			name:        "namespaced caller bare oidc id → qualified subject",
			userID:      "carol",
			principalNS: "customer1",
			authzKey:    "customer1:carol",
			groupKey:    "customer1:carol",
		},
		{
			// A global operator addressing a namespaced OIDC user by its
			// qualified id keeps that user's subject (oidc:customer1:carol),
			// not a global one — global-ness is a property of the target, not
			// the caller.
			name:             "global operator namespaced oidc target → qualified subject, no empty namespace",
			userID:           "customer1:carol",
			isGlobalOperator: true,
			authzKey:         "customer1:carol",
			groupKey:         "customer1:carol",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				Username:         "op",
				IsGlobalOperator: tt.isGlobalOperator,
				Namespace:        tt.principalNS,
				UserType:         models.UserTypeInputOidc,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(nil)
			authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(nil).Maybe()
			authorizer.On("AuthorizeSilent", mock.Anything, principal, mock.Anything, mock.Anything).Return(nil).Maybe()
			controller.On("GetRolesForUserOrGroup", tt.groupKey, authentication.AuthTypeOIDC, false).Return(roles, nil)

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				logger:            logger,
				namespacesEnabled: true,
				oidcConfigs:       config.OIDC{Enabled: true},
			}
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				ID:               tt.userID,
				UserType:         string(models.UserTypeInputOidc),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			}, principal)
			require.IsType(t, &authz.GetRolesForUserOK{}, res)
			controller.AssertCalled(t, "GetRolesForUserOrGroup", tt.groupKey, authentication.AuthTypeOIDC, false)
		})
	}
}

// TestGetRolesForUser_Namespaces — resolved key drives validate, authz,
// and storage; global op qualified passthrough unchanged.
func TestGetRolesForUser_Namespaces(t *testing.T) {
	falseP := false
	userType := models.UserTypeInputDb
	roles := map[string][]authorization.Policy{
		"role1": {{Resource: authorization.Collections("X")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}},
	}

	tests := []struct {
		name             string
		userID           string
		principalNS      string
		isGlobalOperator bool
		authzKey         string // resolved users/<id> key authz is asked for
		wantStatus       any
	}{
		{
			name:        "namespaced caller short name with grant succeeds",
			userID:      "bob",
			principalNS: "customer1",
			authzKey:    "customer1:bob",
			wantStatus:  &authz.GetRolesForUserOK{},
		},
		{
			name:        "namespaced caller short name no grant returns 403",
			userID:      "bob",
			principalNS: "customer1",
			authzKey:    "customer1:bob",
			wantStatus:  &authz.GetRolesForUserForbidden{},
		},
		{
			name:        "namespaced caller non-existent own-ns user returns 404",
			userID:      "ghost",
			principalNS: "customer1",
			authzKey:    "customer1:ghost",
			wantStatus:  &authz.GetRolesForUserNotFound{},
		},
		{
			name:             "global operator qualified passthrough",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &authz.GetRolesForUserOK{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				IsGlobalOperator: tt.isGlobalOperator,
				Namespace:        tt.principalNS,
				UserType:         userType,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			switch tt.wantStatus.(type) {
			case *authz.GetRolesForUserForbidden:
				authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(fmt.Errorf("not allowed"))
			case *authz.GetRolesForUserOK:
				authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(nil)
				// Name-only visibility gate: a namespaced caller only sees a
				// role whose permissions it holds. The operator skips the gate.
				authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(fmt.Errorf("no all")).Maybe()
				authorizer.On("AuthorizeSilent", mock.Anything, principal, mock.Anything, mock.Anything).Return(nil).Maybe()
				controller.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{tt.authzKey: {}}, nil)
				controller.On("GetRolesForUserOrGroup", tt.authzKey, authentication.AuthTypeDb, false).Return(roles, nil)
			case *authz.GetRolesForUserNotFound:
				authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(nil)
				controller.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{}, nil)
			}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				logger:            logger,
				namespacesEnabled: true,
			}
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				ID:               tt.userID,
				UserType:         string(userType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestGetRolesForUser_NameOnlyHidesUnheldGlobalRole pins that in the name-only
// path a namespaced caller must not see a global role whose permissions it does
// not hold — the role's existence would otherwise leak by name.
func TestGetRolesForUser_NameOnlyHidesUnheldGlobalRole(t *testing.T) {
	falseP := false
	userType := models.UserTypeInputDb
	principal := &models.Principal{Namespace: "customer1", UserType: userType}
	const authzKey = "customer1:bob"
	roles := map[string][]authorization.Policy{
		"globalrole": {{Resource: authorization.Collections("X")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}},
	}

	tests := []struct {
		name      string
		holdsPerm bool
		wantRoles int
	}{
		{name: "caller lacks the role's permission: hidden", holdsPerm: false, wantRoles: 0},
		{name: "caller holds the role's permission: visible", holdsPerm: true, wantRoles: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(authzKey)[0]).Return(nil)
			controller.On("GetUsers", authzKey).Return(map[string]apikey.UserView{authzKey: {}}, nil)
			controller.On("GetRolesForUserOrGroup", authzKey, authentication.AuthTypeDb, false).Return(roles, nil)
			// Visibility gate: caller has no role-read-all, so it falls to the
			// per-policy check that decides whether the role is held.
			authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(fmt.Errorf("no all"))
			var silentErr error
			if !tt.holdsPerm {
				silentErr = fmt.Errorf("not held")
			}
			authorizer.On("AuthorizeSilent", mock.Anything, principal, mock.Anything, mock.Anything).Return(silentErr)

			h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, namespacesEnabled: true}
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				ID:               "bob",
				UserType:         string(userType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			}, principal)

			ok, isOK := res.(*authz.GetRolesForUserOK)
			require.True(t, isOK)
			assert.Len(t, ok.Payload, tt.wantRoles)
		})
	}
}

// TestGetRolesForUser_OwnUserSelfReadBypass — the bypass fires when the
// resolved key equals principal.Username; otherwise authz gates the read.
func TestGetRolesForUser_OwnUserSelfReadBypass(t *testing.T) {
	falseP := false
	userType := models.UserTypeInputDb
	principal := &models.Principal{
		Username:  "customer1:alice",
		Namespace: "customer1",
		UserType:  userType,
	}

	tests := []struct {
		name       string
		userID     string // short name the caller sends
		authzKey   string // qualified key the handler resolves to
		wantStatus any
	}{
		{
			// OK with no Authorize mock pins the bypass: an authz call here
			// would be an unexpected mock invocation and fail the test.
			name:       "own roles via short name with no grant",
			userID:     "alice",
			authzKey:   "customer1:alice",
			wantStatus: &authz.GetRolesForUserOK{},
		},
		{
			name:       "foreign own-ns user via short name with no grant",
			userID:     "bob",
			authzKey:   "customer1:bob",
			wantStatus: &authz.GetRolesForUserForbidden{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			switch tt.wantStatus.(type) {
			case *authz.GetRolesForUserOK:
				controller.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{tt.authzKey: {}}, nil)
				controller.On("GetRolesForUserOrGroup", tt.authzKey, authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{}, nil)
			case *authz.GetRolesForUserForbidden:
				authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(fmt.Errorf("not allowed"))
			}

			h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, namespacesEnabled: true}
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				ID:               tt.userID,
				UserType:         string(userType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestGetRolesForUser_GlobalOIDCSubjectSelfBypass pins that the self-read bypass is
// subject-aware. A global OIDC caller whose username looks namespaced
// ("customer1:carol") must not self-match the namespaced OIDC target of the same
// id: that target resolves to a different subject, so the read must be
// authorized. A global OIDC caller reading its own namespace-less id still
// bypasses.
func TestGetRolesForUser_GlobalOIDCSubjectSelfBypass(t *testing.T) {
	falseP := false
	roles := map[string][]authorization.Policy{
		"role1": {{Resource: authorization.Collections("X")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}},
	}

	tests := []struct {
		name       string
		username   string
		userID     string
		denyAuthz  bool   // wire Authorize(Users(userID)) to deny; absence pins the bypass
		groupKey   string // GetRolesForUserOrGroup subject on the bypass path
		wantStatus any
	}{
		{
			name:       "global oidc colon-name caller cannot self-bypass namespaced target",
			username:   "customer1:carol",
			userID:     "customer1:carol",
			denyAuthz:  true,
			wantStatus: &authz.GetRolesForUserForbidden{},
		},
		{
			name:       "global oidc caller reading its own id still bypasses",
			username:   "carol",
			userID:     "carol",
			groupKey:   ":carol",
			wantStatus: &authz.GetRolesForUserOK{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				Username:         tt.username,
				IsGlobalOperator: true,
				UserType:         models.UserTypeInputOidc,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if tt.denyAuthz {
				authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.userID)[0]).Return(fmt.Errorf("not allowed"))
			} else {
				authorizer.On("AuthorizeSilent", mock.Anything, principal, mock.Anything, mock.Anything).Return(nil).Maybe()
				controller.On("GetRolesForUserOrGroup", tt.groupKey, authentication.AuthTypeOIDC, false).Return(roles, nil)
			}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				logger:            logger,
				namespacesEnabled: true,
				oidcConfigs:       config.OIDC{Enabled: true},
			}
			res := h.getRolesForUser(authz.GetRolesForUserParams{
				ID:               tt.userID,
				UserType:         string(models.UserTypeInputOidc),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestGetRolesForUser_GlobalOIDCSelfFlagNoForeignDisclosure pins the disclosure
// the empty namespace prefix closes: a global OIDC caller named "customer1:carol"
// legitimately passes the per-user read gate, but reading the namespaced subject
// of the same id must be a FOREIGN read (own=false) so a role whose permissions
// the caller does not hold is filtered out — not returned at self visibility.
func TestGetRolesForUser_GlobalOIDCSelfFlagNoForeignDisclosure(t *testing.T) {
	falseP := false
	principal := &models.Principal{
		Username:         "customer1:carol",
		IsGlobalOperator: true,
		UserType:         models.UserTypeInputOidc,
	}
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	// The caller holds read on the users domain, so the per-user gate passes; any
	// leak would come solely from the wrong self-flag. The name-only visibility
	// gate denies (no role-read-all, does not hold the target role's perms), so a
	// foreign read must hide the role.
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("customer1:carol")[0]).Return(nil).Maybe()
	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(fmt.Errorf("no all")).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, principal, mock.Anything, mock.Anything).Return(fmt.Errorf("not held")).Maybe()
	controller.On("GetRolesForUserOrGroup", "customer1:carol", authentication.AuthTypeOIDC, false).Return(map[string][]authorization.Policy{
		authorization.Admin: {{Resource: authorization.Collections("X")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}},
	}, nil)

	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		namespacesEnabled: true,
		oidcConfigs:       config.OIDC{Enabled: true},
	}
	res := h.getRolesForUser(authz.GetRolesForUserParams{
		ID:               "customer1:carol",
		UserType:         string(models.UserTypeInputOidc),
		IncludeFullRoles: &falseP,
		HTTPRequest:      req,
	}, principal)
	ok, isOK := res.(*authz.GetRolesForUserOK)
	require.True(t, isOK, "got %T", res)
	assert.Empty(t, ok.Payload, "namespaced target's non-visible role must not leak via a self-read")
}

// TestGetRolesForUserDeprecated_DisabledOnNamespacesEnabled — the deprecated
// path is gated off at the top of the handler on namespace-enabled clusters;
// callers get a 410 before any authz / lookup runs. Pre-NS-disabled clusters
// keep the existing behavior (covered by other tests in this file).
func TestGetRolesForUserDeprecated_DisabledOnNamespacesEnabled(t *testing.T) {
	principal := &models.Principal{
		Username:  "customer1:alice",
		Namespace: "customer1",
		UserType:  models.UserTypeInputDb,
	}
	// No authorizer / controller calls — the gate fires first; an unexpected
	// invocation here would fail the test.
	h := &authZHandlers{
		authorizer:        authorization.NewMockAuthorizer(t),
		controller:        NewMockControllerAndGetUsers(t),
		namespacesEnabled: true,
	}
	res := h.getRolesForUserDeprecated(authz.GetRolesForUserDeprecatedParams{
		ID:          "bob",
		HTTPRequest: req,
	}, principal)
	_, ok := res.(*authz.GetRolesForUserDeprecatedGone)
	assert.True(t, ok, "expected 410 Gone, got %T", res)
}
