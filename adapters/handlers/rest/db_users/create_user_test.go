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

package db_users

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

func TestCreateUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name   string
		userId string
	}{
		{name: "too long", userId: strings.Repeat("A", 129)},
		{name: "invalid characters", userId: "#a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.createUser(users.CreateUserParams{UserID: tt.userId, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserUnprocessableEntity)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name                                 string
		GetUserReturn                        error
		CheckUserIdentifierExistsErrorReturn error
		CheckUserIdentifierExistsValueReturn bool
		CreateUserReturn                     error
	}{
		{name: "get user error", GetUserReturn: errors.New("some error")},
		{name: "check identifier exists, error", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: errors.New("some error")},
		{name: "check identifier exists, repeated collision", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: nil, CheckUserIdentifierExistsValueReturn: true},
		{name: "create user error", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: nil, CreateUserReturn: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(nil, tt.GetUserReturn)
			if tt.GetUserReturn == nil {
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(tt.CheckUserIdentifierExistsValueReturn, tt.CheckUserIdentifierExistsErrorReturn)
			}
			if tt.CheckUserIdentifierExistsErrorReturn == nil && !tt.CheckUserIdentifierExistsValueReturn && tt.GetUserReturn == nil {
				dynUser.On("CreateUser", mock.Anything, "user", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tt.CreateUserReturn)
			}

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateConflict(t *testing.T) {
	tests := []struct {
		name     string
		rbacConf config.StaticAPIKey
	}{
		{name: "no rbac conf", rbacConf: config.StaticAPIKey{}},
		{name: "enabled rbac conf", rbacConf: config.StaticAPIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{}

			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)
			if !tt.rbacConf.Enabled {
				dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{"user": {}}, nil)
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: tt.rbacConf,
				dbUserEnabled:        true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserConflict)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", user).Return(map[string]apikey.UserView{}, nil)
	dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
	dynUser.On("CreateUser", mock.Anything, user, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestCreateSuccessWithKey(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("CreateUserWithKey", mock.Anything, user, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		dbUserEnabled:        true,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{user}, AllowedKeys: []string{"key"}},
	}
	tp := true

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req, Body: users.CreateUserBody{Import: &tp}}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	assert.Equal(t, *parsed.Payload.Apikey, "key")
}

func TestCreateNotFoundWithKey(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		dbUserEnabled:        true,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{user + "false"}, AllowedKeys: []string{"key"}},
	}
	tp := true

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req, Body: users.CreateUserBody{Import: &tp}}, principal)
	parsed, ok := res.(*users.CreateUserNotFound)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestCreateForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserForbidden)
	assert.True(t, ok)
}

func TestCreateUnprocessableEntityCreatingRootUser(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user-root")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer,
		rbacConfig: rbacconf.Config{RootUsers: []string{"user-root"}}, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user-root", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestCreateUnprocessableEntityCreatingAdminlistUser(t *testing.T) {
	tests := []struct {
		name          string
		adminlistConf adminlist.Config
	}{
		{name: "adminlist - read-only user", adminlistConf: adminlist.Config{Enabled: true, ReadOnlyUsers: []string{"user"}}},
		{name: "adminlist - admin user", adminlistConf: adminlist.Config{Enabled: true, Users: []string{"user"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{}

			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

			h := dynUserHandler{
				dbUsers:         dynUser,
				authorizer:      authorizer,
				adminListConfig: tt.adminlistConf,
				dbUserEnabled:   true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserUnprocessableEntity)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestCreateUser_Namespaces(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		known             []string                      // namespaces the Exister reports as existing + active
		inState           map[string]api.NamespaceState // namespaces that exist in a non-active state
		userID            string                        // raw id as the client sends it
		principalNS       string                        // principal.Namespace ("" = global)
		isGlobalOperator  bool
		importUser        bool
		authzKey          string // resolved users/ key the authorizer is asked for; "" if resolution 422s before authz
		wantStatus        any
		wantMsgContains   string // substring the error payload must name; "" = not asserted
	}{
		{
			name:              "ns-disabled bare name succeeds",
			namespacesEnabled: false,
			userID:            "user",
			authzKey:          "user",
			wantStatus:        &users.CreateUserCreated{},
		},
		{
			// Pins resolve-then-authorize: authz is mocked on the qualified key
			// only — a call on the raw "bob" would be an unexpected mock call.
			name:              "namespaced principal short name succeeds",
			namespacesEnabled: true,
			known:             []string{"customer1"},
			userID:            "bob",
			principalNS:       "customer1",
			authzKey:          "customer1:bob",
			wantStatus:        &users.CreateUserCreated{},
		},
		{
			name:              "namespaced principal rejects ':' in name",
			namespacesEnabled: true,
			userID:            "a:b",
			principalNS:       "customer1",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "global operator qualified name succeeds",
			namespacesEnabled: true,
			known:             []string{"customer1"},
			userID:            "customer1:bob",
			isGlobalOperator:  true,
			authzKey:          "customer1:bob",
			wantStatus:        &users.CreateUserCreated{},
		},
		{
			name:              "global operator bare name rejected",
			namespacesEnabled: true,
			userID:            "bob",
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "malformed empty namespace rejected",
			namespacesEnabled: true,
			userID:            ":bob",
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "malformed empty user rejected",
			namespacesEnabled: true,
			userID:            "bob:",
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "malformed multi-colon rejected",
			namespacesEnabled: true,
			userID:            "a:b:c",
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "unknown namespace rejected",
			namespacesEnabled: true,
			userID:            "ns404:bob",
			isGlobalOperator:  true,
			authzKey:          "ns404:bob",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			// No CreateUser mock: the pre-check must reject before apply.
			name:              "namespace being deleted short-circuits before apply",
			namespacesEnabled: true,
			inState:           map[string]api.NamespaceState{"ns1": api.NamespaceStateDeleting},
			userID:            "ns1:user",
			isGlobalOperator:  true,
			authzKey:          "ns1:user",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
			wantMsgContains:   "being deleted",
		},
		{
			// No CreateUser mock: the pre-check must reject before apply. The
			// message must name suspension: reporting a deletion would tell the
			// caller the namespace is never coming back.
			name:              "suspended namespace short-circuits before apply",
			namespacesEnabled: true,
			inState:           map[string]api.NamespaceState{"ns1": api.NamespaceStateSuspended},
			userID:            "ns1:user",
			isGlobalOperator:  true,
			authzKey:          "ns1:user",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
			wantMsgContains:   "namespace is suspended",
		},
		{
			// HTTPStatusForNamespaceErr doesn't cover ErrNamespaceGone, so
			// without the explicit guard this would be a 500.
			name:              "missing namespace renders 422, not 500",
			namespacesEnabled: true,
			userID:            "ns1:user",
			isGlobalOperator:  true,
			authzKey:          "ns1:user",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
			wantMsgContains:   "namespace no longer exists",
		},
		{
			name:              "import on NS-enabled rejected",
			namespacesEnabled: true,
			userID:            "customer1:bob",
			isGlobalOperator:  true,
			importUser:        true,
			authzKey:          "customer1:bob",
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator, Namespace: tt.principalNS}
			authorizer := authorization.NewMockAuthorizer(t)
			if tt.authzKey != "" {
				authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(tt.authzKey)[0]).Return(nil)
			}

			dynUser := NewMockDbUserAndRolesGetter(t)
			if _, ok := tt.wantStatus.(*users.CreateUserCreated); ok {
				// Expected namespace = authzKey prefix (or "" if no ':').
				expectedNS := ""
				if i := strings.Index(tt.authzKey, ":"); i >= 0 {
					expectedNS = tt.authzKey[:i]
				}
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{}, nil)
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
				dynUser.On("CreateUser", mock.Anything, tt.authzKey, mock.Anything, mock.Anything, mock.Anything, expectedNS, mock.Anything).Return(nil)
			}

			ns := namespaces.NewMockExister(t)
			known := map[string]struct{}{}
			for _, n := range tt.known {
				known[n] = struct{}{}
			}
			states := map[string]api.NamespaceState{}
			for _, n := range tt.known {
				states[n] = api.NamespaceStateActive
			}
			for n, st := range tt.inState {
				states[n] = st
			}
			exists := func(name string) bool { _, ok := states[name]; return ok }
			ns.On("Exists", mock.AnythingOfType("string")).Return(exists).Maybe()
			ns.On("IsActive", mock.AnythingOfType("string")).Return(func(name string) bool {
				return states[name] == api.NamespaceStateActive
			}).Maybe()
			ns.On("GetNamespace", mock.AnythingOfType("string")).Return(
				func(name string) api.Namespace {
					return api.Namespace{Name: name, HomeNodes: []string{"node-1"}, State: states[name]}
				},
				exists,
			).Maybe()

			body := users.CreateUserBody{}
			if tt.importUser {
				tp := true
				body.Import = &tp
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: tt.namespacesEnabled,
				namespaces:        ns,
			}

			res := h.createUser(users.CreateUserParams{UserID: tt.userID, HTTPRequest: req, Body: body}, principal)
			assert.IsType(t, tt.wantStatus, res)
			if tt.wantMsgContains != "" {
				assert.Contains(t, responderErrMessage(t, res), tt.wantMsgContains)
			}
		})
	}
}

// responderErrMessage returns the first error message in res's payload. The
// status is asserted separately, so an unexpected responder type fails here
// rather than reporting a confusing empty message.
func responderErrMessage(t *testing.T, res middleware.Responder) string {
	t.Helper()
	var payload *models.ErrorResponse
	switch r := res.(type) {
	case *users.CreateUserUnprocessableEntity:
		payload = r.Payload
	case *users.CreateUserInternalServerError:
		payload = r.Payload
	default:
		t.Fatalf("responder %T carries no error payload", res)
	}
	require.NotNil(t, payload)
	require.NotEmpty(t, payload.Error)
	return payload.Error[0].Message
}

// TestCreateUser_MapsApplyNamespaceErrors asserts the createUser handler
// classifies apply-layer namespace sentinels: the lifecycle family renders
// 422 and everything else is 500. The handler's pre-flight Exists check can
// race with a concurrent namespace delete, so a 500 would be misleading: the
// request is well-formed, the namespace just changed state underneath it.
// Deleting matches the pre-check's own 422 so the race cannot flip the
// status a client sees.
func TestCreateUser_MapsApplyNamespaceErrors(t *testing.T) {
	const userID = "ns1:user"

	tests := []struct {
		name     string
		applyErr error
		expect   any
	}{
		{
			name:     "ErrNamespaceGone returns 422",
			applyErr: fmt.Errorf("apply: %w", namespaces.ErrNamespaceGone),
			expect:   &users.CreateUserUnprocessableEntity{},
		},
		{
			name:     "ErrNamespaceDeleting returns 422",
			applyErr: fmt.Errorf("apply: %w", namespaces.ErrNamespaceDeleting),
			expect:   &users.CreateUserUnprocessableEntity{},
		},
		{
			name:     "ErrNamespaceSuspended returns 422",
			applyErr: fmt.Errorf("apply: %w", namespaces.ErrNamespaceSuspended),
			expect:   &users.CreateUserUnprocessableEntity{},
		},
		{
			name:     "ErrNamespaceNotEmpty returns 422",
			applyErr: fmt.Errorf("apply: %w", namespaces.ErrNamespaceNotEmpty),
			expect:   &users.CreateUserUnprocessableEntity{},
		},
		{
			name:     "ErrInvalidState returns 422",
			applyErr: fmt.Errorf("apply: %w", namespaces.ErrInvalidState),
			expect:   &users.CreateUserUnprocessableEntity{},
		},
		{
			name:     "unrelated error returns 500",
			applyErr: errors.New("disk full"),
			expect:   &users.CreateUserInternalServerError{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: true}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(userID)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", userID).Return(map[string]apikey.UserView{}, nil)
			dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
			dynUser.On("CreateUser", mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything, "ns1", mock.Anything).Return(tt.applyErr)

			// Active, so the pre-check passes and the apply error under test
			// is what the handler renders.
			ns := namespaces.NewMockExister(t)
			ns.On("Exists", mock.AnythingOfType("string")).Return(true).Maybe()
			ns.On("IsActive", mock.AnythingOfType("string")).Return(true).Maybe()
			ns.On("GetNamespace", mock.AnythingOfType("string")).Return(
				func(name string) api.Namespace {
					return api.Namespace{Name: name, HomeNodes: []string{"node-1"}, State: api.NamespaceStateActive}
				},
				func(string) bool { return true },
			).Maybe()

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
				namespaces:        ns,
			}

			res := h.createUser(users.CreateUserParams{
				UserID:      userID,
				HTTPRequest: req,
			}, principal)
			assert.IsType(t, tt.expect, res)
		})
	}
}

func TestListAndGetUser_NamespaceVisibility(t *testing.T) {
	const userID = "u1"
	storedUser := apikey.UserView{Id: userID, Namespace: "ns1", Active: true, ApiKeyFirstLetters: "abc"}

	tests := []struct {
		name             string
		isGlobalOperator bool
		wantNamespace    string
	}{
		{name: "operator sees namespace", isGlobalOperator: true, wantNamespace: "ns1"},
		{name: "non-operator does not see namespace", isGlobalOperator: false, wantNamespace: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(userID)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", userID).Return(map[string]apikey.UserView{userID: storedUser}, nil)
			dynUser.On("GetRolesForUserOrGroup", userID, mock.Anything, false).Return(map[string][]authorization.Policy{}, nil)

			h := dynUserHandler{
				dbUsers:       dynUser,
				authorizer:    authorizer,
				dbUserEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: userID, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.GetUserInfoOK)
			assert.True(t, ok)
			assert.Equal(t, tt.wantNamespace, parsed.Payload.Namespace)
		})
	}
}
