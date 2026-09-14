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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// stubRolesForSubjects answers exactly one GetRolesForSubjects call with roles and
// records the subjects that call asked about.
func stubRolesForSubjects(dynUser *MockDbUserAndRolesGetter, roles map[string]map[string][]authorization.Policy) *[]authorization.Subject {
	requested := &[]authorization.Subject{}
	dynUser.On("GetRolesForSubjects", mock.Anything).Run(func(args mock.Arguments) {
		*requested = args.Get(0).([]authorization.Subject)
	}).Return(roles, nil).Once()
	return requested
}

// TestSuccessListAll pins that listUsers reads every role in one
// GetRolesForSubjects call about exactly the users it lists, and renders each
// user with its own roles.
func TestSuccessListAll(t *testing.T) {
	tests := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		dbUsers           []string
		staticUsers       []string
		roles             map[string]map[string][]authorization.Policy
		wantSubjects      []authorization.Subject
		wantRoles         map[string][]string
	}{
		{
			name:         "only db user",
			principal:    &models.Principal{Username: "not-root"},
			dbUsers:      []string{"user1"},
			staticUsers:  []string{"static"},
			roles:        map[string]map[string][]authorization.Policy{"db:user1": {"role": {}}},
			wantSubjects: []authorization.Subject{dbUserSubject("user1")},
			wantRoles:    map[string][]string{"user1": {"role"}},
		},
		{
			name:        "db + static user",
			principal:   &models.Principal{Username: "root"},
			dbUsers:     []string{"user1"},
			staticUsers: []string{"static"},
			roles: map[string]map[string][]authorization.Policy{
				"db:user1":  {"role": {}},
				"db:static": {"static-role": {}},
			},
			wantSubjects: []authorization.Subject{dbUserSubject("user1"), dbUserSubject("static")},
			wantRoles:    map[string][]string{"user1": {"role"}, "static": {"static-role"}},
		},
		{
			name:      "many db users, one role-less and one absent from the answer",
			principal: &models.Principal{Username: "not-root"},
			dbUsers:   []string{"user1", "user2", "role-less", "absent"},
			roles: map[string]map[string][]authorization.Policy{
				"db:user1":     {"role1": {}},
				"db:user2":     {"role2": {}, "role3": {}},
				"db:role-less": {},
			},
			wantSubjects: []authorization.Subject{dbUserSubject("user1"), dbUserSubject("user2"), dbUserSubject("role-less"), dbUserSubject("absent")},
			wantRoles:    map[string][]string{"user1": {"role1"}, "user2": {"role2", "role3"}, "role-less": {}, "absent": {}},
		},
		{
			name:         "root with static users only",
			principal:    &models.Principal{Username: "root"},
			staticUsers:  []string{"static"},
			roles:        map[string]map[string][]authorization.Policy{"db:static": {"role": {}}},
			wantSubjects: []authorization.Subject{dbUserSubject("static")},
			wantRoles:    map[string][]string{"static": {"role"}},
		},
		{
			name:         "root with nothing to list",
			principal:    &models.Principal{Username: "root"},
			roles:        map[string]map[string][]authorization.Policy{},
			wantSubjects: []authorization.Subject{},
			wantRoles:    map[string][]string{},
		},
		{
			name:              "root with nothing to list on a namespaced cluster",
			principal:         &models.Principal{Username: "root", UserType: models.UserTypeInputOidc, Groups: []string{"admins"}},
			namespacesEnabled: true,
			roles:             map[string]map[string][]authorization.Policy{},
			wantSubjects:      []authorization.Subject{},
			wantRoles:         map[string][]string{},
		},
		{
			name:              "nil principal on a namespaced cluster",
			namespacesEnabled: true,
			dbUsers:           []string{"user1"},
			roles:             map[string]map[string][]authorization.Policy{"db:user1": {"role": {}}},
			wantSubjects:      []authorization.Subject{dbUserSubject("user1")},
			wantRoles:         map[string][]string{"user1": {"role"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, tt.principal, mock.Anything, mock.Anything).Return(nil).Maybe()
			dynUser := NewMockDbUserAndRolesGetter(t)
			stored := make(map[string]apikey.UserView, len(tt.dbUsers))
			for _, id := range tt.dbUsers {
				stored[id] = apikey.UserView{Id: id}
			}
			dynUser.On("GetUsers").Return(stored, nil)
			requested := stubRolesForSubjects(dynUser, tt.roles)

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: tt.staticUsers},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
				dbUserEnabled:        true,
				namespacesEnabled:    tt.namespacesEnabled,
			}

			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, tt.principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			require.True(t, ok, "got %T", res)
			require.ElementsMatch(t, tt.wantSubjects, *requested)

			requestedIDs := make([]string, 0, len(*requested))
			for _, s := range *requested {
				requestedIDs = append(requestedIDs, s.ID)
			}
			payloadIDs := make([]string, 0, len(parsed.Payload))
			for _, user := range parsed.Payload {
				payloadIDs = append(payloadIDs, *user.UserID)
			}
			require.ElementsMatch(t, requestedIDs, payloadIDs)

			require.Len(t, parsed.Payload, len(tt.wantRoles))
			for _, user := range parsed.Payload {
				want, ok := tt.wantRoles[*user.UserID]
				require.True(t, ok, "unexpected user %q", *user.UserID)
				require.ElementsMatch(t, want, user.Roles)
				if len(want) == 0 {
					require.NotNil(t, user.Roles)
					raw, err := json.Marshal(user)
					require.NoError(t, err)
					require.Contains(t, string(raw), `"roles":[]`)
				}
			}
		})
	}
}

// TestListUsersAPIKeyFirstLettersVisibility pins who sees the api-key hint on the
// list endpoint: root always, a built-in admin only on namespace-enabled
// clusters, everyone else never. The caller's roles arrive in the same
// GetRolesForSubjects call as the listed user's.
func TestListUsersAPIKeyFirstLettersVisibility(t *testing.T) {
	dbUser := "user1"
	tests := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		roles             map[string]map[string][]authorization.Policy
		wantSubjects      []authorization.Subject
		wantFirstLetters  string
	}{
		{
			name:              "root sees on namespaced cluster",
			principal:         &models.Principal{Username: "root", UserType: models.UserTypeInputDb},
			namespacesEnabled: true,
			wantSubjects:      []authorization.Subject{dbUserSubject(dbUser)},
			wantFirstLetters:  "abc",
		},
		{
			name:              "admin sees on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb},
			namespacesEnabled: true,
			roles:             map[string]map[string][]authorization.Policy{"db:not-root": {authorization.Admin: {}}},
			wantSubjects:      []authorization.Subject{dbUserSubject(dbUser), dbUserSubject("not-root")},
			wantFirstLetters:  "abc",
		},
		{
			name:              "admin listing itself sees on namespaced cluster",
			principal:         &models.Principal{Username: dbUser, UserType: models.UserTypeInputDb},
			namespacesEnabled: true,
			roles:             map[string]map[string][]authorization.Policy{"db:user1": {authorization.Admin: {}}},
			wantSubjects:      []authorization.Subject{dbUserSubject(dbUser), dbUserSubject(dbUser)},
			wantFirstLetters:  "abc",
		},
		{
			name:              "admin via group sees on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb, Groups: []string{"admin-group"}},
			namespacesEnabled: true,
			roles: map[string]map[string][]authorization.Policy{
				"db:not-root":       {},
				"group:admin-group": {authorization.Admin: {}},
			},
			wantSubjects: []authorization.Subject{
				dbUserSubject(dbUser), dbUserSubject("not-root"),
				{ID: "admin-group", AuthType: authentication.AuthTypeDb, IsGroup: true},
			},
			wantFirstLetters: "abc",
		},
		{
			name:              "oidc caller with admin via its second group sees on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputOidc, Groups: []string{"viewers", "admin-group"}},
			namespacesEnabled: true,
			roles: map[string]map[string][]authorization.Policy{
				"oidc:not-root":     {},
				"group:viewers":     {authorization.Viewer: {}},
				"group:admin-group": {authorization.Admin: {}},
			},
			wantSubjects: []authorization.Subject{
				dbUserSubject(dbUser),
				{ID: "not-root", AuthType: authentication.AuthTypeOIDC},
				{ID: "viewers", AuthType: authentication.AuthTypeOIDC, IsGroup: true},
				{ID: "admin-group", AuthType: authentication.AuthTypeOIDC, IsGroup: true},
			},
			wantFirstLetters: "abc",
		},
		{
			name:              "non-admin hidden on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb, Groups: []string{"viewers"}},
			namespacesEnabled: true,
			roles: map[string]map[string][]authorization.Policy{
				"db:not-root":   {},
				"group:viewers": {authorization.Viewer: {}},
			},
			wantSubjects: []authorization.Subject{
				dbUserSubject(dbUser), dbUserSubject("not-root"),
				{ID: "viewers", AuthType: authentication.AuthTypeDb, IsGroup: true},
			},
			wantFirstLetters: "",
		},
		{
			name:              "admin hidden on non-namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb, Groups: []string{"admin-group"}},
			namespacesEnabled: false,
			roles: map[string]map[string][]authorization.Policy{
				"db:not-root":       {authorization.Admin: {}},
				"group:admin-group": {authorization.Admin: {}},
			},
			wantSubjects:     []authorization.Subject{dbUserSubject(dbUser)},
			wantFirstLetters: "",
		},
		{
			name:              "non-admin hidden while another subject holds admin on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb},
			namespacesEnabled: true,
			roles: map[string]map[string][]authorization.Policy{
				"db:not-root":       {},
				"group:admin-group": {authorization.Admin: {}},
			},
			wantSubjects:     []authorization.Subject{dbUserSubject(dbUser), dbUserSubject("not-root")},
			wantFirstLetters: "",
		},
		{
			name:              "oidc caller named like a db admin hidden on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputOidc},
			namespacesEnabled: true,
			roles: map[string]map[string][]authorization.Policy{
				"db:not-root":   {authorization.Admin: {}},
				"oidc:not-root": {},
			},
			wantSubjects:     []authorization.Subject{dbUserSubject(dbUser), {ID: "not-root", AuthType: authentication.AuthTypeOIDC}},
			wantFirstLetters: "",
		},
		{
			name:              "oidc admin sees on namespaced cluster",
			principal:         &models.Principal{Username: "not-root", UserType: models.UserTypeInputOidc},
			namespacesEnabled: true,
			roles:             map[string]map[string][]authorization.Policy{"oidc:not-root": {authorization.Admin: {}}},
			wantSubjects:      []authorization.Subject{dbUserSubject(dbUser), {ID: "not-root", AuthType: authentication.AuthTypeOIDC}},
			wantFirstLetters:  "abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users()[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers").Return(map[string]apikey.UserView{dbUser: {Id: dbUser, ApiKeyFirstLetters: "abc"}}, nil)
			// role-less target so role visibility never authorizes
			roles := map[string]map[string][]authorization.Policy{"db:" + dbUser: {}}
			for key, callerRoles := range tt.roles {
				roles[key] = callerRoles
			}
			requested := stubRolesForSubjects(dynUser, roles)

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				rbacConfig:        rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
				dbUserEnabled:     true,
				namespacesEnabled: tt.namespacesEnabled,
			}

			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, tt.principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			require.True(t, ok, "got %T", res)
			require.Len(t, parsed.Payload, 1)
			require.Equal(t, tt.wantFirstLetters, parsed.Payload[0].APIKeyFirstLetters)
			require.ElementsMatch(t, tt.wantSubjects, *requested)
		})
	}
}

func TestSuccessListAllAfterImport(t *testing.T) {
	exStaticUser := "static"
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, &models.Principal{Username: "root"}, authorization.READ, authorization.Users()[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(map[string]apikey.UserView{exStaticUser: {Id: exStaticUser, Active: true}}, nil)
	requested := stubRolesForSubjects(dynUser, map[string]map[string][]authorization.Policy{"db:static": {"role": {}}})

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{exStaticUser}, AllowedKeys: []string{"static"}},
		rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		dbUserEnabled:        true,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, &models.Principal{Username: "root"})
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 1)
	user := parsed.Payload[0]
	require.Equal(t, *user.UserID, exStaticUser)
	require.Equal(t, *user.Active, true)
	require.Equal(t, *user.DbUserType, string(models.UserTypeOutputDbUser))
	require.Equal(t, []string{"role"}, user.Roles)
	require.Equal(t, []authorization.Subject{dbUserSubject(exStaticUser)}, *requested)
}

func TestSuccessListAllUserMultiNode(t *testing.T) {
	baseTime := time.Now()

	usersIds := []string{"user1", "user2", "user3", "user4", "user5", "user6"}

	trueptr := true
	tests := []struct {
		name          string
		nodeResponses []map[string]time.Time
		expectedTime  map[string]time.Time
		userIds       []string
	}{
		{name: "single node, single user", nodeResponses: []map[string]time.Time{{}}, expectedTime: map[string]time.Time{usersIds[0]: baseTime}, userIds: usersIds[:1]},
		{name: "single node, multi user", nodeResponses: []map[string]time.Time{{}}, expectedTime: map[string]time.Time{usersIds[0]: baseTime, usersIds[1]: baseTime}, userIds: usersIds[:2]},
		{
			name:          "multi node, latest time local node, single user",
			userIds:       usersIds[:1],
			expectedTime:  map[string]time.Time{usersIds[0]: baseTime},
			nodeResponses: []map[string]time.Time{{usersIds[0]: baseTime.Add(-time.Second)}, {usersIds[0]: baseTime.Add(-time.Second)}},
		},
		{
			name:         "multi node, latest time local node, multi user",
			userIds:      usersIds[:2],
			expectedTime: map[string]time.Time{usersIds[0]: baseTime, usersIds[1]: baseTime},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(-time.Second), usersIds[1]: baseTime.Add(-2 * time.Second)},
				{usersIds[0]: baseTime.Add(-time.Second), usersIds[1]: baseTime.Add(-2 * time.Second)},
			},
		},
		{
			name:          "multi node, latest time other node, single user",
			userIds:       usersIds[:1],
			expectedTime:  map[string]time.Time{usersIds[0]: baseTime.Add(time.Hour)},
			nodeResponses: []map[string]time.Time{{usersIds[0]: baseTime.Add(time.Hour)}, {usersIds[0]: baseTime.Add(time.Minute)}},
		},
		{
			name:         "multi node, latest time other node, multi user",
			userIds:      usersIds[:2],
			expectedTime: map[string]time.Time{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(2 * time.Hour)},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[0]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(2 * time.Hour)},
			},
		},
		{
			name:    "six node, six user",
			userIds: usersIds,
			expectedTime: map[string]time.Time{
				usersIds[0]: baseTime.Add(time.Hour),
				usersIds[1]: baseTime.Add(2 * time.Hour),
				usersIds[2]: baseTime.Add(3 * time.Hour),
				usersIds[3]: baseTime.Add(4 * time.Hour),
				usersIds[4]: baseTime.Add(5 * time.Hour),
				usersIds[5]: baseTime.Add(6 * time.Hour),
			},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[0]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(2 * time.Hour)},
				{usersIds[2]: baseTime.Add(3 * time.Hour), usersIds[3]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[2]: baseTime.Add(-time.Minute), usersIds[3]: baseTime.Add(4 * time.Hour)},
				{usersIds[4]: baseTime.Add(5 * time.Hour), usersIds[5]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[4]: baseTime.Add(-time.Minute), usersIds[5]: baseTime.Add(6 * time.Hour)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{Username: "non-root"}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users()[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			schemaGetter := schema.NewMockSchemaGetter(t)

			usersRet := make(map[string]apikey.UserView)
			for _, user := range tt.userIds {
				usersRet[user] = apikey.UserView{Id: user, LastUsedAt: baseTime}
			}

			dynUser.On("GetUsers").Return(usersRet, nil)
			roles := make(map[string]map[string][]authorization.Policy, len(tt.userIds))
			for _, user := range tt.userIds {
				roles["db:"+user] = map[string][]authorization.Policy{"role": {}}
			}
			stubRolesForSubjects(dynUser, roles)

			var nodes []string
			for i := range tt.nodeResponses {
				nodes = append(nodes, string(rune(i)))
			}
			schemaGetter.On("Nodes").Return(nodes)

			server := httptest.NewServer(&fakeHandler{t: t, counter: atomic.Int32{}, nodeResponses: tt.nodeResponses})
			defer server.Close()

			remote := clients.NewRemoteUser(&http.Client{}, FakeNodeResolver{path: server.URL})

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}}, dbUserEnabled: true,
				nodesGetter: schemaGetter,
				remoteUser:  remote,
			}

			res := h.listUsers(users.ListAllUsersParams{IncludeLastUsedTime: &trueptr, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			for i := range tt.userIds {
				uid := *parsed.Payload[i].UserID
				require.Equal(t, parsed.Payload[i].LastUsedAt.String(), strfmt.DateTime(tt.expectedTime[uid]).String())
			}
		})
	}
}

// TestSuccessListForbidden pins that a non-root caller who may read no db user
// gets an empty listing without a role lookup, which a failing leader would
// otherwise turn into a 500.
func TestSuccessListForbidden(t *testing.T) {
	tests := []struct {
		name              string
		rbacConfig        rbacconf.Config
		namespacesEnabled bool
	}{
		{name: "rbac disabled"},
		{name: "rbac on a namespaced cluster", rbacConfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, mock.Anything).Return(errors.New("some error"))
			authorizer.On("FilterAuthorizedResources", mock.Anything, principal, authorization.READ, mock.Anything).Return([]string{}, nil).Maybe()
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers").Return(map[string]apikey.UserView{"test": {Id: "test"}}, nil)

			log, _ := test.NewNullLogger()
			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				logger:            log,
				dbUserEnabled:     true,
				rbacConfig:        tt.rbacConfig,
				namespacesEnabled: tt.namespacesEnabled,
			}

			// no authorization for anything => response will be empty
			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			require.True(t, ok, "got %T", res)
			require.NotNil(t, parsed.Payload)
			require.Empty(t, parsed.Payload)
			dynUser.AssertNotCalled(t, "GetRolesForSubjects", mock.Anything)
		})
	}
}

// TestListUsersReadError pins that a failed GetUsers or GetRolesForSubjects fails
// the whole listing instead of rendering an empty or role-less one.
func TestListUsersReadError(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		usersErr          error
		wantMessage       string
	}{
		{name: "role lookup fails on a flat cluster", wantMessage: "leader unavailable"},
		{name: "role lookup fails on a namespaced cluster, caller's admin subjects in the same lookup", namespacesEnabled: true, wantMessage: "leader unavailable"},
		{name: "user read fails before any role lookup", usersErr: errors.New("users unavailable"), wantMessage: "users unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{Username: "not-root", UserType: models.UserTypeInputDb}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users()[0]).Return(nil).Maybe()
			dynUser := NewMockDbUserAndRolesGetter(t)
			if tt.usersErr != nil {
				dynUser.On("GetUsers").Return(nil, tt.usersErr)
			} else {
				dynUser.On("GetUsers").Return(map[string]apikey.UserView{"user1": {Id: "user1"}}, nil)
				dynUser.On("GetRolesForSubjects", mock.Anything).Return(nil, errors.New("leader unavailable")).Once()
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				rbacConfig:        rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
				dbUserEnabled:     true,
				namespacesEnabled: tt.namespacesEnabled,
			}

			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
			parsed, ok := res.(*users.ListAllUsersInternalServerError)
			require.True(t, ok, "got %T", res)
			require.Len(t, parsed.Payload.Error, 1)
			require.Contains(t, parsed.Payload.Error[0].Message, tt.wantMessage)
		})
	}
}

func TestListNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 0)
}

// TestListUsers_Namespaces — per-item response stripping: short id (no
// Namespace) for a namespaced caller; full id (with Namespace) for a global op.
func TestListUsers_Namespaces(t *testing.T) {
	storedUser := apikey.UserView{Id: "customer1:bob", Namespace: "customer1", Active: true}

	tests := []struct {
		name             string
		principalNS      string
		isGlobalOperator bool
		wantUserID       string
		wantNamespace    string
	}{
		{
			name:          "namespaced caller sees short id, no namespace field",
			principalNS:   "customer1",
			wantUserID:    "bob",
			wantNamespace: "",
		},
		{
			name:             "global operator sees qualified id and namespace field",
			isGlobalOperator: true,
			wantUserID:       "customer1:bob",
			wantNamespace:    "customer1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator, Namespace: tt.principalNS}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(storedUser.Id)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers").Return(map[string]apikey.UserView{storedUser.Id: storedUser}, nil)
			dynUser.On("GetRolesForSubjects", []authorization.Subject{dbUserSubject(storedUser.Id)}).Return(map[string]map[string][]authorization.Policy{}, nil).Once()

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
			}

			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			require.True(t, ok)
			require.Len(t, parsed.Payload, 1)
			require.Equal(t, tt.wantUserID, *parsed.Payload[0].UserID)
			require.Equal(t, tt.wantNamespace, parsed.Payload[0].Namespace)
		})
	}
}

// TestListUsers_CrossNamespaceIsolation pins that a namespaced caller sees only
// users in its own namespace: both customer1:bob and customer2:bob exist in
// storage, but the resource filter (driven by the matcher's users/<id>
// specialization, exercised here via FilterAuthorizedResources) returns only
// the caller's customer1:bob, stripped to the short name. The role lookup is
// asked about customer1:bob and the caller only.
func TestListUsers_CrossNamespaceIsolation(t *testing.T) {
	stored := map[string]apikey.UserView{
		"customer1:bob": {Id: "customer1:bob", Namespace: "customer1", Active: true},
		"customer2:bob": {Id: "customer2:bob", Namespace: "customer2", Active: true},
	}
	principal := &models.Principal{Namespace: "customer1", UserType: models.UserTypeInputDb}
	nullLogger, _ := test.NewNullLogger()

	authorizer := authorization.NewMockAuthorizer(t)
	// Wildcard-parent shortcut path fails (no users/* grant), forcing the
	// filter to fall through to per-item filtering.
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, "users/*").Return(errors.New("not allowed on wildcard"))
	// Per-item filter returns only the caller's own-ns resource — what the
	// matcher's users/<id> specialization produces in production.
	authorizer.On("FilterAuthorizedResources", mock.Anything, principal, authorization.READ, mock.Anything, mock.Anything).
		Return([]string{"users/customer1:bob"}, nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(stored, nil)
	requested := stubRolesForSubjects(dynUser, map[string]map[string][]authorization.Policy{})

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
		rbacConfig:        rbacconf.Config{Enabled: true},
		logger:            nullLogger,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	require.True(t, ok)
	require.Len(t, parsed.Payload, 1)
	require.Equal(t, "bob", *parsed.Payload[0].UserID, "expected short id, no namespace prefix")
	require.Empty(t, parsed.Payload[0].Namespace, "namespace field must be hidden for non-operator")
	require.ElementsMatch(t, []authorization.Subject{dbUserSubject("customer1:bob"), dbUserSubject("")}, *requested)
}
