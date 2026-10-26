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

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/wl/dbusers"
)

type setCall struct {
	userId    string
	expiresAt time.Time
}

type fakeRaft struct {
	users  map[string]apikey.UserView
	getErr error
	setErr error
	sets   []setCall
}

func (f *fakeRaft) GetUsers(userIds ...string) (map[string]apikey.UserView, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	found := map[string]apikey.UserView{}
	for _, id := range userIds {
		if u, ok := f.users[id]; ok {
			found[id] = u
		}
	}
	return found, nil
}

func (f *fakeRaft) SetUserExpiration(_ context.Context, userId string, expiresAt time.Time) error {
	f.sets = append(f.sets, setCall{userId: userId, expiresAt: expiresAt})
	return f.setErr
}

func TestSetUserExpiration(t *testing.T) {
	future := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Millisecond)
	futureBody := `{"expiresAt": "` + future.Format(time.RFC3339Nano) + `"}`
	pastBody := `{"expiresAt": "` + time.Now().Add(-time.Hour).Format(time.RFC3339Nano) + `"}`
	admin := &models.Principal{Username: "admin", UserType: models.UserTypeInputDb}
	existing := map[string]apikey.UserView{"user": {Id: "user", Active: true}}

	cases := []struct {
		name              string
		principal         *models.Principal
		namespacesEnabled bool
		userID            string
		body              string
		users             map[string]apikey.UserView
		static            config.StaticAPIKey
		authzErr          error
		getErr            error
		setErr            error
		wantKey           string
		wantCode          int
		wantSet           *setCall
	}{
		{name: "authz denied", body: futureBody, users: existing, authzErr: errors.New("forbidden"), wantCode: http.StatusForbidden},
		{
			name:      "namespaced own user by short id",
			principal: &models.Principal{Username: "customer1:user", UserType: models.UserTypeInputDb, Namespace: "customer1"}, namespacesEnabled: true,
			body: futureBody, users: map[string]apikey.UserView{"customer1:user": {Id: "customer1:user"}},
			wantKey: "customer1:user", wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:      "namespaced root user by short id",
			principal: &models.Principal{Username: "customer1:admin", UserType: models.UserTypeInputDb, Namespace: "customer1"}, namespacesEnabled: true,
			userID: "ns-root", body: futureBody, users: map[string]apikey.UserView{"customer1:ns-root": {Id: "customer1:ns-root"}},
			wantKey: "customer1:ns-root", wantCode: http.StatusUnprocessableEntity,
		},
		{name: "static user", userID: "static-user", body: futureBody, static: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}}, wantCode: http.StatusUnprocessableEntity},
		{
			name: "imported static user", userID: "static-user", body: futureBody,
			users:  map[string]apikey.UserView{"static-user": {Id: "static-user", ImportedWithKey: true}},
			static: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}}, wantCode: http.StatusOK,
			wantSet: &setCall{userId: "static-user", expiresAt: future},
		},
		{name: "unknown user", userID: "unknown", body: futureBody, users: existing, wantCode: http.StatusNotFound},
		{name: "GetUsers error", body: futureBody, getErr: errors.New("leader unreachable"), wantCode: http.StatusInternalServerError},
		{name: "past time", body: pastBody, users: existing, wantCode: http.StatusUnprocessableEntity},
		{name: "neither set, expiresAt null", body: `{"expiresAt": null}`, users: existing, wantCode: http.StatusUnprocessableEntity},
		{name: "neverExpires false", body: `{"neverExpires": false}`, users: existing, wantCode: http.StatusUnprocessableEntity},
		{name: "both set", body: `{"expiresAt": "` + future.Format(time.RFC3339Nano) + `", "neverExpires": true}`, users: existing, wantCode: http.StatusUnprocessableEntity},
		{name: "neverExpires true clears", body: `{"neverExpires": true}`, users: existing, wantCode: http.StatusOK, wantSet: &setCall{userId: "user"}},
		{
			name: "SetUserExpiration error", body: futureBody, users: existing, setErr: errors.New("apply failed"),
			wantCode: http.StatusInternalServerError, wantSet: &setCall{userId: "user", expiresAt: future},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			principal, userID, wantKey := admin, "user", "user"
			if tc.principal != nil {
				principal = tc.principal
			}
			if tc.userID != "" {
				userID, wantKey = tc.userID, tc.userID
			}
			if tc.wantKey != "" {
				wantKey = tc.wantKey
			}

			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().Authorize(mock.Anything, principal, authorization.UPDATE, authorization.Users(wantKey)[0]).Return(tc.authzErr)
			raft := &fakeRaft{users: tc.users, getErr: tc.getErr, setErr: tc.setErr}
			api := operations.NewWeaviateAPI(nil)
			SetupHandlers(api, raft, authorizer, dbusers.NewValidatingExpiry(),
				rbacconf.Config{RootUsers: []string{"root-user", "customer1:ns-root"}}, tc.static, tc.namespacesEnabled)
			var body users.SetUserExpirationBody
			require.NoError(t, runtime.JSONConsumer().Consume(strings.NewReader(tc.body), &body))
			params := users.SetUserExpirationParams{
				HTTPRequest: httptest.NewRequest(http.MethodPut, "/v1/users/db/"+userID+"/expiration", nil),
				UserID:      userID,
				Body:        body,
			}

			rec := httptest.NewRecorder()
			api.UsersSetUserExpirationHandler.Handle(params, principal).WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
			if tc.wantSet == nil {
				require.Empty(t, raft.sets)
				return
			}
			require.Equal(t, []setCall{*tc.wantSet}, raft.sets)
		})
	}
}
