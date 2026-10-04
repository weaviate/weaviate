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

package rest

import (
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// TestGetOwnInfo pins that GET /users/me strips the caller's own namespace
// from every namespace-bearing field a role can carry — including the user-ref
// added by namespaced user management — for a namespaced caller, while an
// operator's response stays raw.
func TestGetOwnInfo(t *testing.T) {
	// A role whose only permission references a user id in the caller's own
	// namespace. Built via the conv round-trip so it is a real stored policy.
	userRefPerm := &models.Permission{
		Action: strPtr("read_users"),
		Users:  &models.PermissionUsers{Users: strPtr("customer1:apiuser")},
	}
	policyPtrs, err := conv.PermissionToPolicies(userRefPerm)
	require.NoError(t, err)
	policies := make([]authorization.Policy, len(policyPtrs))
	for i, p := range policyPtrs {
		policies[i] = *p
	}

	expiresAt := strfmt.DateTime(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))

	tests := []struct {
		name      string
		principal *models.Principal
		// lookup is what GetUsers returns for the principal's name. A nil lookup
		// sets no expectation, so a call to GetUsers fails the row.
		lookup        map[string]apikey.UserView
		lookupErr     error
		wantUser      string
		wantExpiresAt *strfmt.DateTime
	}{
		{
			name:      "namespaced caller: own-namespace user-ref stripped",
			principal: &models.Principal{Username: "customer1:u", UserType: "db", Namespace: "customer1", IsDynamicDbUser: true},
			lookup:    map[string]apikey.UserView{},
			wantUser:  "apiuser",
		},
		{
			name:      "static key operator: response stays raw, store not read even if a DB user shares its name",
			principal: &models.Principal{Username: "admin", UserType: "db", IsGlobalOperator: true},
			wantUser:  "customer1:apiuser",
		},
		{
			name:          "namespaced db caller's expiry is read by its qualified name",
			principal:     &models.Principal{Username: "customer1:u", UserType: "db", Namespace: "customer1", IsDynamicDbUser: true},
			lookup:        map[string]apikey.UserView{"customer1:u": {Id: "customer1:u", ExpiresAt: time.Time(expiresAt)}},
			wantUser:      "apiuser",
			wantExpiresAt: &expiresAt,
		},
		{
			name:      "store read fails",
			principal: &models.Principal{Username: "customer1:u", UserType: "db", Namespace: "customer1", IsDynamicDbUser: true},
			lookup:    map[string]apikey.UserView{},
			lookupErr: errors.New("store unavailable"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := authorization.NewMockController(t)
			controller.On("GetRolesForUserOrGroup", tt.principal.Username, mock.Anything, false).
				Return(map[string][]authorization.Policy{"viewer": policies}, nil)
			localUsers := db_users.NewMockDbUserAndRolesGetter(t)
			if tt.lookup != nil {
				localUsers.On("GetUsers", tt.principal.Username).Return(tt.lookup, tt.lookupErr)
			}

			logger, _ := test.NewNullLogger()
			h := &authNHandlers{
				authzController: controller,
				rbacConfig:      rbacconf.Config{Enabled: true},
				localUsers:      localUsers,
				dbUsersEnabled:  true,
				logger:          logger,
			}

			res := h.getOwnInfo(users.GetOwnInfoParams{}, tt.principal)
			if tt.lookupErr != nil {
				_, ok := res.(*users.GetOwnInfoInternalServerError)
				require.True(t, ok, "got %T", res)
				return
			}
			parsed, ok := res.(*users.GetOwnInfoOK)
			require.True(t, ok, "got %T", res)
			require.Len(t, parsed.Payload.Roles, 1)
			require.Len(t, parsed.Payload.Roles[0].Permissions, 1)
			require.NotNil(t, parsed.Payload.Roles[0].Permissions[0].Users)
			require.Equal(t, tt.wantUser, *parsed.Payload.Roles[0].Permissions[0].Users.Users)
			require.Equal(t, tt.wantExpiresAt, parsed.Payload.ExpiresAt)
		})
	}
}
