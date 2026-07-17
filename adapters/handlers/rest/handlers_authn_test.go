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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
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

	tests := []struct {
		name      string
		principal *models.Principal
		wantUser  string
	}{
		{
			name:      "namespaced caller: own-namespace user-ref stripped",
			principal: &models.Principal{Username: "customer1:u", UserType: "db", Namespace: "customer1"},
			wantUser:  "apiuser",
		},
		{
			name:      "operator: response stays raw",
			principal: &models.Principal{Username: "admin", UserType: "db", IsGlobalOperator: true},
			wantUser:  "customer1:apiuser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := authorization.NewMockController(t)
			controller.On("GetRolesForUserOrGroup", tt.principal.Username, mock.Anything, false).
				Return(map[string][]authorization.Policy{"viewer": policies}, nil)

			logger, _ := test.NewNullLogger()
			h := &authNHandlers{
				authzController: controller,
				rbacConfig:      rbacconf.Config{Enabled: true},
				logger:          logger,
			}

			res := h.getOwnInfo(users.GetOwnInfoParams{}, tt.principal)
			parsed, ok := res.(*users.GetOwnInfoOK)
			require.True(t, ok, "got %T", res)
			require.Len(t, parsed.Payload.Roles, 1)
			require.Len(t, parsed.Payload.Roles[0].Permissions, 1)
			require.NotNil(t, parsed.Payload.Roles[0].Permissions[0].Users)
			require.Equal(t, tt.wantUser, *parsed.Payload.Roles[0].Permissions[0].Users.Users)
		})
	}
}
