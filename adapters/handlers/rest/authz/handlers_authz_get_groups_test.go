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

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestGetGroups(t *testing.T) {
	principal := &models.Principal{Username: "user1", UserType: models.UserTypeInputOidc}
	groups := []string{"group1", "group2"}

	tests := []struct {
		name       string
		groupType  string
		setupMocks func(*authorization.MockAuthorizer, *MockControllerAndGetUsers)
		assertRes  func(*testing.T, middleware.Responder)
	}{
		{
			name:      "success returns filtered groups",
			groupType: string(authentication.AuthTypeOIDC),
			setupMocks: func(a *authorization.MockAuthorizer, c *MockControllerAndGetUsers) {
				c.On("GetUsersOrGroupsWithRoles", true, authentication.AuthTypeOIDC).Return(groups, nil)
				a.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Groups(authentication.AuthTypeOIDC, groups[0])[0]).Return(nil)
			},
			assertRes: func(t *testing.T, res middleware.Responder) {
				parsed, ok := res.(*authz.GetGroupsOK)
				assert.True(t, ok)
				assert.Equal(t, groups, parsed.Payload)
			},
		},
		{
			name:       "non-oidc group type is a bad request",
			groupType:  string(authentication.AuthTypeDb),
			setupMocks: func(a *authorization.MockAuthorizer, c *MockControllerAndGetUsers) {},
			assertRes: func(t *testing.T, res middleware.Responder) {
				_, ok := res.(*authz.GetGroupsBadRequest)
				assert.True(t, ok)
			},
		},
		{
			name:      "controller error surfaces as internal server error",
			groupType: string(authentication.AuthTypeOIDC),
			setupMocks: func(a *authorization.MockAuthorizer, c *MockControllerAndGetUsers) {
				c.On("GetUsersOrGroupsWithRoles", true, authentication.AuthTypeOIDC).Return(nil, fmt.Errorf("internal server error"))
			},
			assertRes: func(t *testing.T, res middleware.Responder) {
				parsed, ok := res.(*authz.GetGroupsInternalServerError)
				assert.True(t, ok)
				assert.Contains(t, parsed.Payload.Error[0].Message, "internal server error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()
			tt.setupMocks(authorizer, controller)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getGroups(authz.GetGroupsParams{
				GroupType:   tt.groupType,
				HTTPRequest: req,
			}, principal)

			tt.assertRes(t, res)
		})
	}
}
