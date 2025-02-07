//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestAuthorizeRoleScopes(t *testing.T) {
	type testCase struct {
		name           string
		principal      *models.Principal
		originalVerb   string
		policies       []authorization.Policy
		roleName       string
		authorizeSetup func(*mocks.Authorizer)
		expectedError  string
	}

	tests := []testCase{
		{
			name:         "has full role management permissions",
			principal:    &models.Principal{Username: "admin"},
			originalVerb: authorization.CREATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "newRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call succeeds - has full permissions
				a.On("Authorize", &models.Principal{Username: "admin"}, authorization.CREATE, authorization.Roles("newRole")[0]).
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match and all required permissions",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.CREATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "newRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.CREATE, authorization.Roles("newRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("newRole")[0]).
					Return(nil).Once()
				// Third call succeeds - has required permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.READ, "collections/ABC").
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match but missing required permissions",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.CREATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
				{Resource: "collections/XYZ", Verb: authorization.UPDATE},
			},
			roleName: "newRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.CREATE, authorization.Roles("newRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("newRole")[0]).
					Return(nil).Once()
				// Third call succeeds - has first permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.READ, "collections/ABC").
					Return(nil).Once()
				// Fourth call fails - missing second permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.UPDATE, "collections/XYZ").
					Return(errors.New("missing write permission")).Once()
			},
			expectedError: "missing write permission",
		},
		{
			name:         "has neither full management nor role scope match",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.CREATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "newRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.CREATE, authorization.Roles("newRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call fails - no role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("newRole")[0]).
					Return(errors.New("no role scope match")).Once()
			},
			expectedError: "can only create roles with less or equal permissions as the current user: no role scope match",
		},
		{
			name:         "has full role management permissions for update",
			principal:    &models.Principal{Username: "admin"},
			originalVerb: authorization.UPDATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call succeeds - has full permissions
				a.On("Authorize", &models.Principal{Username: "admin"}, authorization.UPDATE, authorization.Roles("existingRole")[0]).
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match and all required permissions for update",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.UPDATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.UPDATE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(nil).Once()
				// Third call succeeds - has required permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.READ, "collections/ABC").
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match but missing some required permissions for update",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.UPDATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
				{Resource: "collections/XYZ", Verb: authorization.DELETE},
			},
			roleName: "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.UPDATE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(nil).Once()
				// Third call succeeds - has first permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.READ, "collections/ABC").
					Return(nil).Once()
				// Fourth call fails - missing delete permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.DELETE, "collections/XYZ").
					Return(errors.New("missing delete permission")).Once()
			},
			expectedError: "missing delete permission",
		},
		{
			name:         "has neither full management nor role scope match for update",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.UPDATE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
			},
			roleName: "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.UPDATE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call fails - no role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(errors.New("no role scope match")).Once()
			},
			expectedError: "can only create roles with less or equal permissions as the current user: no role scope match",
		},
		{
			name:         "has full role management permissions for delete",
			principal:    &models.Principal{Username: "admin"},
			originalVerb: authorization.DELETE,
			policies:     []authorization.Policy{},
			roleName:     "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call succeeds - has full permissions
				a.On("Authorize", &models.Principal{Username: "admin"}, authorization.DELETE, authorization.Roles("existingRole")[0]).
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match for delete",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.DELETE,
			policies:     []authorization.Policy{},
			roleName:     "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.DELETE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.DELETE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(nil).Once()
			},
			expectedError: "",
		},
		{
			name:         "has role scope match but missing permissions for delete",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.DELETE,
			policies: []authorization.Policy{
				{Resource: "collections/ABC", Verb: authorization.READ},
				{Resource: "collections/XYZ", Verb: authorization.DELETE},
			},
			roleName: "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.DELETE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.DELETE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(nil).Once()
				// Third call succeeds - has first permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.READ, "collections/ABC").
					Return(nil).Once()
				// Fourth call fails - missing delete permission
				a.On("AuthorizeSilent", &models.Principal{Username: "user"}, authorization.DELETE, "collections/XYZ").
					Return(errors.New("missing delete permission")).Once()
			},
			expectedError: "missing delete permission",
		},
		{
			name:         "get role fails during delete",
			principal:    &models.Principal{Username: "user"},
			originalVerb: authorization.DELETE,
			policies:     nil,
			roleName:     "existingRole",
			authorizeSetup: func(a *mocks.Authorizer) {
				// First call fails - no full permissions
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.DELETE, authorization.Roles("existingRole")[0]).
					Return(errors.New("no full permissions")).Once()
				// Second call succeeds - has role scope match
				a.On("Authorize", &models.Principal{Username: "user"}, authorization.VerbWithScope(authorization.DELETE, authorization.ROLE_SCOPE_MATCH), authorization.Roles("existingRole")[0]).
					Return(nil).Once()
			},
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			logger, _ := test.NewNullLogger()

			if tt.authorizeSetup != nil {
				tt.authorizeSetup(authorizer)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				logger:     logger,
			}

			err := h.authorizeRoleScopes(tt.principal, tt.originalVerb, tt.policies, tt.roleName)

			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.expectedError)
			}

			authorizer.AssertExpectations(t)
		})
	}
}
