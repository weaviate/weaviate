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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestRemovePermissionsSuccessUpdate(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.RemovePermissionsParams{
		ID: "test",
		Body: authz.RemovePermissionsBody{
			Permissions: []*models.Permission{
				{
					Action: String("manage_roles"),
					Roles:  &models.PermissionRoles{},
				},
			},
		},
	}
	policies, err := conv.PermissionToPolicies(params.Body.Permissions...)
	require.Nil(t, err)

	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Roles(params.ID)[0]).Return(nil)
	controller.On("GetRoles", params.ID).Return(map[string][]authorization.Policy{params.ID: {
		{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
		{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
	}}, nil)
	controller.On("RemovePermissions", params.ID, policies).Return(nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.removePermissions(params, principal)
	parsed, ok := res.(*authz.RemovePermissionsOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestRemovePermissionsBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RemovePermissionsParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role has to have at least 1 permission",
			params: authz.RemovePermissionsParams{
				ID: "someRole",
				Body: authz.RemovePermissionsBody{
					Permissions: []*models.Permission{},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role has to have at least 1 permission",
		},
		// {
		// 	name: "invalid permission",
		// 	params: authz.RemovePermissionsParams{
		// 		Body: authz.RemovePermissionsBody{
		// 			Name: String("someName"),
		// 			Permissions: []*models.Permission{
		// 				{
		// 					Action: String("manage_roles"),
		// 				},
		// 			},
		// 		},
		// 	},
		// 	principal:     &models.Principal{Username: "user1"},
		// 	expectedError: "invalid permission",
		// },
		{
			name: "update builtin role",
			params: authz.RemovePermissionsParams{
				ID: authorization.BuiltInRoles[0],
				Body: authz.RemovePermissionsBody{
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not update builtin role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			h := &authZHandlers{
				controller: controller,
				logger:     logger,
			}
			res := h.removePermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.RemovePermissionsBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRemovePermissionsForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RemovePermissionsParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "remove permissions",
			params: authz.RemovePermissionsParams{
				ID: "someRole",
				Body: authz.RemovePermissionsBody{
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
							Roles:  &models.PermissionRoles{},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("some error from authZ"),
			expectedError: "some error from authZ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.removePermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.RemovePermissionsForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRemovePermissionsRoleNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RemovePermissionsParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role not found",
			params: authz.RemovePermissionsParams{
				ID: "some role",
				Body: authz.RemovePermissionsBody{
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{
								Collection: String("*"),
							},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.ID).Return(map[string][]authorization.Policy{}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.removePermissions(tt.params, tt.principal)
			_, ok := res.(*authz.RemovePermissionsNotFound)
			assert.True(t, ok)
		})
	}
}

func TestRemovePermissionsInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RemovePermissionsParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "update some role",
			params: authz.RemovePermissionsParams{
				ID: "someRole",
				Body: authz.RemovePermissionsBody{
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
							Roles:  &models.PermissionRoles{},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			upsertErr:     fmt.Errorf("some error from controller"),
			expectedError: "some error from controller",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.ID).Return(map[string][]authorization.Policy{tt.params.ID: {
				{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
				{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
			}}, nil)
			controller.On("RemovePermissions", mock.Anything, mock.Anything).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.removePermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.RemovePermissionsInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
