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

package rest

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

func TestRemovePermissionsSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.RemovePermissionsParams{
		Body: authz.RemovePermissionsBody{
			Name: String("test"),
			Permissions: []*models.Permission{
				{
					Action: String("manage_roles"),
				},
			},
		},
	}
	policies, err := conv.PermissionToPolicies(params.Body.Permissions...)
	require.Nil(t, err)

	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Roles("test")[0]).Return(nil)
	controller.On("RemovePermissions", *params.Body.Name, policies).Return(nil)

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
			name: "role name is required",
			params: authz.RemovePermissionsParams{
				Body: authz.RemovePermissionsBody{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role name is required",
		},
		{
			name: "role has to have at least 1 permission",
			params: authz.RemovePermissionsParams{
				Body: authz.RemovePermissionsBody{
					Name:        String("someName"),
					Permissions: []*models.Permission{},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role has to have at least 1 permission",
		},
		{
			name: "invalid action",
			params: authz.RemovePermissionsParams{
				Body: authz.RemovePermissionsBody{
					Name: String("someName"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_somethingelse"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "invalid permission",
		},
		{
			name: "update builtin role",
			params: authz.RemovePermissionsParams{
				Body: authz.RemovePermissionsBody{
					Name: &authorization.BuiltInRoles[0],
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
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)

			h := &authZHandlers{
				authorizer: authorizer,
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
			name: "update some role",
			params: authz.RemovePermissionsParams{
				Body: authz.RemovePermissionsBody{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)

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
				Body: authz.RemovePermissionsBody{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
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
