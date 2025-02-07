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
	schemaMocks "github.com/weaviate/weaviate/usecases/schema/mocks"
)

func TestAddPermissionsSuccess(t *testing.T) {
	type testCase struct {
		name      string
		principal *models.Principal
		params    authz.AddPermissionsParams
	}

	tests := []testCase{
		{
			name:      "all are *",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				ID: "test",
				Body: authz.AddPermissionsBody{
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
		},
		{
			name:      "collection checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				ID: "newRole",
				Body: authz.AddPermissionsBody{
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{Collection: String("ABC")},
						},
					},
				},
			},
		},
		{
			name:      "collection and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				ID: "newRole",
				Body: authz.AddPermissionsBody{
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{
								Collection: String("ABC"),
							},
						},
					},
				},
			},
		},
		{
			name:      "* collections and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				ID: "newRole",
				Body: authz.AddPermissionsBody{
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(&models.Role{
				Name:        &tt.params.ID,
				Permissions: tt.params.Body.Permissions,
			})
			require.Nil(t, err)

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.ID).Return(map[string][]authorization.Policy{
				"test": {
					{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
				},
			}, nil)
			controller.On("UpsertRolesPermissions", policies).Return(nil)

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestAddPermissionsBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role has to have at least 1 permission",
			params: authz.AddPermissionsParams{
				ID: "someName",
				Body: authz.AddPermissionsBody{
					Permissions: []*models.Permission{},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role has to have at least 1 permission",
		},
		{
			name: "update builtin role",
			params: authz.AddPermissionsParams{
				ID: authorization.BuiltInRoles[0],
				Body: authz.AddPermissionsBody{
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not update built-in role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := mocks.NewController(t)
			authorizer := mocks.NewAuthorizer(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()
			h := &authZHandlers{
				controller:   controller,
				authorizer:   authorizer,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAddPermissionsForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "update some role",
			params: authz.AddPermissionsParams{
				ID: "someRole",
				Body: authz.AddPermissionsBody{
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
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAddPermissionsRoleNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role not found",
			params: authz.AddPermissionsParams{
				ID: "some role",
				Body: authz.AddPermissionsBody{
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
			res := h.addPermissions(tt.params, tt.principal)
			_, ok := res.(*authz.AddPermissionsNotFound)
			assert.True(t, ok)
		})
	}
}

func TestAddPermissionsInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "update some role",
			params: authz.AddPermissionsParams{
				ID: "someRole",
				Body: authz.AddPermissionsBody{
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
			controller.On("GetRoles", tt.params.ID).Return(map[string][]authorization.Policy{
				"test": {
					{Resource: "whatever", Verb: authorization.READ, Domain: "whatever"},
				},
			}, nil)
			controller.On("UpsertRolesPermissions", mock.Anything).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
