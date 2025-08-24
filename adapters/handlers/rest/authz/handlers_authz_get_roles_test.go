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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestGetRolesSuccess(t *testing.T) {
	type testCase struct {
		name            string
		principal       *models.Principal
		authorizedRoles []string
		expectedRoles   map[string][]authorization.Policy
	}

	tests := []testCase{
		{
			name:            "success non root user",
			principal:       &models.Principal{Username: "user1"},
			authorizedRoles: []string{"testRole"},
			expectedRoles: map[string][]authorization.Policy{
				"testRole": {},
			},
		},
		{
			name:            "success as root user",
			principal:       &models.Principal{Username: "root"},
			authorizedRoles: []string{"testRole", "root"},
			expectedRoles: map[string][]authorization.Policy{
				"testRole": {}, "root": {},
			},
		},
		{
			name:            "success without principal",
			principal:       nil,
			authorizedRoles: []string{"testRole"},
			expectedRoles: map[string][]authorization.Policy{
				"testRole": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()
			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles").Return(tt.expectedRoles, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
			}
			res := h.getRoles(authz.GetRolesParams{HTTPRequest: req}, tt.principal)
			parsed, ok := res.(*authz.GetRolesOK)
			assert.True(t, ok)
			assert.Len(t, parsed.Payload, len(tt.expectedRoles))
		})
	}
}
