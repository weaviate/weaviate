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
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestGetRolesSuccess(t *testing.T) {
	returnedPolices := map[string][]authorization.Policy{
		"testRole": {}, "root": {},
	}

	type testCase struct {
		name          string
		principal     *models.Principal
		expectedRoles int
	}

	tests := []testCase{
		{
			name:          "success non root user",
			principal:     &models.Principal{Username: "user1"},
			expectedRoles: 1,
		},
		{
			name:          "success as root user",
			principal:     &models.Principal{Username: "root"},
			expectedRoles: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles").Return(returnedPolices, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{Enabled: true, Admins: []string{"root"}},
			}
			res := h.getRoles(authz.GetRolesParams{}, tt.principal)
			parsed, ok := res.(*authz.GetRolesOK)
			assert.True(t, ok)
			assert.Len(t, parsed.Payload, tt.expectedRoles)
		})
	}
}
