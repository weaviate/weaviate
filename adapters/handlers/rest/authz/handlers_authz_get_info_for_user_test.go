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
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestGetInfoForUserSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.GetInfoForOwnUserParams{}

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.getInfoForOwnUser(params, principal)
	parsed, ok := res.(*authz.GetInfoForOwnUserOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)

	assert.Equal(t, principal.Username, *parsed.Payload.Username)
}

func TestGetInfoForUserUnauthenticated(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	params := authz.GetInfoForOwnUserParams{}

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.getInfoForOwnUser(params, nil)
	parsed, ok := res.(*authz.GetInfoForOwnUserUnauthorized)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}
