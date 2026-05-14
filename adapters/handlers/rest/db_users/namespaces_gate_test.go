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

package db_users

import (
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestMutatingHandlers_NamespacesEnabledNonOperatorForbidden locks the
// invariant that on namespace-enabled clusters every user-mutating handler
// rejects callers that are not global operators with 403, even if RBAC would
// otherwise permit the action. createUser is covered by
// TestCreateUser_Namespaces; this test covers the remaining handlers so the
// gate does not silently regress on any one of them.
func TestMutatingHandlers_NamespacesEnabledNonOperatorForbidden(t *testing.T) {
	const userID = "user"

	tests := []struct {
		name     string
		verb     string
		call     func(h *dynUserHandler, principal *models.Principal) middleware.Responder
		wantType any
	}{
		{
			name: "deleteUser",
			verb: authorization.DELETE,
			call: func(h *dynUserHandler, p *models.Principal) middleware.Responder {
				return h.deleteUser(users.DeleteUserParams{UserID: userID, HTTPRequest: req}, p)
			},
			wantType: &users.DeleteUserForbidden{},
		},
		{
			name: "rotateKey",
			verb: authorization.UPDATE,
			call: func(h *dynUserHandler, p *models.Principal) middleware.Responder {
				return h.rotateKey(users.RotateUserAPIKeyParams{UserID: userID, HTTPRequest: req}, p)
			},
			wantType: &users.RotateUserAPIKeyForbidden{},
		},
		{
			name: "deactivateUser",
			verb: authorization.UPDATE,
			call: func(h *dynUserHandler, p *models.Principal) middleware.Responder {
				return h.deactivateUser(users.DeactivateUserParams{UserID: userID, HTTPRequest: req}, p)
			},
			wantType: &users.DeactivateUserForbidden{},
		},
		{
			name: "activateUser",
			verb: authorization.UPDATE,
			call: func(h *dynUserHandler, p *models.Principal) middleware.Responder {
				return h.activateUser(users.ActivateUserParams{UserID: userID, HTTPRequest: req}, p)
			},
			wantType: &users.ActivateUserForbidden{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			principal := &models.Principal{Username: "alice", IsGlobalOperator: false}
			authorizer := authorization.NewMockAuthorizer(t)
			// RBAC permits the action — the gate must still reject it.
			authorizer.On("Authorize", mock.Anything, principal, tc.verb, authorization.Users(userID)[0]).Return(nil)

			h := &dynUserHandler{
				dbUsers:           NewMockDbUserAndRolesGetter(t),
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
			}

			res := tc.call(h, principal)
			assert.IsType(t, tc.wantType, res)
		})
	}
}
