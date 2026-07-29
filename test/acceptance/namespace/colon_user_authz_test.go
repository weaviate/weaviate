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

package namespace

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestGlobalCallerColonUserIDAuthz is the e2e guard for the colon-in-user-id
// matcher fix. A global static-key caller (ns=="") with a narrow grant over
// "gtarget" must not have it leak to a different user "x:gtarget" — expect 403
// (deny), not 404 (authorized-through to a missing user).
func TestGlobalCallerColonUserIDAuthz(t *testing.T) {
	t.Parallel()

	// gCaller (global, ns=="") gets a custom role granting read over one user:
	// users/gtarget. Unqualified, so it passes the no-qualified-namespace validator.
	readUsers := authorization.ReadUsers
	target := gTarget
	const roleName = "colon-authz-reader"
	helper.CreateRoleAndAssign(t, adminKey, gCaller, roleName, &models.Permission{
		Action: &readUsers,
		Users:  &models.PermissionUsers{Users: &target},
	})

	// Positive control: the grant authorizes the exact target. Polling also
	// settles RAFT-apply lag, so the deny below is the matcher, not lag.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.Client(t).Authz.GetRolesForUser(
			authz.NewGetRolesForUserParams().
				WithID(gTarget).
				WithUserType(string(models.UserTypeInputDb)),
			helper.CreateAuth(gCallerKey),
		)
		assert.NoError(c, err, "grant over users/%s must authorize the caller", gTarget)
	}, 15*time.Second, 100*time.Millisecond, "role binding for %q never became authorizing", roleName)

	// The fix: the same grant must NOT reach a different colon-bearing user.
	_, err := helper.Client(t).Authz.GetRolesForUser(
		authz.NewGetRolesForUserParams().
			WithID("x:"+gTarget).
			WithUserType(string(models.UserTypeInputDb)),
		helper.CreateAuth(gCallerKey),
	)
	require.Error(t, err)
	var forbidden *authz.GetRolesForUserForbidden
	require.True(t, errors.As(err, &forbidden),
		"global caller's grant over users/%s must not widen to users/x:%s; got %T", gTarget, gTarget, err)
}
