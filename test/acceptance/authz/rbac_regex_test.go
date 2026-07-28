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
	"errors"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthZDataObjectRegexBreakout proves through the real enforcer that a regex
// in a data permission's object field cannot escalate beyond its collection. The
// object is the last path segment, so an unconfined payload "o|*" is stored as
// ".../objects/o|.*" = "(...objects/o)|(.*$)"; the second branch drops every
// anchor and matches any resource. custom-user, granted read_data only on the
// public collection, must still be denied the secret collection's object.
func TestAuthZDataObjectRegexBreakout(t *testing.T) {
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpShared(t)
	defer down()

	publicClass := "RegexBreakoutPublic"
	secretClass := "RegexBreakoutSecret"
	publicObj := strfmt.UUID(uuid.New().String())
	secretObj := strfmt.UUID(uuid.New().String())

	for _, class := range []string{publicClass, secretClass} {
		deleteObjectClass(t, class, adminAuth)
		require.NoError(t, createClass(t, &models.Class{
			Class:      class,
			Properties: []*models.Property{{Name: "prop", DataType: schema.DataTypeText.PropString()}},
		}, adminAuth))
	}
	defer deleteObjectClass(t, publicClass, adminAuth)
	defer deleteObjectClass(t, secretClass, adminAuth)

	helper.CreateObjectAuth(t, &models.Object{ID: publicObj, Class: publicClass, Properties: map[string]interface{}{"prop": "public"}}, adminKey)
	helper.CreateObjectAuth(t, &models.Object{ID: secretObj, Class: secretClass, Properties: map[string]interface{}{"prop": "secret"}}, adminKey)

	injectedObject := "o|*"
	roleName := "regexBreakoutRole"
	role := &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{{
			Action: String(authorization.ReadData),
			Data:   &models.PermissionData{Collection: &publicClass, Object: &injectedObject},
		}},
	}
	helper.DeleteRole(t, adminKey, roleName)
	helper.CreateRole(t, adminKey, role)
	defer helper.DeleteRole(t, adminKey, roleName)
	helper.AssignRoleToUser(t, adminKey, roleName, customUser)
	defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

	t.Run("breakout: object regex on public collection must not read the secret collection", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsClassGet(
			objects.NewObjectsClassGetParams().WithClassName(secretClass).WithID(secretObj), helper.CreateAuth(customKey))
		require.Error(t, err)
		var forbidden *objects.ObjectsClassGetForbidden
		require.True(t, errors.As(err, &forbidden),
			"object regex %q on %s escalated to %s: %v", injectedObject, publicClass, secretClass, err)
	})

	t.Run("grant still works within the public collection", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsClassGet(
			objects.NewObjectsClassGetParams().WithClassName(publicClass).WithID(publicObj), helper.CreateAuth(customKey))
		require.NoError(t, err)
	})

	// Cross-domain: the object segment is last, so an unconfined "o|*" second
	// branch is a bare ".*$" that matches resources in any domain — a data grant
	// reaching users. custom-user must be denied a user it has no users permission
	// for.
	t.Run("cross-domain breakout: data object regex must not read a user", func(t *testing.T) {
		bystander := "regex-breakout-bystander"
		helper.DeleteUser(t, bystander, adminKey)
		helper.CreateUser(t, bystander, adminKey)
		defer helper.DeleteUser(t, bystander, adminKey)

		_, err := helper.Client(t).Users.GetUserInfo(
			users.NewGetUserInfoParams().WithUserID(bystander), helper.CreateAuth(customKey))
		require.Error(t, err)
		var forbidden *users.GetUserInfoForbidden
		require.True(t, errors.As(err, &forbidden),
			"data object regex reached the users domain: %v", err)
	})
}

// TestAuthZInvalidRegexPermissionRejected proves the enforcer-panic DoS is closed
// at the API boundary: a permission target that would not compile as a casbin
// pattern (or is over-long, or contains '/') is rejected at role create (422), so
// it never reaches the matcher, and the node stays responsive.
func TestAuthZInvalidRegexPermissionRejected(t *testing.T) {
	adminKey := "admin-key"

	_, down := composeUpShared(t)
	defer down()

	roleName := "invalidRegexDoSRole"
	payloads := []struct {
		name   string
		object string
	}{
		{"unbalanced bracket", "["},
		{"unicode class escape (survives a naive compile check)", `\p{L}`},
		{"slash breaks segment boundaries", "a/b"},
		{"over-long target", strings.Repeat("a", 300)},
	}
	for _, p := range payloads {
		t.Run("rejected at create: "+p.name, func(t *testing.T) {
			object := p.object
			role := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{{
					Action: String(authorization.ReadData),
					Data:   &models.PermissionData{Collection: String("*"), Object: &object},
				}},
			}
			helper.DeleteRole(t, adminKey, roleName)
			_, err := helper.Client(t).Authz.CreateRole(
				authz.NewCreateRoleParams().WithBody(role), helper.CreateAuth(adminKey))
			require.Error(t, err)
			var unprocessable *authz.CreateRoleUnprocessableEntity
			require.True(t, errors.As(err, &unprocessable), "expected 422, got %v", err)
		})
	}

	t.Run("server stays responsive after the rejected creates", func(t *testing.T) {
		require.NotEmpty(t, helper.GetRoles(t, adminKey))
	})
}
