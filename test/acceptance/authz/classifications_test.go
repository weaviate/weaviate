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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/classifications"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzClassification(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"

	postRole := "post"
	getRole := "get"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	clsA := articles.ArticlesClass()
	clsB := articles.ParagraphsClass()

	classify := func() (*classifications.ClassificationsPostCreated, error) {
		return helper.Client(t).Classifications.ClassificationsPost(
			classifications.NewClassificationsPostParams().WithParams(&models.Classification{
				Class:              clsA.Class,
				ClassifyProperties: []string{"hasParagraphs"},
				BasedOnProperties:  []string{"title"},
				Type:               "knn",
				Settings: map[string]interface{}{
					"k": 5,
				},
			}), helper.CreateAuth(customKey),
		)
	}

	get := func(id string) (*classifications.ClassificationsGetOK, error) {
		return helper.Client(t).Classifications.ClassificationsGet(
			classifications.NewClassificationsGetParams().WithID(id), helper.CreateAuth(customKey),
		)
	}

	var id string

	t.Run("setup", func(t *testing.T) {
		helper.CreateClassAuth(t, clsB, adminKey)
		helper.CreateClassAuth(t, clsA, adminKey)
		helper.CreateObjectAuth(t, articles.NewParagraph().WithID(UUID1).WithContents("Hello, World!").Object(), adminKey)
		helper.CreateObjectAuth(t, articles.NewArticle().WithID(UUID1).WithTitle("Classifications").WithReferences(&models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/" + clsB.Class + "/" + UUID1.String()),
			Class:  strfmt.URI(clsB.Class),
		}).Object(), adminKey)
	})

	t.Run("fail to start a classification without update_collections:Article", func(t *testing.T) {
		_, err := classify()
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsPostForbidden) //nolint:errorlint
		require.True(t, ok)
		require.Contains(t, parsed.Payload.Error[0].Message, authorization.UpdateCollections)
		require.Contains(t, parsed.Payload.Error[0].Message, clsA.Class)
	})

	t.Run("add the permission to update the collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(&models.Role{
			Name: &postRole,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(clsA.Class).Permission(),
			},
		}), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		helper.AssignRoleToUser(t, adminKey, postRole, customUser)
	})

	t.Run("fail to start a classification without read_data:Article", func(t *testing.T) {
		_, err := classify()
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsPostForbidden) //nolint:errorlint
		require.True(t, ok)
		require.Contains(t, parsed.Payload.Error[0].Message, authorization.ReadData)
		require.Contains(t, parsed.Payload.Error[0].Message, clsA.Class)
	})

	t.Run("add the permission to read data in the collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(postRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{
					helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(clsA.Class).Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
	})

	t.Run("fail to start a classification without read_collections:Article", func(t *testing.T) {
		_, err := classify()
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsPostForbidden) //nolint:errorlint
		require.True(t, ok)
		require.Contains(t, parsed.Payload.Error[0].Message, authorization.ReadCollections)
		require.Contains(t, parsed.Payload.Error[0].Message, clsA.Class)
	})

	t.Run("add the permission to read the collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(postRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{
					helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(clsA.Class).Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
	})

	t.Run("fail to start a classification without read_data:Paragraph", func(t *testing.T) {
		_, err := classify()
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsPostForbidden) //nolint:errorlint
		require.True(t, ok)
		require.Contains(t, parsed.Payload.Error[0].Message, authorization.ReadData)
		require.Contains(t, parsed.Payload.Error[0].Message, clsB.Class)
	})

	t.Run("add the permission to read data in the reference collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(postRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{
					helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(clsB.Class).Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
	})

	t.Run("fail to start a classification without read_collections:Paragraph", func(t *testing.T) {
		_, err := classify()
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsPostForbidden) //nolint:errorlint
		require.True(t, ok)
		require.Contains(t, parsed.Payload.Error[0].Message, authorization.ReadCollections)
		require.Contains(t, parsed.Payload.Error[0].Message, clsB.Class)
	})

	t.Run("add the permission to read the reference collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(postRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{
					helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(clsB.Class).Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
	})

	t.Run("start a classification with the correct permissions", func(t *testing.T) {
		res, err := classify()
		require.NoError(t, err)
		id = res.Payload.ID.String()
	})

	t.Run("revoke postRole from user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{postRole}}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
	})

	t.Run("fail to get a classification without the correct permissions", func(t *testing.T) {
		_, err := get(id)
		require.Error(t, err)
		parsed, ok := err.(*classifications.ClassificationsGetForbidden) //nolint:errorlint
		require.True(t, ok)
		require.NotNil(t, parsed.Payload)
	})

	t.Run("add the permission to read the collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: &getRole,
				Permissions: []*models.Permission{
					helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(clsA.Class).Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		helper.AssignRoleToUser(t, adminKey, getRole, customUser)
	})

	t.Run("get a classification with the correct permissions", func(t *testing.T) {
		_, err := get(id)
		require.NoError(t, err)
	})
}
