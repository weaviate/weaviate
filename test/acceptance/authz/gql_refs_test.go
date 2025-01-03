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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/client/graphql"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGraphQLRefs(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"

	_, teardown := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer teardown()

	articlesCls := articles.ArticlesClass()
	paragraphsCls := articles.ParagraphsClass()

	roleName := "can-query-articles"

	t.Run("create classes", func(t *testing.T) {
		helper.CreateClassAuth(t, paragraphsCls, adminKey)
		helper.CreateClassAuth(t, articlesCls, adminKey)
	})

	t.Run("import objects", func(t *testing.T) {
		paragraphObjs := make([]*models.Object, 0)
		paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(UUID1).Object())
		paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(UUID2).Object())
		helper.CreateObjectsBatchAuth(t, paragraphObjs, adminKey)

		articleObjs := make([]*models.Object, 0)
		articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 1").WithReferences(
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + UUID1.String())},
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + UUID2.String())},
		).Object())
		articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 2").Object())
		helper.CreateObjectsBatchAuth(t, articleObjs, adminKey)
	})

	t.Run("create and assign a role that can query for articles", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{Name: String(roleName), Permissions: []*models.Permission{
			{Action: String(authorization.ReadCollections), Collections: &models.PermissionCollections{Collection: authorization.All}},
			{Action: String(authorization.ReadData), Data: &models.PermissionData{Collection: String(articlesCls.Class)}},
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
	})

	t.Run("successfully query with Get for just the articles", func(t *testing.T) {
		res := assertGQL(t, "{ Get { Article { title } } }", customKey)
		data, ok := res.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Len(t, data["Article"], 2)
	})

	t.Run("successfully query with Aggregate for just the articles data", func(t *testing.T) {
		res := assertGQL(t, "{ Aggregate { Article { meta { count } } } }", customKey)
		data, ok := res.Data["Aggregate"].(map[string]any)
		require.True(t, ok)
		dataL, ok := data["Article"].([]any)
		require.True(t, ok)
		require.Equal(t, json.Number("2"), dataL[0].(map[string]any)["meta"].(map[string]any)["count"])
	})

	t.Run("fail to query with Get for articles when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title } } }`, UUID1)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(resp.Payload.Errors))
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("fail to query with Aggregate for articles when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Aggregate { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { meta { count } } } }`, UUID1)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(resp.Payload.Errors))
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("fail to query for articles returning paragraphs", func(t *testing.T) {
		query := "{ Get { Article { title hasParagraphs { ... on Paragraph { _additional { id } } } } } }"
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(resp.Payload.Errors))
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("add permission to read data in paragraphs class", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(roleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{{
				Action: String(authorization.ReadData),
				Data:   &models.PermissionData{Collection: String(paragraphsCls.Class)},
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query for articles with Get when filtering on paragraphs returning paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title hasParagraphs { ... on Paragraph { _additional { id } } } } } }`, UUID1)
		res := assertGQL(t, query, customKey)
		data, ok := res.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Len(t, data["Article"], 1)
		art, ok := data["Article"].([]any)[0].(map[string]any)
		require.True(t, ok)
		require.Len(t, art["hasParagraphs"], 2)
	})

	t.Run("successfully query for articles with Aggregate when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Aggregate { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { meta { count } } } }`, UUID1)
		res := assertGQL(t, query, customKey)
		data, ok := res.Data["Aggregate"].(map[string]any)
		require.True(t, ok)
		dataL, ok := data["Article"].([]any)
		require.True(t, ok)
		require.Equal(t, json.Number("1"), dataL[0].(map[string]any)["meta"].(map[string]any)["count"])
	})
}

func TestAuthZGraphQLRefsGroupBy(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	articlesCls := articles.ArticlesClass()
	paragraphsCls := articles.ParagraphsClass()

	helper.DeleteClassWithAuthz(t, paragraphsCls.Class, adminAuth)
	helper.DeleteClassWithAuthz(t, articlesCls.Class, adminAuth)
	helper.CreateClassAuth(t, paragraphsCls, adminKey)
	helper.CreateClassAuth(t, articlesCls, adminKey)
	defer helper.DeleteClassWithAuthz(t, paragraphsCls.Class, adminAuth)
	defer helper.DeleteClassWithAuthz(t, articlesCls.Class, adminAuth)

	paragraphObjs := []*models.Object{articles.NewParagraph().WithID(UUID1).Object(), articles.NewParagraph().WithID(UUID2).Object()}
	helper.CreateObjectsBatchAuth(t, paragraphObjs, adminKey)

	articleObjs := make([]*models.Object, 0)
	articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 1").WithReferences(
		&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + UUID1.String())},
		&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + UUID2.String())},
	).WithVector([]float32{1, 0}).Object())
	articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 2").WithVector([]float32{1, 0}).Object())
	helper.CreateObjectsBatchAuth(t, articleObjs, adminKey)

	roleName := t.Name() + "_role"

	readCollectionsAction := authorization.ReadCollections
	readDataAction := authorization.ReadData

	all := "*"
	requiredPermissions := []*models.Permission{
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &paragraphsCls.Class},
		},
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
	}

	groupByQuery := `{
				  Get {
					Article(
					  groupBy: {groups: 10, objectsPerGroup: 10, path: "hasParagraphs"}
					  nearVector: {vector: [1, 0]}
					) {
					  _additional {
						group {
						  hits {
							_additional {
							  id
							}
							hasParagraphs {
							  ... on Paragraph {
								contents
							  }
							}
						  }
						}
					  }
					}
				  }
				}
				`

	t.Run("create and assign a role that can query for articles", func(t *testing.T) {
		role := &models.Role{Name: &roleName, Permissions: requiredPermissions}
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.DeleteRole(t, adminKey, *role.Name)

		res, err := queryGQL(t, groupByQuery, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Nil(t, res.Payload.Errors)
		data, ok := res.Payload.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Contains(t, data, "Article")
	})

	t.Run("One permission is missing", func(t *testing.T) {
		for _, permissions := range generateMissingLists(requiredPermissions) {
			role := &models.Role{Name: &roleName, Permissions: permissions}
			helper.DeleteRole(t, adminKey, *role.Name)
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer helper.DeleteRole(t, adminKey, *role.Name)
			res, err := queryGQL(t, groupByQuery, customKey)
			if err != nil {
				require.Nil(t, res)
				var errForbidden *graphql.GraphqlPostForbidden
				require.True(t, errors.As(err, &errForbidden))
			} else {
				require.NotNil(t, res.Payload.Errors)
				require.Contains(t, res.Payload.Errors[0].Message, "forbidden")
			}
		}
	})
}
