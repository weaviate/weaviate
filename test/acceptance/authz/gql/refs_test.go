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

package gql

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGraphQLRefs(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacAdmins(adminUser).
		Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	articlesCls := articles.ArticlesClass()
	paragraphsCls := articles.ParagraphsClass()

	paragraph1UUID := strfmt.UUID("00000000-0000-0000-0000-000000000001")
	paragraph2UUID := strfmt.UUID("00000000-0000-0000-0000-000000000002")

	roleName := "can-query-articles"

	t.Run("create classes", func(t *testing.T) {
		helper.CreateClassAuth(t, paragraphsCls, adminKey)
		helper.CreateClassAuth(t, articlesCls, adminKey)
	})

	t.Run("import objects", func(t *testing.T) {
		paragraphObjs := make([]*models.Object, 0)
		paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(paragraph1UUID).Object())
		paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(paragraph2UUID).Object())
		helper.CreateObjectsBatchAuth(t, paragraphObjs, adminKey)

		articleObjs := make([]*models.Object, 0)
		articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 1").WithReferences(
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + paragraph1UUID.String())},
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + paragraph2UUID.String())},
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
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title } } }`, paragraph1UUID)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(resp.Payload.Errors))
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("fail to query with Aggregate for articles when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Aggregate { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { meta { count } } } }`, paragraph1UUID)
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
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(roleName),
			Permissions: []*models.Permission{{
				Action: String(authorization.ReadData),
				Data:   &models.PermissionData{Collection: String(paragraphsCls.Class)},
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query for articles with Get when filtering on paragraphs returning paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title hasParagraphs { ... on Paragraph { _additional { id } } } } } }`, paragraph1UUID)
		res := assertGQL(t, query, customKey)
		data, ok := res.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Len(t, data["Article"], 1)
		art, ok := data["Article"].([]any)[0].(map[string]any)
		require.True(t, ok)
		require.Len(t, art["hasParagraphs"], 2)
	})

	t.Run("successfully query for articles with Aggregate when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Aggregate { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { meta { count } } } }`, paragraph1UUID)
		res := assertGQL(t, query, customKey)
		data, ok := res.Data["Aggregate"].(map[string]any)
		require.True(t, ok)
		dataL, ok := data["Article"].([]any)
		require.True(t, ok)
		require.Equal(t, json.Number("1"), dataL[0].(map[string]any)["meta"].(map[string]any)["count"])
	})
}
