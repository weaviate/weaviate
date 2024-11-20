package gql

import (
	"context"
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

func TestAuthZGraphQLGetWithRefs(t *testing.T) {
	t.Parallel()

	adminUser := "existing-user"
	adminKey := "existing-key"
	adminRole := "admin"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(customUser, customKey, customRole).
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
			{Action: String(authorization.ReadCollections)},
			{Action: String(authorization.ReadObjectsCollection), Collection: String(articlesCls.Class)},
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
	})

	t.Run("successfully query for just the articles", func(t *testing.T) {
		res := assertGQL(t, "{ Get { Article { title } } }", customKey)
		data, ok := res.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Len(t, data["Article"], 2)
	})

	t.Run("fail to query for articles when filtering on paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title } } }`, paragraph1UUID)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.Equal(t, len(resp.Payload.Errors), 1)
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("fail to query for articles returning paragraphs", func(t *testing.T) {
		query := "{ Get { Article { title hasParagraphs { ... on Paragraph { _additional { id } } } } } }"
		resp, err := queryGQL(t, query, customKey)
		fmt.Printf("%+v\n", resp.Payload.Data)
		require.Nil(t, err)
		require.Equal(t, len(resp.Payload.Errors), 1)
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("add permission to read data in paragraphs class", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(roleName),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsCollection),
				Collection: String(paragraphsCls.Class),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query for articles when filtering on paragraphs returning paragraphs", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { Article(where: {operator: Equal, path: ["hasParagraphs", "Paragraph", "_id"], valueText: "%s"}) { title hasParagraphs { ... on Paragraph { _additional { id } } } } } }`, paragraph1UUID)
		res := assertGQL(t, query, customKey)
		data, ok := res.Data["Get"].(map[string]any)
		require.True(t, ok)
		require.Len(t, data["Article"], 1)
		art, ok := data["Article"].([]any)[0].(map[string]any)
		require.True(t, ok)
		require.Len(t, art["hasParagraphs"], 2)
	})
}
