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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthzSearchWithGRPC(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	grpcClient := helper.ClientGRPC(t)

	t.Run("setup", func(t *testing.T) {
		helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
		helper.CreateClassAuth(t, articles.ArticlesClass(), adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("How to git gud").Object()}, adminKey)
	})

	t.Run("correctly fail to perform a gRPC Search call without permissions", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		_, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: articles.ArticlesClass().Class,
		})
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.PermissionDenied)
	})

	t.Run("create role with necessary permissions on articles and assign to custom user", func(t *testing.T) {
		roleName := "role"
		helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(articles.ArticlesClass().Class).Permission(),
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(articles.ArticlesClass().Class).Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
	})

	t.Run("correctly succeed to perform a gRPC Search call with permissions", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: articles.ArticlesClass().Class,
		})
		t.Log(err)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 1)
	})
}
