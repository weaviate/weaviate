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

func TestAuthzAggregateWithGRPC(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	customUser2 := "custom-user2"
	customKey2 := "custom-key2"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey, customUser2: customKey2}, nil)
	defer down()

	grpcClient := helper.ClientGRPC(t)

	tests := []struct {
		name             string
		mtEnabled        bool
		tenantName       string
		tenantPermission string
	}{
		{
			name:             "with multi-tenancy",
			mtEnabled:        true,
			tenantName:       "tenant1",
			tenantPermission: "tenant1",
		},
		{
			name:             "without multi-tenancy",
			mtEnabled:        false,
			tenantName:       "",
			tenantPermission: "*",
		},
	}

	helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer helper.DeleteClassAuth(t, articles.ArticlesClass().Class, adminKey)

			cls := articles.ArticlesClass()
			if tt.mtEnabled {
				cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
			}
			helper.CreateClassAuth(t, cls, adminKey)
			if tt.mtEnabled {
				helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: tt.tenantName}}, adminKey)
			}
			helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("How to git gud").WithTenant(tt.tenantName).Object()}, adminKey)

			roleName := fmt.Sprintf("role-%v", tt.mtEnabled)
			helper.DeleteClassAuth(t, roleName, adminKey)
			defer helper.DeleteClassAuth(t, roleName, adminKey)
			helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
				helper.NewDataPermission().
					WithAction(authorization.ReadData).
					WithCollection(articles.ArticlesClass().Class).
					WithTenant(tt.tenantPermission).
					Permission(),
			}})

			t.Run("correctly fail to perform a gRPC Search call without permissions", func(t *testing.T) {
				ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
				_, err := grpcClient.Aggregate(ctx, &protocol.AggregateRequest{
					Collection:   articles.ArticlesClass().Class,
					Tenant:       tt.tenantName,
					ObjectsCount: true,
				})
				require.NotNil(t, err)
				require.Equal(t, status.Code(err), codes.PermissionDenied)
			})

			t.Run("correctly succeed to perform a gRPC Search call with permissions", func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
				resp, err := grpcClient.Aggregate(ctx, &protocol.AggregateRequest{
					Collection:   articles.ArticlesClass().Class,
					Tenant:       tt.tenantName,
					ObjectsCount: true,
				})
				if err != nil {
					t.Logf("Error: %+v", err)
				}
				require.Nil(t, err)
				require.NotNil(t, resp)
				require.Equal(t, *resp.GetSingleResult().ObjectsCount, int64(1))
			})
		})
	}

	t.Run("with multi-tenancy and only partial permissions", func(t *testing.T) {
		cls := articles.ArticlesClass()
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		helper.CreateClassAuth(t, cls, adminKey)
		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("How to git gud").WithTenant("tenant1").Object()}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("How to git gud").WithTenant("tenant2").Object()}, adminKey)

		// role with permission for tenant1 but not 2
		roleName := "role-tenant-1"
		helper.DeleteRole(t, adminKey, roleName)
		defer helper.DeleteRole(t, adminKey, roleName)

		helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
			helper.NewDataPermission().
				WithAction(authorization.ReadData).
				WithCollection(articles.ArticlesClass().Class).
				WithTenant("tenant1").
				Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser2)

		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey2))
		resp, err := grpcClient.Aggregate(ctx, &protocol.AggregateRequest{
			Collection:   articles.ArticlesClass().Class,
			Tenant:       "tenant1",
			ObjectsCount: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, *resp.GetSingleResult().ObjectsCount, int64(1))

		_, err = grpcClient.Aggregate(ctx, &protocol.AggregateRequest{
			Collection:   articles.ArticlesClass().Class,
			Tenant:       "tenant2",
			ObjectsCount: true,
		})
		require.Error(t, err)
		require.Equal(t, status.Code(err), codes.PermissionDenied)
	})
}
