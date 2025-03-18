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

func TestAuthzGRPCSearch(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
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

			t.Run("correctly fail to perform a gRPC Search call without permissions", func(t *testing.T) {
				ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
				_, err := grpcClient.Search(ctx, &protocol.SearchRequest{
					Collection: articles.ArticlesClass().Class,
					Tenant:     tt.tenantName,
				})
				require.NotNil(t, err)
				require.Equal(t, status.Code(err), codes.PermissionDenied)
			})

			t.Run("create role with necessary permissions on articles and assign to custom user", func(t *testing.T) {
				roleName := fmt.Sprintf("role-%v", tt.mtEnabled)
				helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
					helper.NewDataPermission().
						WithAction(authorization.ReadData).
						WithCollection(articles.ArticlesClass().Class).
						WithTenant(tt.tenantPermission).
						Permission(),
				}})
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			})

			t.Run("correctly succeed to perform a gRPC Search call with permissions", func(t *testing.T) {
				ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
				resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
					Collection: articles.ArticlesClass().Class,
					Tenant:     tt.tenantName,
				})
				if err != nil {
					t.Logf("Error: %+v", err)
				}
				require.Nil(t, err)
				require.NotNil(t, resp)
				require.Len(t, resp.Results, 1)
			})
		})
	}
}

func TestAuthzGRPCSearchWithMT(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"
	roleName := "role"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	grpcClient := helper.ClientGRPC(t)

	defer helper.DeleteClassAuth(t, articles.ArticlesClass().Class, adminKey)

	cls := articles.ArticlesClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	tenant1 := "tenant1"
	tenant2 := "tenant2"

	helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
	helper.CreateClassAuth(t, cls, adminKey)
	for _, tenant := range []string{tenant1, tenant2} {
		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: tenant}}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("A Treatise on the Astrolabe").WithTenant(tenant).Object()}, adminKey)
	}

	helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
		helper.NewDataPermission().
			WithAction(authorization.ReadData).
			WithCollection(articles.ArticlesClass().Class).
			WithTenant(tenant1).
			Permission(),
	}})
	helper.AssignRoleToUser(t, adminKey, roleName, customUser)

	t.Run(fmt.Sprintf("correctly fail to perform a gRPC Search call on %s", tenant2), func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		_, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: articles.ArticlesClass().Class,
			Tenant:     tenant2,
		})
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.PermissionDenied)
	})

	t.Run(fmt.Sprintf("correctly succeed to perform a gRPC Search call on %s", tenant1), func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: articles.ArticlesClass().Class,
			Tenant:     tenant1,
		})
		if err != nil {
			t.Logf("Error: %+v", err)
		}
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 1)
	})
}
