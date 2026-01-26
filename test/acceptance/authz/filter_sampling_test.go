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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthzFilterSamplingWithGRPC(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	customUser2 := "custom-user2"
	customKey2 := "custom-key2"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey, customUser2: customKey2}, nil)
	defer down()

	className := "FilterSamplingAuthzTest"

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:            "category",
						DataType:        []string{"text"},
						Tokenization:    "field",
						IndexFilterable: func() *bool { b := true; return &b }(),
					},
				},
				Vectorizer: "none",
			}
			if tt.mtEnabled {
				cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
			}
			helper.CreateClassAuth(t, cls, adminKey)
			defer helper.DeleteClassAuth(t, className, adminKey)

			if tt.mtEnabled {
				helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: tt.tenantName}}, adminKey)
			}

			// Create objects with category property
			objects := []*models.Object{
				{Class: className, Tenant: tt.tenantName, Properties: map[string]interface{}{"category": "electronics"}},
				{Class: className, Tenant: tt.tenantName, Properties: map[string]interface{}{"category": "electronics"}},
				{Class: className, Tenant: tt.tenantName, Properties: map[string]interface{}{"category": "books"}},
			}
			helper.CreateObjectsBatchAuth(t, objects, adminKey)

			roleName := fmt.Sprintf("filter-sampling-role-%v", tt.mtEnabled)
			helper.DeleteRole(t, adminKey, roleName)
			defer helper.DeleteRole(t, adminKey, roleName)

			helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
				helper.NewDataPermission().
					WithAction(authorization.ReadData).
					WithCollection(className).
					WithTenant(tt.tenantPermission).
					Permission(),
			}})

			t.Run("fail to perform FilterSampling without permissions", func(t *testing.T) {
				_, err := filterSamplingGRPC(t, context.Background(), className, "category", customKey, tt.tenantName)
				require.NotNil(t, err)
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			})

			t.Run("succeed with only ReadData permission (gRPC doesn't require ReadCollections)", func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				resp, err := filterSamplingGRPC(t, context.Background(), className, "category", customKey, tt.tenantName)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, uint64(3), resp.TotalObjects)
				require.Len(t, resp.Samples, 2) // electronics and books
			})
		})
	}

	t.Run("multi-tenant isolation", func(t *testing.T) {
		cls := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:            "category",
					DataType:        []string{"text"},
					Tokenization:    "field",
					IndexFilterable: func() *bool { b := true; return &b }(),
				},
			},
			Vectorizer:         "none",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		helper.CreateClassAuth(t, cls, adminKey)
		defer helper.DeleteClassAuth(t, className, adminKey)

		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{Class: className, Tenant: "tenant1", Properties: map[string]interface{}{"category": "tenant1-cat"}},
		}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{Class: className, Tenant: "tenant2", Properties: map[string]interface{}{"category": "tenant2-cat"}},
		}, adminKey)

		// Role with permission for tenant1 only
		roleName := "filter-sampling-tenant1-only"
		helper.DeleteRole(t, adminKey, roleName)
		defer helper.DeleteRole(t, adminKey, roleName)

		helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
			helper.NewDataPermission().
				WithAction(authorization.ReadData).
				WithCollection(className).
				WithTenant("tenant1").
				Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser2)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser2)

		t.Run("user can access tenant1", func(t *testing.T) {
			resp, err := filterSamplingGRPC(t, context.Background(), className, "category", customKey2, "tenant1")
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, uint64(1), resp.TotalObjects)
		})

		t.Run("user cannot access tenant2", func(t *testing.T) {
			_, err := filterSamplingGRPC(t, context.Background(), className, "category", customKey2, "tenant2")
			require.Error(t, err)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	})

	t.Run("wildcard tenant permission allows access to all tenants", func(t *testing.T) {
		cls := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:            "category",
					DataType:        []string{"text"},
					Tokenization:    "field",
					IndexFilterable: func() *bool { b := true; return &b }(),
				},
			},
			Vectorizer:         "none",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		helper.CreateClassAuth(t, cls, adminKey)
		defer helper.DeleteClassAuth(t, className, adminKey)

		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: "tenantA"}, {Name: "tenantB"}}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{Class: className, Tenant: "tenantA", Properties: map[string]interface{}{"category": "catA"}},
		}, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{Class: className, Tenant: "tenantB", Properties: map[string]interface{}{"category": "catB"}},
		}, adminKey)

		// Role with wildcard tenant permission
		roleName := "filter-sampling-wildcard-tenant"
		helper.DeleteRole(t, adminKey, roleName)
		defer helper.DeleteRole(t, adminKey, roleName)

		helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
			helper.NewDataPermission().
				WithAction(authorization.ReadData).
				WithCollection(className).
				WithTenant("*").
				Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		// User with wildcard can access all tenants
		for _, tenant := range []string{"tenantA", "tenantB"} {
			resp, err := filterSamplingGRPC(t, context.Background(), className, "category", customKey, tenant)
			require.NoError(t, err, "should access %s", tenant)
			require.NotNil(t, resp)
			require.Equal(t, uint64(1), resp.TotalObjects)
		}
	})
}
