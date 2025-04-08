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

package test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestGRPCTenantsGet(t *testing.T) {
	grpcClient, _ := newClient(t)

	className := "GRPCTenantsGet"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	helper.CreateClass(t, &testClass)
	tenants := make([]*models.Tenant, len(tenantNames))
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenantNames[i], ActivityStatus: "HOT"}
	}
	helper.CreateTenants(t, className, tenants)

	t.Run("Gets tenants of a class", func(t *testing.T) {
		resp, err := grpcClient.TenantsGet(context.TODO(), &pb.TenantsGetRequest{
			Collection: className,
		})
		if err != nil {
			t.Fatalf("error while getting tenants: %v", err)
		}
		for _, tenant := range resp.Tenants {
			require.Equal(t, slices.Contains(tenantNames, tenant.Name), true)
			require.Equal(t, tenant.ActivityStatus, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT)
		}
	})

	t.Run("Gets two tenants by their names", func(t *testing.T) {
		resp, err := grpcClient.TenantsGet(context.TODO(), &pb.TenantsGetRequest{
			Collection: className,
			Params: &pb.TenantsGetRequest_Names{
				Names: &pb.TenantNames{
					Values: []string{tenantNames[0], tenantNames[2]},
				},
			},
		})
		if err != nil {
			t.Fatalf("error while getting tenants: %v", err)
		}
		require.Equal(t, resp.Tenants, []*pb.Tenant{{
			Name:           tenantNames[0],
			ActivityStatus: pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT,
		}, {
			Name:           tenantNames[2],
			ActivityStatus: pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT,
		}})
	})

	t.Run("Returns error when tenant names are missing", func(t *testing.T) {
		_, err := grpcClient.TenantsGet(context.TODO(), &pb.TenantsGetRequest{
			Collection: className,
			Params:     &pb.TenantsGetRequest_Names{},
		})
		require.NotNil(t, err)
	})

	t.Run("Returns error when tenant names are specified empty", func(t *testing.T) {
		_, err := grpcClient.TenantsGet(context.TODO(), &pb.TenantsGetRequest{
			Collection: className,
			Params: &pb.TenantsGetRequest_Names{
				Names: &pb.TenantNames{
					Values: []string{},
				},
			},
		})
		require.NotNil(t, err)
	})

	t.Run("Returns nothing when tenant names are not found", func(t *testing.T) {
		resp, err := grpcClient.TenantsGet(context.TODO(), &pb.TenantsGetRequest{
			Collection: className,
			Params: &pb.TenantsGetRequest_Names{
				Names: &pb.TenantNames{
					Values: []string{"NonExistentTenant"},
				},
			},
		})
		require.Nil(t, err)
		require.Empty(t, resp.Tenants)
	})
}
