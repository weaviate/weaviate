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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_DownloadS3Journey(t *testing.T) {
	t.Run("happy path with RF 2 download from s3 provider, HOT status", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("offloading", "us-west-1").
			WithText2VecContextionary().
			WithWeaviateClusterWithGRPC().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

		className := "MultiTenantClass"
		testClass := models.Class{
			Class:             className,
			ReplicationConfig: &models.ReplicationConfig{Factor: 2},
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
		t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
			helper.CreateClass(t, &testClass)
		})

		tenantNames := []string{
			"Tenant1", "Tenant2", "Tenant3",
		}
		t.Run("create tenants", func(t *testing.T) {
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenants {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
		})

		tenantObjects := []*models.Object{
			{
				ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[0],
				},
				Tenant: tenantNames[0],
			},
			{
				ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[1],
				},
				Tenant: tenantNames[1],
			},
			{
				ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[2],
				},
				Tenant: tenantNames[2],
			},
		}

		defer func() {
			helper.DeleteClass(t, className)
		}()

		t.Run("add tenant objects", func(t *testing.T) {
			for _, obj := range tenantObjects {
				assert.Nil(t, helper.CreateObject(t, obj))
			}
		})

		t.Run("verify object creation", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				assert.Equal(t, obj.Class, resp.Class)
				assert.Equal(t, obj.Properties, resp.Properties)
			}
		})

		t.Run("updating tenant status", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				},
			})
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenantsGRPC(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Tenants {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_FROZEN, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_FROZEN))
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			_, err = helper.TenantObject(t, tenantObjects[0].Class, tenantObjects[0].ID, tenantNames[0])
			require.NotNil(t, err)
		})

		t.Run("updating tenant status to HOT", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusHOT,
				},
			})
		})

		t.Run("verify tenant status HOT", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenantsGRPC(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Tenants {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT))
		})

		t.Run("verify object creation", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				assert.Equal(t, obj.Class, resp.Class)
				assert.Equal(t, obj.Properties, resp.Properties)
			}
		})
	})

	t.Run("happy path with RF 2 download from s3 provider COLD status", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("offloading", "us-west-1").
			WithText2VecContextionary().
			WithWeaviateClusterWithGRPC().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

		className := "MultiTenantClass"
		testClass := models.Class{
			Class:             className,
			ReplicationConfig: &models.ReplicationConfig{Factor: 2},
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
		t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
			helper.CreateClass(t, &testClass)
		})

		tenantNames := []string{
			"Tenant1", "Tenant2", "Tenant3",
		}
		t.Run("create tenants", func(t *testing.T) {
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenants {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
		})

		tenantObjects := []*models.Object{
			{
				ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[0],
				},
				Tenant: tenantNames[0],
			},
			{
				ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[1],
				},
				Tenant: tenantNames[1],
			},
			{
				ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[2],
				},
				Tenant: tenantNames[2],
			},
		}

		defer func() {
			helper.DeleteClass(t, className)
		}()

		t.Run("add tenant objects", func(t *testing.T) {
			for _, obj := range tenantObjects {
				assert.Nil(t, helper.CreateObject(t, obj))
			}
		})

		t.Run("verify object creation", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				assert.Equal(t, obj.Class, resp.Class)
				assert.Equal(t, obj.Properties, resp.Properties)
			}
		})

		t.Run("updating tenant status to COLD", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusCOLD,
				},
			})
		})

		t.Run("verify tenant status COLD", func(t *testing.T) {
			resp, err := helper.GetTenantsGRPC(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Tenants {
				if tn.Name == tenantNames[0] {
					require.Equal(t, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_COLD, tn.ActivityStatus)
					break
				}
			}
		})

		t.Run("updating tenant status", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				},
			})
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenantsGRPC(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Tenants {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_FROZEN, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_FROZEN))
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			_, err = helper.TenantObject(t, tenantObjects[0].Class, tenantObjects[0].ID, tenantNames[0])
			require.NotNil(t, err)
		})

		t.Run("updating tenant status to COLD", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusCOLD,
				},
			})
		})

		t.Run("verify tenant status COLD", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenantsGRPC(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Tenants {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_COLD, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", pb.TenantActivityStatus_TENANT_ACTIVITY_STATUS_COLD))
		})
	})
}
