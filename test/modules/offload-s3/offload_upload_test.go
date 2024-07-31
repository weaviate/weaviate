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
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_UploadS3Journey(t *testing.T) {
	t.Run("happy path with RF 2 upload to s3 provider", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("offloading", "us-west-1").
			WithText2VecContextionary().
			With3NodeCluster().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())

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

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				if tn.Name == tenantNames[0] {
					require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
					break
				}
			}
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			_, err = helper.TenantObject(t, tenantObjects[0].Class, tenantObjects[0].ID, tenantNames[0])
			require.NotNil(t, err)
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Payload {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
		})

		t.Run("add objects to frozen tenant shall fail", func(t *testing.T) {
			t.Run("add frozen tenant objects", func(t *testing.T) {
				for _, obj := range tenantObjects {
					obj.Tenant = tenantNames[0]
					params := objects.NewObjectsCreateParams().WithBody(obj)
					_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
					require.NotNil(t, err)
				}
			})

			tenantRefs := []*models.Object{
				{
					ID:    "169b62a7-ef1c-481d-8fb0-27f11716bde7",
					Class: className,
					Properties: map[string]interface{}{
						"name":        tenantNames[0],
						"mutableProp": "ref#0",
					},
					Tenant: tenantNames[0],
				},
			}

			t.Run("add frozen tenant object references", func(t *testing.T) {
				for i, obj := range tenantObjects {
					ref := &models.SingleRef{Beacon: helper.NewBeacon(className, tenantRefs[0].ID)}
					params := objects.NewObjectsClassReferencesCreateParams().
						WithClassName(className).WithID(obj.ID).WithBody(ref).WithPropertyName("refProp").WithTenant(&tenantNames[i])
					_, err := helper.Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
					require.NotNil(t, err)
				}
			})
		})
	})

	t.Run("node is down while RF is 3, one weaviate node is down", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("weaviate-offload", "us-west-1").
			WithText2VecContextionary().
			With3NodeCluster().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())

		className := "MultiTenantClass"
		testClass := models.Class{
			Class:             className,
			ReplicationConfig: &models.ReplicationConfig{Factor: 3},
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

		t.Run("StopNode-2", func(t *testing.T) {
			require.Nil(t, compose.Stop(ctx, compose.GetWeaviateNode2().Name(), nil))
		})

		t.Run("updating tenant status", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenantNames[0],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				},
			})
		})

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)

			for _, tn := range resp.Payload {
				if tn.Name == tenantNames[0] {
					require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
					break
				}
			}
		})

		t.Run("StartNode-2", func(t *testing.T) {
			require.Nil(t, compose.Start(ctx, compose.GetWeaviateNode2().Name()))
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			_, err = helper.TenantObject(t, tenantObjects[0].Class, tenantObjects[0].ID, tenantNames[0])
			require.NotNil(t, err)
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Payload {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
		})
	})

	t.Run("unhappy path with RF 3, cloud provider is down", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("offloading", "us-west-1").
			WithWeaviateEnv("OFFLOAD_TIMEOUT", "2").
			WithText2VecContextionary().
			With3NodeCluster().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())

		className := "MultiTenantClass"
		testClass := models.Class{
			Class:             className,
			ReplicationConfig: &models.ReplicationConfig{Factor: 3},
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

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)

			for _, tn := range resp.Payload {
				if tn.Name == tenantNames[0] {
					require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
					break
				}
			}
		})

		t.Run("terminate Minio", func(t *testing.T) {
			require.Nil(t, compose.TerminateContainer(ctx, docker.MinIO))
		})

		t.Run("verify tenant status HOT", func(xt *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(xt, className)
				require.Nil(t, err)

				for _, tn := range resp.Payload {
					if tn.Name == tenantNames[0] {
						assert.Equal(at, models.TenantActivityStatusHOT, tn.ActivityStatus)
						break
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusHOT))
		})
	})
}
