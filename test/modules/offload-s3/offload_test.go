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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	envS3AccessKey           = "AWS_ACCESS_KEY_ID"
	envS3SecretKey           = "AWS_SECRET_KEY"
	s3BackupJourneyAccessKey = "aws_access_key"
	s3BackupJourneySecretKey = "aws_secret_key"
)

func Test_Upload_DownloadS3Journey(t *testing.T) {
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
			tenants := []*models.Tenant{}
			for i := range tenantNames {
				tenants = append(tenants, &models.Tenant{
					Name:           tenantNames[i],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				})
			}

			helper.UpdateTenants(t, className, tenants)
		})

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
						break
					}
				}
			}
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			for i := range tenantObjects {
				_, err = helper.TenantObject(t, tenantObjects[i].Class, tenantObjects[i].ID, tenantNames[i])
				require.NotNil(t, err)
			}
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Payload {
					for i := range tenantNames {
						if tn.Name == tenantNames[i] {
							assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
							break
						}
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
		})

		t.Run("updating tenant status to HOT", func(t *testing.T) {
			tenants := []*models.Tenant{}
			for i := range tenantNames {
				tenants = append(tenants, &models.Tenant{
					Name:           tenantNames[i],
					ActivityStatus: models.TenantActivityStatusHOT,
				})
			}

			helper.UpdateTenants(t, className, tenants)
		})

		t.Run("verify tenant status HOT", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Payload {
					for i := range tenantNames {
						if tn.Name == tenantNames[i] {
							assert.Equal(at, models.TenantActivityStatusHOT, tn.ActivityStatus)
							break
						}
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusHOT))
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
			tenants := []*models.Tenant{}
			for i := range tenantNames {
				tenants = append(tenants, &models.Tenant{
					Name:           tenantNames[i],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				})
			}

			helper.UpdateTenants(t, className, tenants)
		})

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
						break
					}
				}
			}
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			for i := range tenantObjects {
				_, err = helper.TenantObject(t, tenantObjects[i].Class, tenantObjects[i].ID, tenantNames[i])
				require.NotNil(t, err)
			}
		})
	})
}

func Test_AutoTenantActivation(t *testing.T) {
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
			Enabled:              true,
			AutoTenantActivation: true,
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
		tenants := []*models.Tenant{}
		for i := range tenantNames {
			tenants = append(tenants, &models.Tenant{
				Name:           tenantNames[i],
				ActivityStatus: models.TenantActivityStatusFROZEN,
			})
		}

		helper.UpdateTenants(t, className, tenants)
	})

	t.Run("verify tenant status FREEZING", func(t *testing.T) {
		resp, err := helper.GetTenants(t, className)
		require.Nil(t, err)
		for _, tn := range resp.Payload {
			for i := range tenantNames {
				if tn.Name == tenantNames[i] {
					require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
					break
				}
			}
		}
	})

	t.Run("verify tenant status", func(t *testing.T) {
		assert.EventuallyWithT(t, func(at *assert.CollectT) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
						break
					}
				}
			}
		}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
	})

	objectsToCreate := []*models.Object{
		{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[1],
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[2],
			},
			Tenant: tenantNames[2],
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		},
	}

	t.Run("add tenant objects", func(t *testing.T) {
		for i, obj := range objectsToCreate {
			res, err := helper.CreateObjectWithResponse(t, obj)
			if err != nil {
				oe := &objects.ObjectsCreateUnprocessableEntity{}
				as := errors.As(err, &oe)
				require.True(t, as)
				t.Log(oe.Payload.Error[0])
				t.Log(oe.Error())
				t.Log(oe.Payload)
			}
			require.Nil(t, err)

			if obj.ID == "" {
				// Some of the test objects were created without ID
				objectsToCreate[i].ID = res.ID
			}
		}
	})

	t.Run("verify object creation", func(t *testing.T) {
		for _, obj := range objectsToCreate {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
			require.Nil(t, err)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
		}
	})
}

func Test_ConcurrentFreezeUnfreeze(t *testing.T) {
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

	defer func() {
		helper.DeleteClass(t, className)
	}()

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
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[1],
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    strfmt.UUID(uuid.NewString()),
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
		tenants := []*models.Tenant{}
		for i := range tenantNames {
			tenants = append(tenants, &models.Tenant{
				Name:           tenantNames[i],
				ActivityStatus: models.TenantActivityStatusFROZEN,
			})
		}

		for idx := 0; idx < 5; idx++ {
			go helper.UpdateTenants(t, className, tenants)
		}
	})

	t.Run("verify tenant status", func(t *testing.T) {
		assert.EventuallyWithT(t, func(at *assert.CollectT) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
						break
					}
				}
			}
		}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
	})

	t.Run("verify tenant does not exists", func(t *testing.T) {
		for i := range tenantObjects {
			_, err = helper.TenantObject(t, tenantObjects[i].Class, tenantObjects[i].ID, tenantNames[i])
			require.NotNil(t, err)
		}
	})

	t.Run("updating tenant status to HOT", func(t *testing.T) {
		tenants := []*models.Tenant{}
		for i := range tenantNames {
			tenants = append(tenants, &models.Tenant{
				Name:           tenantNames[i],
				ActivityStatus: models.TenantActivityStatusHOT,
			})
		}

		for idx := 0; idx < 5; idx++ {
			go helper.UpdateTenants(t, className, tenants)
		}
	})

	t.Run("verify tenant status HOT", func(t *testing.T) {
		assert.EventuallyWithT(t, func(at *assert.CollectT) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						assert.Equal(at, models.TenantActivityStatusHOT, tn.ActivityStatus)
						break
					}
				}
			}
		}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusHOT))
	})
}
