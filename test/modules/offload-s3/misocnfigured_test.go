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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_DeleteTenantsWhileMisconfigured(t *testing.T) {
	t.Run("misconfigure offload, create tenant, delete tenant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, "the_wrong_Key")
		t.Setenv(envS3SecretKey, "the_wrong_Key")

		compose, err := docker.New().
			WithOffloadS3("not-existing-bucket", "us-west-1").
			WithWeaviateEnv("OFFLOAD_TIMEOUT", "120").
			WithWeaviateEnv("OFFLOAD_S3_ENDPOINT", "http://test-minio:0000"). // wrong port configured
			WithoutWeaviateEnvs("OFFLOAD_S3_BUCKET_AUTO_CREATE").
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
				tenants[i] = &models.Tenant{Name: tenantNames[i], ActivityStatus: models.TenantActivityStatusACTIVE}
			}
			helper.CreateTenants(t, className, tenants)
		})

		t.Run("delete tenant 1 in time manner", func(t *testing.T) {
			customCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			errChan := make(chan error, 1)
			go func() {
				errChan <- helper.DeleteTenantsWithContext(t, customCtx, className, []string{tenantNames[0]})
			}()

			select {
			case err := <-errChan:
				require.NoError(t, err, "got error while expecting none")

			case <-customCtx.Done():
				require.FailNowf(t, "operation took longer than timeout: %v", customCtx.Err().Error())
			}
		})

		t.Run("get tenants after deletion", func(t *testing.T) {
			response, err := helper.GetTenants(t, className)
			require.NoError(t, err)
			returnedTenantsNames := []string{}
			for _, tn := range response.Payload {
				returnedTenantsNames = append(returnedTenantsNames, tn.Name)
			}

			slices.Sort(tenantNames[1:])
			slices.Sort(returnedTenantsNames)
			require.Equal(t, tenantNames[1:], returnedTenantsNames, "expected tenant to be deleted but it's not")
		})
	})
}

func Test_DeleteTenantsWhileProviderIsDown(t *testing.T) {
	t.Run("offload, create tenant, minio down, delete tenant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		compose, err := docker.New().
			WithOffloadS3("not-existing-bucket", "us-west-1").
			WithWeaviateEnv("OFFLOAD_TIMEOUT", "120").
			WithoutWeaviateEnvs("OFFLOAD_S3_BUCKET_AUTO_CREATE").
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
				tenants[i] = &models.Tenant{Name: tenantNames[i], ActivityStatus: models.TenantActivityStatusACTIVE}
			}
			helper.CreateTenants(t, className, tenants)
		})
		t.Run("stop minio", func(t *testing.T) {
			require.NoError(t, compose.StopMinIO(ctx))
		})

		t.Run("delete tenant 1 in time manner", func(t *testing.T) {
			customCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			errChan := make(chan error, 1)
			go func() {
				errChan <- helper.DeleteTenantsWithContext(t, customCtx, className, []string{tenantNames[0]})
			}()

			select {
			case err := <-errChan:
				require.NoError(t, err, "got error while expecting none")

			case <-customCtx.Done():
				require.FailNowf(t, "operation took longer than timeout: %v", customCtx.Err().Error())
			}
		})

		t.Run("get tenants after deletion", func(t *testing.T) {
			response, err := helper.GetTenants(t, className)
			require.NoError(t, err)
			returnedTenantsNames := []string{}
			for _, tn := range response.Payload {
				returnedTenantsNames = append(returnedTenantsNames, tn.Name)
			}

			slices.Sort(tenantNames[1:])
			slices.Sort(returnedTenantsNames)
			require.Equal(t, tenantNames[1:], returnedTenantsNames, "expected tenant to be deleted but it's not")
		})
	})
}
