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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	numTenants = 50
)

func Test_MultiTenantBackupJourney(t *testing.T) {
	ctx := context.Background()

	multiTenantBackupJourneyStart(t, ctx, false, "backups", "", "")
	t.Run("with override bucket and path", func(t *testing.T) {
		multiTenantBackupJourneyStart(t, ctx, true, "testbucketoverride", "testbucketoverride", "testBucketPathOverride")
	})
}

func multiTenantBackupJourneyStart(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	azureBackupJourneyBackupIDCluster := "azure-backup-cluster-multi-tenant"
	azureBackupJourneyBackupIDSingleNode := "azure-backup-single-node-multi-tenant"
	if override {
		azureBackupJourneyBackupIDCluster = "azure-backup-cluster-multi-tenant-override"
		azureBackupJourneyBackupIDSingleNode = "azure-backup-single-node-multi-tenant-override"
	}

	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("single node", func(t *testing.T) {
		ctx := context.Background()
		t.Log("pre-instance env setup")
		containerToUse := containerName
		if override {
			containerToUse = overrideBucket
		}
		t.Setenv(envAzureContainer, containerToUse)

		compose, err := docker.New().
			WithBackendAzure(containerToUse).
			WithText2VecContextionary().
			WithWeaviate().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, compose.Terminate(ctx))
		}()

		t.Log("post-instance env setup")
		azuriteEndpoint := compose.GetAzurite().URI()
		t.Setenv(envAzureEndpoint, azuriteEndpoint)
		t.Setenv(envAzureStorageConnectionString, fmt.Sprintf(connectionString, azuriteEndpoint))

		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, containerToUse)
		defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, containerToUse)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"azure", azureBackupJourneyClassName, azureBackupJourneyBackupIDSingleNode, tenantNames, override, overrideBucket, overridePath)
		})
	})

	t.Run("multiple node", func(t *testing.T) {
		ctx := context.Background()
		t.Log("pre-instance env setup")
		containerToUse := containerName
		if override {
			containerToUse = overrideBucket
		}
		t.Setenv(envAzureContainer, containerToUse)

		compose, err := docker.New().
			WithBackendAzure(containerToUse).
			WithText2VecContextionary().
			WithWeaviateCluster(3).
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, compose.Terminate(ctx))
		}()

		t.Log("post-instance env setup")
		azuriteEndpoint := compose.GetAzurite().URI()
		t.Setenv(envAzureEndpoint, azuriteEndpoint)
		t.Setenv(envAzureStorageConnectionString, fmt.Sprintf(connectionString, azuriteEndpoint))

		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, containerToUse)
		defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, containerToUse)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "azure", azureBackupJourneyClassName,
				azureBackupJourneyBackupIDCluster, tenantNames, override, overrideBucket, overridePath, compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
