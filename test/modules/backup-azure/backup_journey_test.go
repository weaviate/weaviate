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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	envAzureEndpoint  = "AZURE_ENDPOINT"
	envAzureContainer = "BACKUP_AZURE_CONTAINER"

	envAzureStorageConnectionString = "AZURE_STORAGE_CONNECTION_STRING"
	connectionString                = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"

	azureBackupJourneyClassName = "AzureBackup"
)

func Test_BackupJourney(t *testing.T) {
	ctx := context.Background()

	backupJourneyStart(t, ctx, false, "backups", "", "")
	t.Run("with override bucket and path", func(t *testing.T) {
		backupJourneyStart(t, ctx, true, "testbucketoverride", "testbucketoverride", "testBucketPathOverride")
	})
}

func backupJourneyStart(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	azureBackupJourneyBackupIDCluster := "azure-backup-cluster"
	azureBackupJourneyBackupIDSingleNode := "azure-backup-single-node"
	if override {
		azureBackupJourneyBackupIDCluster = "azure-backup-cluster-override"
		azureBackupJourneyBackupIDSingleNode = "azure-backup-single-node-override"
	}

	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		azureBackupJourneyContainerName := "azure-single-node"
		t.Setenv(envAzureContainer, azureBackupJourneyContainerName)
		t.Logf("BACKUP_AZURE_CONTAINER old test is set to %s", os.Getenv("BACKUP_AZURE_CONTAINER"))

		compose, err := docker.New().
			WithBackendAzure(azureBackupJourneyContainerName).
			WithText2VecContextionary().
			WithWeaviateEnv("ENABLE_CLEANUP_UNFINISHED_BACKUPS", "true").
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

		// Create the default container
		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)
		defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)

		// If using override, create the override container as well
		if override {
			moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, overrideBucket)
			defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, overrideBucket)
		}

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"azure", azureBackupJourneyClassName, azureBackupJourneyBackupIDSingleNode, nil, override, overrideBucket, overridePath)
		})

		t.Run("cancel after restart", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
			journey.CancelFromRestartJourney(t, compose, compose.GetWeaviate().Name(), modstgazure.Name)
		})
	})

	t.Run("multiple node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		azureBackupJourneyContainerName := "azure-multiple-nodes"
		t.Setenv(envAzureContainer, azureBackupJourneyContainerName)

		compose, err := docker.New().
			WithBackendAzure(azureBackupJourneyContainerName).
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

		// Create the default container
		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)
		defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)

		// If using override, create the override container as well
		if override {
			moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, overrideBucket)
			defer moduleshelper.DeleteAzureContainer(ctx, t, azuriteEndpoint, overrideBucket)
		}

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "azure", azureBackupJourneyClassName,
				azureBackupJourneyBackupIDCluster, nil, override, overrideBucket, overridePath, compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
