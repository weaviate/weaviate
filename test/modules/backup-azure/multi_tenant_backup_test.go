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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envAzureContainer, azureBackupJourneyContainerName)

		compose, err := docker.New().
			WithBackendAzure(azureBackupJourneyContainerName).
			WithText2VecContextionary().
			WithWeaviate().
			Start(ctx)
		require.Nil(t, err)

		t.Log("post-instance env setup")
		azuriteEndpoint := compose.GetAzurite().URI()
		t.Setenv(envAzureEndpoint, azuriteEndpoint)
		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"azure", azureBackupJourneyClassName, azureBackupJourneyBackupIDSingleNode, tenantNames)
		})

		require.Nil(t, compose.Terminate(ctx))
	})

	t.Run("multiple node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envAzureContainer, azureBackupJourneyContainerName)

		compose, err := docker.New().
			WithBackendAzure(azureBackupJourneyContainerName).
			WithText2VecContextionary().
			WithWeaviate().
			WithWeaviateClusterSize(2).
			Start(ctx)
		require.Nil(t, err)

		t.Log("post-instance env setup")
		azuriteEndpoint := compose.GetAzurite().URI()
		t.Setenv(envAzureEndpoint, azuriteEndpoint)
		moduleshelper.CreateAzureContainer(ctx, t, azuriteEndpoint, azureBackupJourneyContainerName)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-azure", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "azure", azureBackupJourneyClassName,
				azureBackupJourneyBackupIDCluster, tenantNames, compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})

		require.Nil(t, compose.Terminate(ctx))
	})
}
