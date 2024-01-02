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

const numTenants = 50

func Test_MultiTenantBackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envGCSCredentials, "")
		t.Setenv(envGCSProjectID, gcsBackupJourneyProjectID)
		t.Setenv(envGCSBucket, gcsBackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendGCS(gcsBackupJourneyBucketName).
			WithText2VecContextionary().
			WithWeaviate().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Log("post-instance env setup")
		t.Setenv(envGCSEndpoint, compose.GetGCS().URI())
		t.Setenv(envGCSStorageEmulatorHost, compose.GetGCS().URI())
		moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"gcs", gcsBackupJourneyClassName,
				gcsBackupJourneyBackupIDSingleNode, tenantNames)
		})
	})

	t.Run("multiple node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envGCSCredentials, "")
		t.Setenv(envGCSProjectID, gcsBackupJourneyProjectID)
		t.Setenv(envGCSBucket, gcsBackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendGCS(gcsBackupJourneyBucketName).
			WithText2VecContextionary().
			WithWeaviateCluster().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Log("post-instance env setup")
		t.Setenv(envGCSEndpoint, compose.GetGCS().URI())
		t.Setenv(envGCSStorageEmulatorHost, compose.GetGCS().URI())
		moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "gcs", gcsBackupJourneyClassName,
				gcsBackupJourneyBackupIDCluster, tenantNames,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode2().URI())
		})
	})
}
