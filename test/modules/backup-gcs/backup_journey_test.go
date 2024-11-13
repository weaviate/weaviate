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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	envGCSEndpoint            = "GCS_ENDPOINT"
	envGCSStorageEmulatorHost = "STORAGE_EMULATOR_HOST"
	envGCSCredentials         = "GOOGLE_APPLICATION_CREDENTIALS"
	envGCSProjectID           = "GOOGLE_CLOUD_PROJECT"
	envGCSBucket              = "BACKUP_GCS_BUCKET"
	envGCSUseAuth             = "BACKUP_GCS_USE_AUTH"

	gcsBackupJourneyClassName          = "GcsBackup"
	gcsBackupJourneyBackupIDSingleNode = "gcs_backup_single_node"
	gcsBackupJourneyBackupIDCluster    = "gcs_backup_cluster"
	gcsBackupJourneyProjectID          = "gcs_backup_journey"
)

func Test_BackupJourney(t *testing.T) {
	tests := []struct {
		name           string
		overrideBucket bool
		bucket         string
		bucketOverride string
		pathOverride   string
	}{
		{
			name:           "default backup",
			overrideBucket: false,
			bucket:         "backups",
			bucketOverride: "",
			pathOverride:   "",
		},
		{
			name:           "with override bucket and path",
			overrideBucket: true,
			bucket:         "backups",
			bucketOverride: "gcsbjtestbucketoverride",
			pathOverride:   "gcsbjtstBuckPathOveride",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBackupJourney(t, ctx, tt.overrideBucket, tt.bucket, tt.bucketOverride, tt.pathOverride)
		})
	}
}

func runBackupJourney(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	gcsBackupJourneyBucketName := containerName

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
		moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, "gcsbjtestbucketoverride")

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"gcs", gcsBackupJourneyClassName, gcsBackupJourneyBackupIDSingleNode+overrideBucket, nil, override, overrideBucket, overridePath)
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
			WithWeaviateCluster(3).
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
		moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, "gcsbjtestbucketoverride")
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "gcs", gcsBackupJourneyClassName,
				gcsBackupJourneyBackupIDCluster+overrideBucket, nil, override, overrideBucket, overridePath, compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
