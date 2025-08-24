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

	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
)

const (
	envMinioEndpoint = "MINIO_ENDPOINT"
	envAwsRegion     = "AWS_REGION"
	envS3AccessKey   = "AWS_ACCESS_KEY_ID"
	envS3SecretKey   = "AWS_SECRET_KEY"
	envS3Bucket      = "BACKUP_S3_BUCKET"
	envS3Endpoint    = "BACKUP_S3_ENDPOINT"
	envS3UseSSL      = "BACKUP_S3_USE_SSL"

	s3BackupJourneyClassName          = "S3Backup"
	s3BackupJourneyBackupIDSingleNode = "s3-backup-single-node"
	s3BackupJourneyBackupIDCluster    = "s3-backup-cluster"
	s3BackupJourneyRegion             = "eu-west-1"
	s3BackupJourneyAccessKey          = "aws_access_key"
	s3BackupJourneySecretKey          = "aws_secret_key"
)

func Test_BackupJourney(t *testing.T) {
	ctx := context.Background()

	runBackupJourney(t, ctx, false, "backups", "", "")
	t.Run("with override bucket and path", func(t *testing.T) {
		runBackupJourney(t, ctx, true, "testbucketoverride", "testbucketoverride", "testBucketPathOverride")
	})
}

func runBackupJourney(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	s3BackupJourneyBucketName := containerName
	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)
		t.Setenv(envS3Bucket, s3BackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendS3(s3BackupJourneyBucketName, s3BackupJourneyRegion).
			WithText2VecContextionary().
			WithWeaviateEnv("ENABLE_CLEANUP_UNFINISHED_BACKUPS", "true").
			WithWeaviate().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("post-instance env setup", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
		})

		t.Run("backup-s3", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"s3", s3BackupJourneyClassName, s3BackupJourneyBackupIDSingleNode, nil, override, overrideBucket, overridePath)
		})

		t.Run("cancel after restart", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
			journey.CancelFromRestartJourney(t, compose, compose.GetWeaviate().Name(), modstgs3.Name)
		})
	})

	t.Run("multiple node", func(t *testing.T) {
		ctx := context.Background()

		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)
		t.Setenv(envS3Bucket, s3BackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendS3(s3BackupJourneyBucketName, s3BackupJourneyRegion).
			WithText2VecContextionary().
			WithWeaviateCluster(3).
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("post-instance env setup", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
		})

		t.Run("backup-s3", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "s3", s3BackupJourneyClassName,
				s3BackupJourneyBackupIDCluster, nil, override, overrideBucket, overridePath,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
