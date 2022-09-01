//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/journey"
	"github.com/stretchr/testify/require"
)

const (
	weaviateEndpoint = "WEAVIATE_ENDPOINT"
	envMinioEndpoint = "MINIO_ENDPOINT"
	envAwsRegion     = "AWS_REGION"
	envS3AccessKey   = "AWS_ACCESS_KEY_ID"
	envS3SecretKey   = "AWS_SECRET_KEY"
	envS3Bucket      = "STORAGE_S3_BUCKET"

	s3BackupJourneyClassName  = "S3Backup"
	s3BackupJourneySnapshotID = "s3-snapshot"
	s3BackupJourneyBucketName = "snapshots"
	s3BackupJourneyRegion     = "eu-west-1"
	s3BackupJourneyAccessKey  = "aws_access_key"
	s3BackupJourneySecretKey  = "aws_secret_key"
)

func Test_BackupJourney(t *testing.T) {
	t.Skip("to be enabled after finishing WEAVIATE-278")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("pre-instance env setup", func(t *testing.T) {
		require.Nil(t, os.Setenv(envS3AccessKey, s3BackupJourneyAccessKey))
		require.Nil(t, os.Setenv(envS3SecretKey, s3BackupJourneySecretKey))
		require.Nil(t, os.Setenv(envS3Bucket, s3BackupJourneyBucketName))
	})

	compose, err := docker.New().
		WithStorageAWSS3(s3BackupJourneyBucketName).
		WithText2VecContextionary().
		WithWeaviate().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	}()

	t.Run("post-instance env setup", func(t *testing.T) {
		createBucket(ctx, t, compose.GetMinIO().URI(), s3BackupJourneyRegion, s3BackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	// journey tests
	t.Run("storage-aws-s3", func(t *testing.T) {
		journey.BackupJourneyTests(t, os.Getenv(weaviateEndpoint),
			"s3", s3BackupJourneyClassName, s3BackupJourneySnapshotID)
	})
}
