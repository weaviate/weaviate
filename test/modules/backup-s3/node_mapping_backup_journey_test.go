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
)

func Test_NodeMappingBackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)
		t.Setenv(envS3Bucket, s3BackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendS3(s3BackupJourneyBucketName).
			WithText2VecContextionary().
			WithWeaviate().
			WithSecondWeaviate().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("post-instance env setup", func(t *testing.T) {
			createBucket(ctx, t, compose.GetMinIO().URI(), s3BackupJourneyRegion, s3BackupJourneyBucketName)
			helper.SetupClient(compose.GetWeaviate().URI())
		})

		t.Run("backup-s3", func(t *testing.T) {
			journey.NodeMappingBackupJourneyTests_SingleNode_Backup(t, compose.GetWeaviate().URI(),
				"s3", s3BackupJourneyClassName, s3BackupJourneyBackupIDSingleNode, nil)
		})

		// Now change our tests to use the second cluster and trigger a backup restore
		helper.SetupClient(compose.GetSecondWeaviate().URI())

		t.Run("restore-s3", func(t *testing.T) {
			journey.NodeMappingBackupJourneyTests_SingleNode_Restore(t, compose.GetSecondWeaviate().URI(),
				"s3", s3BackupJourneyClassName, s3BackupJourneyBackupIDSingleNode, nil, map[string]string{
					docker.Weaviate: docker.SecondWeaviate,
				})
		})
	})
}
