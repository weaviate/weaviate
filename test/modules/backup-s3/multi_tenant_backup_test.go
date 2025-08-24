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
)

const numTenants = 50

func Test_MultiTenantBackupJourney(t *testing.T) {
	ctx := context.Background()

	multiTenantBackupJourneyStart(t, ctx, false, "backups", "", "")
	t.Run("with override bucket and path", func(t *testing.T) {
		multiTenantBackupJourneyStart(t, ctx, true, "testbucketoverride", "testbucketoverride", "testBucketPathOverride")
	})
}

func multiTenantBackupJourneyStart(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	s3BackupJourneyBucketName := containerName

	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("single node", func(t *testing.T) {
		ctx := context.Background()

		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)
		t.Setenv(envS3Bucket, s3BackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendS3(s3BackupJourneyBucketName, s3BackupJourneyRegion).
			WithText2VecContextionary().
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
				"s3", s3BackupJourneyClassName, s3BackupJourneyBackupIDSingleNode, tenantNames, override, overrideBucket, overridePath)
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
				s3BackupJourneyBackupIDCluster, tenantNames, override, overrideBucket, overridePath,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
