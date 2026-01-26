//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
)

const numTenants = 50

func Test_MultiTenantBackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	multiTenantBackupJourneyStart(t, ctx, false, "", "")
	t.Run("with override bucket and path", func(t *testing.T) {
		multiTenantBackupJourneyStart(t, ctx, true, "testbucketoverride", "testBucketPathOverride")
	})
}

func multiTenantBackupJourneyStart(t *testing.T, ctx context.Context, override bool, overrideBucket, overridePath string) {
	bucket := overrideBucket
	if !override {
		bucket = journey.S3BucketName
	}

	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("multiple node", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)
		t.Setenv(envS3Bucket, bucket)

		compose, err := docker.New().
			WithBackendS3(bucket, s3BackupJourneyRegion).
			WithText2VecContextionary().
			WithWeaviateCluster(3).
			WithWeaviateEnv("BACKUP_CHUNK_TARGET_SIZE", "1024").
			WithWeaviateEnv("PERSISTENCE_LSM_MAX_SEGMENT_SIZE", "1024").
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
			minioURL := compose.GetMinIO().URI()

			journey.BackupJourneyTests_Cluster(t, "s3", s3BackupJourneyClassName,
				s3BackupJourneyBackupIDCluster, minioURL, tenantNames, override, overrideBucket, overridePath,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
