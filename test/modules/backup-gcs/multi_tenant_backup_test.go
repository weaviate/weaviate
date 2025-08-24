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

const numTenants = 50

func Test_MultiTenantBackupJourney(t *testing.T) {
	// Set up a context with a 30-minute timeout to manage test duration
	ctx := context.Background()

	// Define test cases using a table-driven approach
	tests := []struct {
		name               string // Name for the subtest
		override           bool   // Whether to override bucket/path
		bucket             string // Bucket name
		overrideBucket     string // Override bucket name (if any)
		overrideBucketPath string // Override path for bucket (if any)
	}{
		{
			name:               "default backup journey",
			override:           false,
			bucket:             "backups",
			overrideBucket:     "",
			overrideBucketPath: "",
		},
		{
			name:               "with override bucket and path",
			override:           true,
			bucket:             "backups",
			overrideBucket:     "gcsmbjtestbucketoverride",
			overrideBucketPath: "testBucketPathOverride",
		},
	}

	// Run each test case as a subtest
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the multi-tenant backup journey with the specified parameters
			multiTenantBackupJourneyStart(
				t,
				ctx,
				tt.override,           // Apply override if true
				tt.bucket,             // Primary bucket name
				tt.overrideBucket,     // Override bucket name, if set
				tt.overrideBucketPath, // Override path, if set
			)
		})
	}
}

func multiTenantBackupJourneyStart(t *testing.T, ctx context.Context, override bool, containerName, overrideBucket, overridePath string) {
	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	t.Run("single node", func(t *testing.T) {
		ctx := context.Background()
		t.Log("pre-instance env setup")
		gcsBackupJourneyBucketName := "gcp-single-node"
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
		if override {
			moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, overrideBucket)
			defer moduleshelper.DeleteGCSBucket(ctx, t, overrideBucket)
		}
		defer moduleshelper.DeleteGCSBucket(ctx, t, gcsBackupJourneyBucketName)

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"gcs", gcsBackupJourneyClassName,
				gcsBackupJourneyBackupIDSingleNode, tenantNames, override, overrideBucket, overridePath)
		})
	})

	t.Run("multiple node", func(t *testing.T) {
		ctx := context.Background()
		t.Log("pre-instance env setup")
		gcsBackupJourneyBucketName := "gcp-multiple-nodes"
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
		if override {
			moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, overrideBucket)
			defer moduleshelper.DeleteGCSBucket(ctx, t, overrideBucket)
		}
		defer moduleshelper.DeleteGCSBucket(ctx, t, gcsBackupJourneyBucketName)

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "gcs", gcsBackupJourneyClassName,
				gcsBackupJourneyBackupIDCluster+overrideBucket, tenantNames, override, overrideBucket, overridePath,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})
	})
}
