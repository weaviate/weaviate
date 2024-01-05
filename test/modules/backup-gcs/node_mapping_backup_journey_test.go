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

func Test_NodeMappingBackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("single node", func(t *testing.T) {
		t.Log("pre-instance env setup")
		t.Setenv(envGCSCredentials, "")
		t.Setenv(envGCSProjectID, gcsBackupJourneyProjectID)
		t.Setenv(envGCSBucket, gcsBackupJourneyBucketName)

		compose, err := docker.New().
			WithBackendGCS(gcsBackupJourneyBucketName).
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

		t.Log("post-instance env setup")
		t.Setenv(envGCSEndpoint, compose.GetGCS().URI())
		t.Setenv(envGCSStorageEmulatorHost, compose.GetGCS().URI())
		moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("backup-gcs", func(t *testing.T) {
			journey.NodeMappingBackupJourneyTests_SingleNode_Backup(t, compose.GetWeaviate().URI(),
				"gcs", gcsBackupJourneyClassName, gcsBackupJourneyBackupIDSingleNode, nil)
		})

		// Now change our tests to use the second cluster and trigger a backup restore
		helper.SetupClient(compose.GetSecondWeaviate().URI())

		t.Run("restore-gcs", func(t *testing.T) {
			journey.NodeMappingBackupJourneyTests_SingleNode_Restore(t, compose.GetSecondWeaviate().URI(),
				"gcs", gcsBackupJourneyClassName, gcsBackupJourneyBackupIDSingleNode, nil, map[string]string{
					docker.Weaviate: docker.SecondWeaviate,
				})
		})
	})
}
