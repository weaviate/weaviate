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

package backup_dedupe_replicas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

const oldWeaviateImage = "cr.weaviate.io/semitechnologies/weaviate:1.34.0"

// TestBackupCrossVersionRestore proves a backup created by an older release restores normally on this build without any deduplication machinery.
func TestBackupCrossVersionRestore(t *testing.T) {
	ctx := context.Background()

	const (
		className        = "CrossVersionArticles"
		backupID         = "cross-version-1"
		numSeeded        = 300
		envWeaviateImage = "TEST_WEAVIATE_IMAGE"
	)

	prevImage, hadImage := os.LookupEnv(envWeaviateImage)
	require.NoError(t, os.Setenv(envWeaviateImage, oldWeaviateImage))
	restoreImageEnv := func() {
		if hadImage {
			_ = os.Setenv(envWeaviateImage, prevImage)
		} else {
			_ = os.Unsetenv(envWeaviateImage)
		}
	}
	defer restoreImageEnv()

	oldCompose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		Start(ctx)
	require.NoError(t, err)
	oldTerminated := false
	defer func() {
		if !oldTerminated {
			require.NoError(t, oldCompose.Terminate(ctx))
		}
	}()
	restoreImageEnv()

	helper.SetupClient(oldCompose.GetWeaviate().URI())
	defer helper.ResetClient()

	var ids []strfmt.UUID
	t.Run("old release creates a plain backup", func(t *testing.T) {
		helper.CreateClass(t, newReplicatedClass(className))
		ids = seedObjects(t, oldCompose.GetWeaviate().URI(), className, numSeeded)
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))
	})

	artifact := downloadBackupPrefix(t, minioClient(t, oldCompose.GetMinIO().URI()), backupID)
	require.NotEmpty(t, artifact)

	require.NoError(t, oldCompose.Terminate(ctx))
	oldTerminated = true

	newCompose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, newCompose.Terminate(ctx))
	}()
	defer dumpNodeLogs(t, newCompose)

	host := newCompose.GetWeaviate().URI()
	helper.SetupClient(host)
	newMinio := minioClient(t, newCompose.GetMinIO().URI())
	uploadBackupPrefix(t, newMinio, artifact)

	t.Run("this build restores it without dedupe machinery", func(t *testing.T) {
		var global entbackup.DistributedBackupDescriptor
		require.True(t, readJSONObject(t, newMinio, fmt.Sprintf("%s/%s", backupID, ubak.GlobalBackupFile), &global))
		assert.False(t, global.DedupeReplicas)
		assert.NotEqual(t, ubak.VersionDedupeReplicas, global.Version)

		_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendS3, backupID, nil, false)
		if err != nil {
			t.Fatalf("restore refused: %s", restoreErrorMessage(err))
		}
		helper.ExpectBackupEventuallyRestored(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))
		requireOnEveryNode(t, host, className, ids)
	})
}

func downloadBackupPrefix(t *testing.T, client *minio.Client, prefix string) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	out := map[string][]byte{}
	for obj := range client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		require.NoError(t, obj.Err)
		r, err := client.GetObject(ctx, bucketName, obj.Key, minio.GetObjectOptions{})
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, r.Close())
		require.NoError(t, err)
		out[obj.Key] = data
	}
	return out
}

func uploadBackupPrefix(t *testing.T, client *minio.Client, objects map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	for key, data := range objects {
		_, err := client.PutObject(ctx, bucketName, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		require.NoError(t, err)
	}
}
