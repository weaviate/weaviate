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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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

	s3BackupJourneyClassName       = "S3Backup"
	s3BackupJourneyBackupIDCluster = "s3-backup-cluster"
	s3BackupJourneyRegion          = "eu-west-1"
	s3BackupJourneyAccessKey       = "aws_access_key"
	s3BackupJourneySecretKey       = "aws_secret_key"
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
			WithWeaviateEnv("LOG_LEVEL", "warning").
			WithWeaviateEnv("BACKUP_SHARED_SHARDS_ENABLED", "true").
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()
		minioURL := compose.GetMinIO().URI()

		t.Run("post-instance env setup", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
		})

		t.Run("backup-s3", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "s3", s3BackupJourneyClassName,
				s3BackupJourneyBackupIDCluster, nil, override, overrideBucket, overridePath,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
		})

		t.Run("one copy per shard", func(t *testing.T) {
			defer printLogsOnError(t, compose)
			clusterOneBackupPerShardTest(t, "s3", s3BackupJourneyClassName+"oneCopy", s3BackupJourneyBackupIDCluster+"_one_copy_per_shard", s3BackupJourneyBucketName, minioURL, overridePath,
				compose.GetWeaviateNode(1).URI(), compose.GetWeaviateNode(2).URI(), compose.GetWeaviateNode(3).URI())
		})
	})
}

func printLogsOnError(t *testing.T, compose *docker.DockerCompose) {
	if !t.Failed() {
		return
	}

	// When a test fails, dump logs of all compose containers.
	for _, container := range compose.Containers() {
		logs, err := container.Container().Logs(context.Background())
		if err != nil {
			t.Logf("failed to get logs for container %s: %v", container.Name(), err)
			continue
		}
		func() {
			defer logs.Close()
			t.Logf("=== start for container %s ===\n=== start logs ===", container.Name())

			scanner := bufio.NewScanner(logs)

			for scanner.Scan() {
				line := scanner.Text()
				t.Log(line)
			}
			t.Logf("=== logs for container %s ===\n=== end logs ===", container.Name())
		}()
	}
}

func TestLocal(t *testing.T) {
	// t.Skip("only for local testing with minio")
	clusterOneBackupPerShardTest(t, "s3", s3BackupJourneyClassName+"oneCopy", s3BackupJourneyBackupIDCluster+"_one_copy_per_shard_local", "weaviate-backups", "localhost:9000", "testtesttest", "localhost:8080", "localhost:8081", "localhost:8082")
}

// only run this on s3 as we will check minio directly for the correct sizes
func clusterOneBackupPerShardTest(t *testing.T, backend, className, backupID, bucket, minioURL, overridePath string, nodeEndpoints ...string) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)

	backupID += fmt.Sprintf("%v", rand.Intn(2000)) // ensure unique backup ID per test run

	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)
	t.Logf("Test parameters: backend=%s, className=%s, backupID=%s, bucket=%s, minioURL=%s, overridePath=%s",
		backend, className, backupID, bucket, minioURL, overridePath)
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "contents",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3, AsyncEnabled: true},
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
	}
	helper.DeleteClass(t, class.Class)
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, class.Class)

	// add test data
	numObjects := 1000
	for i := 0; i < numObjects; i++ {
		objs := &models.Object{
			Class: class.Class,
			Properties: map[string]interface{}{
				"contents": fmt.Sprintf("This is test object number %d", i),
			},
		}
		require.NoError(t, helper.CreateObject(t, objs))
	}

	// create backup and wait for completion
	t.Logf("Creating backup with config: bucket=%s, path=%s", bucket, overridePath)
	_, err := helper.CreateBackup(t, &models.BackupConfig{Bucket: bucket, Path: overridePath}, class.Class, backend, backupID)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
		statusResp, err := helper.CreateBackupStatus(t, backend, backupID, bucket, overridePath)
		if err != nil {
			t.Logf("CreateBackupStatus error: %v", err)
		}
		require.NoError(t1, err)
		require.NotNil(t1, statusResp)
		require.NotNil(t1, statusResp.Payload)
		require.NotNil(t1, statusResp.Payload.Status)

		t.Logf("Backup status: ID=%s, Status=%s, Error=%s, Path=%s",
			statusResp.Payload.ID, *statusResp.Payload.Status,
			statusResp.Payload.Error, statusResp.Payload.Path)

		assert.Equal(t1, backupID, statusResp.Payload.ID)
		assert.Equal(t1, backend, statusResp.Payload.Backend)
		assert.Contains(t1, statusResp.Payload.Path, bucket)
		if overridePath != "" {
			assert.Contains(t1, statusResp.Payload.Path, overridePath)
		}

		assert.Equal(t1, string(backup.Success), *statusResp.Payload.Status,
			statusResp.Payload.Error)
	}, 120*time.Second, 1000*time.Millisecond)

	checkEndpoints(t, nodeEndpoints, class.Class, numObjects)
	helper.SetupClient(uploaderEndpoint) // switch back to uploader

	// we have 3 shards on 3 nodes each = 9 shards in total. If
	// - each node backs up its own shards we would get 9 chunks in the backup
	// - each shard is backed up only once we would get 3 chunks in the backup
	t.Logf("Checking folder chunks with minioURL=%s, bucket=%s, backupID=%s", minioURL, bucket, backupID)
	chunks, err := getFolderChunks(t, minioURL, bucket, backupID)
	if err != nil {
		t.Logf("getFolderChunks error: %v", err)
	}
	require.NoError(t, err)
	t.Logf("Found %d chunks", chunks)
	require.Equal(t, 3, chunks, "expected one backup chunk per shard (3)")

	helper.DeleteClass(t, class.Class) // delete class before restore

	// restore backup
	t.Logf("Restoring backup with config: bucket=%s, path=%s", bucket, overridePath)
	_, err = helper.RestoreBackup(t, &models.RestoreConfig{Bucket: bucket, Path: overridePath}, class.Class, backend, backupID, nil, false)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
		statusResp, err := helper.RestoreBackupStatus(t, backend, backupID, bucket, overridePath)
		if err != nil {
			t.Logf("RestoreBackupStatus error: %v", err)
		}
		require.NoError(t1, err)
		require.NotNil(t1, statusResp)
		require.NotNil(t1, statusResp.Payload)
		require.NotNil(t1, statusResp.Payload.Status)

		t.Logf("Restore status: ID=%s, Status=%s, Error=%s, Path=%s",
			statusResp.Payload.ID, *statusResp.Payload.Status,
			statusResp.Payload.Error, statusResp.Payload.Path)

		assert.Equal(t1, backupID, statusResp.Payload.ID)
		assert.Equal(t1, backend, statusResp.Payload.Backend)
		assert.Contains(t1, statusResp.Payload.Path, bucket)
		if overridePath != "" {
			assert.Contains(t1, statusResp.Payload.Path, overridePath)
		}

		assert.Equal(t1, string(backup.Success), *statusResp.Payload.Status,
			statusResp.Payload.Error)
	}, 120*time.Second, 1000*time.Millisecond)

	checkEndpoints(t, nodeEndpoints, class.Class, numObjects)
}

// getFolderChunks gets all the chunks for a given backupID from the specified S3 bucket
func getFolderChunks(t *testing.T, minioURL, bucketName, backupId string) (int, error) {
	t.Helper()
	client, err := minio.New(minioURL, &minio.Options{
		Creds:  credentials.NewStaticV4(s3BackupJourneyAccessKey, s3BackupJourneySecretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// List objects with the specified prefix
	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	numChunks := 0
	for object := range objectCh {
		require.NoError(t, object.Err)
		if !strings.Contains(object.Key, backupId) || !strings.Contains(object.Key, "chunk") {
			t.Logf("ignored chunk: %s", object.Key)
			continue
		}
		t.Logf("found chunk: %s", object.Key)

		numChunks++
	}

	return numChunks, nil
}

func checkEndpoints(t *testing.T, nodeEndpoints []string, classname string, numObjects int) {
	t.Helper()
	for i := range nodeEndpoints {
		helper.SetupClient(nodeEndpoints[i])
		resp, err := queryGQL(t, fmt.Sprintf("{ Aggregate { %s { meta { count } } } }", classname))
		require.NoError(t, err)
		require.Nil(t, resp.Payload.Errors)
		require.NotNil(t, resp.Payload.Data)

		countJson := resp.Payload.Data["Aggregate"].(map[string]interface{})[classname].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"].(json.Number)
		count, err := countJson.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(numObjects), count, "expected all objects to be present on node %d", i+1)
	}
}

func queryGQL(t *testing.T, query string) (*gql.GraphqlPostOK, error) {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	return helper.Client(t).Graphql.GraphqlPost(params, nil)
}
