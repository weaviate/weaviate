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

package moduleshelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func EnsureClassExists(t *testing.T, className string, tenant string) {
	query := fmt.Sprintf("{Aggregate{%s", className)
	if tenant != "" {
		query += fmt.Sprintf("(tenant:%q)", tenant)
	}
	query += " { meta { count}}}}"
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

	class := resp.Get("Aggregate", className).Result.([]interface{})
	require.Len(t, class, 1)
}

func EnsureCompressedVectorsRestored(t *testing.T, className string) {
	query := fmt.Sprintf("{Get{%s(limit:1){_additional{vector}}}}", className)
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

	class := resp.Get("Get", className).Result.([]interface{})
	require.Len(t, class, 1)
	vecResp := class[0].(map[string]interface{})["_additional"].(map[string]interface{})["vector"].([]interface{})

	searchVec := graphqlhelper.Vec2String(graphqlhelper.ParseVec(t, vecResp))

	limit := 10
	query = fmt.Sprintf(
		"{Get{%s(nearVector:{vector:%s} limit:%d){_additional{vector}}}}",
		className, searchVec, limit)
	resp = graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	class = resp.Get("Get", className).Result.([]interface{})
	require.Len(t, class, limit)
}

func GetClassCount(t *testing.T, className string, tenant string) int64 {
	query := fmt.Sprintf("{Aggregate{%s", className)
	if tenant != "" {
		query += fmt.Sprintf("(tenant:%q)", tenant)
	}
	query += " { meta { count}}}}"
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

	class := resp.Get("Aggregate", className).Result.([]interface{})
	require.Len(t, class, 1)

	meta := class[0].(map[string]interface{})["meta"].(map[string]interface{})

	countPayload := meta["count"].(json.Number)

	count, err := countPayload.Int64()
	require.Nil(t, err)

	return count
}

func CreateTestFiles(t *testing.T, dirPath string) []string {
	count := 5
	filePaths := make([]string, count)
	var fileName string

	for i := 0; i < count; i += 1 {
		fileName = fmt.Sprintf("file_%d.db", i)
		filePaths[i] = filepath.Join(dirPath, fileName)
		file, err := os.Create(filePaths[i])
		if err != nil {
			t.Fatalf("failed to create test file '%s': %s", fileName, err)
		}
		fmt.Fprintf(file, "This is content of db file named %s", fileName)
		file.Close()
		t.Logf("Created test file: %s\n", filePaths[i])
	}
	return filePaths
}

func CreateGCSBucket(ctx context.Context, t *testing.T, projectID, bucketName string) {
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		opts := []option.ClientOption{option.WithoutAuthentication()}
		if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
			opts = append(opts, option.WithEndpoint(emulatorHost))
		}
		client, err := storage.NewClient(ctx, opts...)
		assert.Nil(t, err)
		defer client.Close()

		assert.Nil(t, client.Bucket(bucketName).Create(ctx, projectID, nil))
	}, 10*time.Second, 500*time.Millisecond)
}

func DeleteGCSBucket(ctx context.Context, t *testing.T, bucketName string) {
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		opts := []option.ClientOption{option.WithoutAuthentication()}
		if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
			opts = append(opts, option.WithEndpoint(emulatorHost))
		}
		client, err := storage.NewClient(ctx, opts...)
		assert.Nil(t, err)
		defer client.Close()

		bucket := client.Bucket(bucketName)
		// we do iterate over objects because GCP doesn't allow deleting non-empty buckets
		it := bucket.Objects(ctx, nil)
		for {
			objAttrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			assert.Nil(t, err)

			obj := bucket.Object(objAttrs.Name)
			err = obj.Delete(ctx)
			assert.Nil(t, err)
		}
		assert.Nil(t, bucket.Delete(ctx))
	}, 5*time.Second, 500*time.Millisecond)
}

func CreateAzureContainer(ctx context.Context, t *testing.T, endpoint, containerName string) {
	t.Log("Creating azure container", containerName)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		// First try to create a client to check if Azurite is ready
		connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
		client, err := azblob.NewClientFromConnectionString(fmt.Sprintf(connectionString, endpoint), nil)
		assert.NoError(collect, err, "Failed to create Azure client")

		// Try to list containers to verify connection
		pager := client.NewListContainersPager(nil)
		_, err = pager.NextPage(ctx)
		assert.NoError(collect, err, "Failed to list containers (Azurite might not be ready)")

		// Now try to create the container
		_, err = client.CreateContainer(ctx, containerName, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerAlreadyExists" {
				// Container already exists, we're good
				return
			}
			assert.NoError(collect, err, "Failed to create container %s", containerName)
		}
	}, 10*time.Second, 1*time.Second)
}

func DeleteAzureContainer(ctx context.Context, t *testing.T, endpoint, containerName string) {
	t.Log("Deleting azure container", containerName)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
		client, err := azblob.NewClientFromConnectionString(fmt.Sprintf(connectionString, endpoint), nil)
		assert.NoError(collect, err, "Failed to create Azure client")

		_, err = client.DeleteContainer(ctx, containerName, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerNotFound" {
				// Container doesn't exist, which is fine for deletion
				return
			}
			assert.NoError(collect, err, "Failed to delete container %s", containerName)
		}
	}, 10*time.Second, 1*time.Second)
}

func CreateS3Bucket(ctx context.Context, t *testing.T, endpoint, region, bucketName string) {
	t.Logf("Creating S3 bucket %s at endpoint %s", bucketName, endpoint)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		client, err := createS3Client(ctx, endpoint, region)
		assert.NoError(collect, err, "Failed to create S3 client")

		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			// Ignore "bucket already exists" errors
			if !isBucketAlreadyExistsError(err) {
				assert.NoError(collect, err, "Failed to create bucket %s", bucketName)
			}
		}
	}, 10*time.Second, 500*time.Millisecond)
}

func DeleteS3Bucket(ctx context.Context, t *testing.T, endpoint, region, bucketName string) {
	t.Logf("Deleting S3 bucket %s", bucketName)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		client, err := createS3Client(ctx, endpoint, region)
		assert.NoError(collect, err, "Failed to create S3 client")

		// First, delete all objects in the bucket
		listOutput, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err == nil && listOutput.Contents != nil {
			for _, obj := range listOutput.Contents {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}

		// Now delete the bucket
		_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		// Ignore "bucket not found" errors during cleanup
		if err != nil && !isBucketNotFoundError(err) {
			assert.NoError(collect, err, "Failed to delete bucket %s", bucketName)
		}
	}, 10*time.Second, 500*time.Millisecond)
}

func createS3Client(ctx context.Context, endpoint, region string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"aws_access_key", "aws_secret_key", "",
		)),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	}), nil
}

func isBucketAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// MinIO and S3 return different error messages for existing buckets
	return contains(errStr, "BucketAlreadyOwnedByYou") ||
		contains(errStr, "BucketAlreadyExists") ||
		contains(errStr, "bucket already exists")
}

func isBucketNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "NoSuchBucket") || contains(errStr, "NotFound")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
