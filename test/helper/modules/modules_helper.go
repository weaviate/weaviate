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

// TryDeleteGCSBucket attempts to delete a GCS bucket without failing the test.
// Unlike DeleteGCSBucket, this function logs errors instead of failing, making it
// suitable for best-effort cleanup in defer statements or background goroutines.
// It retries bucket deletion up to 10 times to handle eventual consistency issues.
func TryDeleteGCSBucket(ctx context.Context, t *testing.T, bucketName string) {
	opts := []option.ClientOption{option.WithoutAuthentication()}
	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		opts = append(opts, option.WithEndpoint(emulatorHost))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		t.Logf("Failed to create GCS client for cleanup of bucket %s: %v", bucketName, err)
		return
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			break
		}
		obj := bucket.Object(objAttrs.Name)
		_ = obj.Delete(ctx)
	}

	success := false
	for i := 0; i < 10; i++ {
		err := bucket.Delete(ctx)
		if err == nil {
			success = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if success {
		t.Logf("Successfully deleted bucket: %s", bucketName)
	} else {
		t.Logf("Could not delete bucket %s", bucketName)
	}
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
