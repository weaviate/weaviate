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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"google.golang.org/api/option"
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
	var client *storage.Client
	var err error = fmt.Errorf("client not yet defined")
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		client, err = storage.NewClient(ctx, option.WithoutAuthentication())
		assert.Nil(collect, err)

		client.Bucket(bucketName).Create(ctx, projectID, nil)
		itr := client.Buckets(ctx, projectID)
		err = fmt.Errorf("bucket not found")
		for b, _ := itr.Next(); b != nil; b, _ = itr.Next() {
			if b.Name == bucketName {
				err = nil
				return
			}
		}
		assert.Nil(collect, err)
	}, 30*time.Second, 2*time.Second, "failed to create bucket")

}

func CreateAzureContainer(ctx context.Context, t *testing.T, endpoint, containerName string) {
	t.Log("Waiting for Azure Storage Emulator to start")
	var client *azblob.Client
	var err error = fmt.Errorf("client not yet defined")
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
		client, err = azblob.NewClientFromConnectionString(fmt.Sprintf(connectionString, endpoint), nil)
		assert.Nil(collect, err)

		t.Log("Creating azure container", containerName)
		_, err = client.CreateContainer(ctx, containerName, nil)
		pager := client.NewListContainersPager(&azblob.ListContainersOptions{})
		page, _ := pager.NextPage(ctx)

		for _, container := range page.ContainerItems {
			if container.Name != nil && *container.Name == containerName {
				err = nil
				return
			}
		}
		assert.Nil(collect, err)
	}, 15*time.Second, 2*time.Second)
}

func DeleteAzureContainer(ctx context.Context, t *testing.T, endpoint, containerName string) {
	var client *azblob.Client
	var err error = fmt.Errorf("client not yet defined")
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
		client, err = azblob.NewClientFromConnectionString(fmt.Sprintf(connectionString, endpoint), nil)
		require.Nil(t, err)

		t.Log("Deleting azure container", containerName)
		client.DeleteContainer(ctx, containerName, nil)
		pager := client.NewListContainersPager(&azblob.ListContainersOptions{})
		page, _ := pager.NextPage(ctx)
		err = nil
		for _, container := range page.ContainerItems {
			if container.Name != nil && *container.Name == containerName {
				err = fmt.Errorf("container not deleted")

			}
		}



		require.Nil(collect, err)
	}, 30*time.Second, 2*time.Second)
}
