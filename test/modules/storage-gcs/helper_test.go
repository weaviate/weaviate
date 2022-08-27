//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func makeTestDir(t *testing.T, basePath string) string {
	rand.Seed(time.Now().UnixNano())
	dirPath := filepath.Join(basePath, strconv.Itoa(rand.Intn(10000000)))
	makeDir(t, dirPath)
	return dirPath
}

func makeDir(t *testing.T, dirPath string) {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		t.Fatalf("failed to make test dir '%s': %s", dirPath, err)
	}
}

func removeDir(t *testing.T, dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		t.Errorf("failed to remove test dir '%s': %s", dirPath, err)
	}
}

func createTestFiles(t *testing.T, dirPath string) []string {
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
	}
	return filePaths
}

func createClass(t *testing.T, class *models.Class) {
	helper.CreateClass(t, class)
}

func deleteClass(t *testing.T, className string) {
	helper.DeleteClass(t, className)
}

func getClassCount(t *testing.T, className string) int64 {
	query := fmt.Sprintf("{ Aggregate { %s { meta { count}}}}", className)
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

	class := resp.Get("Aggregate", className).Result.([]interface{})
	require.Len(t, class, 1)

	meta := class[0].(map[string]interface{})["meta"].(map[string]interface{})

	countPayload := meta["count"].(json.Number)

	count, err := countPayload.Int64()
	require.Nil(t, err)

	return count
}

func createObjectsBatch(t *testing.T, batch []*models.Object) {
	helper.CreateObjectsBatch(t, batch)
}

func getCreateBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.SnapshotMeta {
	return helper.CreateBackupStatus(t, className, storageName, snapshotID)
}

func getRestoreBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.SnapshotRestoreMeta {
	return helper.RestoreBackupStatus(t, className, storageName, snapshotID)
}

func createBucket(ctx context.Context, t *testing.T, projectID, bucketName string) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.Nil(t, err)

	err = client.Bucket(bucketName).Create(ctx, projectID, nil)
	gcsErr, ok := err.(*googleapi.Error)
	if ok {
		// the bucket persists from the previous test.
		// if the bucket already exists, we can proceed
		if gcsErr.Code == http.StatusConflict {
			return
		}
	}
	require.Nil(t, err)
}
