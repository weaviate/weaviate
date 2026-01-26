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

package journey

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

const (
	s3BackupJourneyAccessKey = "aws_access_key"
	s3BackupJourneySecretKey = "aws_secret_key"
	S3BucketName             = "s3-backup-journey-bucket"
)

func clusterBackupJourneyTest(t *testing.T, backend, className,
	backupID, coordinatorEndpoint string, tenantNames []string, pqEnabled bool, override bool, overrideBucket, overrideLocation string,
	nodeEndpoints ...string,
) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)
	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	if len(tenantNames) > 0 {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, multiTenant)
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenantNames {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
			addTestObjects(t, className, tenantNames)
		})
	} else {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, !multiTenant)
			addTestObjects(t, className, nil)
		})
	}

	if pqEnabled {
		pq := map[string]interface{}{
			"enabled":   true,
			"segments":  1,
			"centroids": 16,
		}
		helper.EnablePQ(t, className, pq)
	}

	helper.SetupClient(coordinatorEndpoint)
	t.Logf("coordinator selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// send backup requests to the chosen coordinator
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney,
			checkClassAndDataPresence, tenantNames, pqEnabled, map[string]string{}, override, overrideBucket, overrideLocation)
	})

	t.Run(fmt.Sprintf("cancelling with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourneyWithCancellation(t, className, backend, fmt.Sprintf("%s_with_cancellation", backupID), clusterJourney, overrideBucket, overrideLocation)
	})

	t.Run(fmt.Sprintf("listing backups with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourneyWithListing(t, clusterJourney, className, backend, backupID, overrideBucket, overrideLocation)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func clusterBackupEmptyClassJourneyTest(t *testing.T, backend, className, backupID,
	coordinatorEndpoint string, tenantNames []string, override bool, overrideBucket, overridePath string, nodeEndpoints ...string,
) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)
	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	if len(tenantNames) > 0 {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, multiTenant)
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenantNames {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
		})
	} else {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, !multiTenant)
		})
	}

	helper.SetupClient(coordinatorEndpoint)
	t.Logf("coordinator selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// send backup requests to the chosen coordinator
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney,
			checkClassPresenceOnly, tenantNames, false, map[string]string{}, false, "", "")
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func clusterNodeMappingBackupJourneyTest(t *testing.T, backend, className, backupID, coordinatorEndpoint string, override bool, overrideBucket, overridePath string, nodeEndpoints ...string) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)

	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)
	t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
		addTestClass(t, className, !multiTenant)
	})

	// send backup requests to the chosen coordinator, with nodeMapping.
	// for nodeMapping we simply reverse the node(s) around, where node1 is now node2 and node2 is now node1.
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney, checkClassPresenceOnly, nil, false,
			map[string]string{"node1": "node2", "node2": "node1"}, override, overrideBucket, overridePath)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func clusterIncrementalBackupJourneyTest(t *testing.T, backend, overrideBucket, overridePath, minioURL string, nodeEndpoints ...string) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)

	booksClass := books.ClassMixedContextionaryVectorizerFlat()
	booksClass.Class = "BookLargeCollectionIncremental"
	helper.DeleteClass(t, booksClass.Class)
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	// add lots of data
	numObjects := 10000
	for i := 0; i < numObjects; i++ {
		var b book
		gofakeit.Struct(&b)
		helper.CreateObject(t, b.toObject(booksClass.Class))
	}
	checkCount(t, []string{uploaderEndpoint}, booksClass.Class, numObjects)

	backupID := "backup-large-collection-base"
	createBackupAndWait(t, backend, overrideBucket, overridePath, backupID, "", booksClass)

	// add more data after backup completed and do incremental backup
	numObjects2 := 100
	for i := 0; i < numObjects2; i++ {
		var b book
		gofakeit.Struct(&b)
		helper.CreateObject(t, b.toObject(booksClass.Class))
	}

	backupIDIncremental1 := "backup-large-collection-incremental1"
	createBackupAndWait(t, backend, overrideBucket, overridePath, backupIDIncremental1, backupID, booksClass)

	// add more data after backup completed and do a second incremental backup
	for i := 0; i < numObjects2; i++ {
		var b book
		gofakeit.Struct(&b)
		helper.CreateObject(t, b.toObject(booksClass.Class))
	}
	checkCount(t, []string{uploaderEndpoint}, booksClass.Class, numObjects+2*numObjects2)

	backupIDIncremental2 := "backup-large-collection-incremental2"
	createBackupAndWait(t, backend, overrideBucket, overridePath, backupIDIncremental2, backupIDIncremental1, booksClass)

	helper.DeleteClass(t, booksClass.Class)
	restoreBackup(t, backend, overrideBucket, overridePath, backupID, booksClass)
	checkCount(t, []string{uploaderEndpoint}, booksClass.Class, numObjects)

	helper.DeleteClass(t, booksClass.Class)
	restoreBackup(t, backend, overrideBucket, overridePath, backupIDIncremental1, booksClass)
	checkCount(t, []string{uploaderEndpoint}, booksClass.Class, numObjects+numObjects2)

	helper.DeleteClass(t, booksClass.Class)
	restoreBackup(t, backend, overrideBucket, overridePath, backupIDIncremental2, booksClass)
	checkCount(t, []string{uploaderEndpoint}, booksClass.Class, numObjects+2*numObjects2)

	// verify that incremental backups are smaller than the full backup. Only on S3 backends where we can check the
	// actual stored size.
	if minioURL != "" {
		backupSize1, err := getTotalSize(t, minioURL, overrideBucket, backupID)
		require.NoError(t, err)

		backupSize2, err := getTotalSize(t, minioURL, overrideBucket, backupIDIncremental1)
		require.NoError(t, err)

		backupSize3, err := getTotalSize(t, minioURL, overrideBucket, backupIDIncremental2)
		require.NoError(t, err)

		t.Logf("backup size full: %d, backup size incremental1: %d, backup size incremental1: %d", backupSize1, backupSize2, backupSize3)
		require.Less(t, backupSize2*3, backupSize1)
		require.Less(t, backupSize3*3, backupSize1)
	}

	// check that sizes are as expected. We return the size of the pre-compression/deduplication data in the backup status
	res1, err := helper.CreateBackupStatus(t, backend, backupID, overrideBucket, overridePath)
	require.NoError(t, err)
	res2, err := helper.CreateBackupStatus(t, backend, backupIDIncremental1, overrideBucket, overridePath)
	require.NoError(t, err)
	res3, err := helper.CreateBackupStatus(t, backend, backupIDIncremental2, overrideBucket, overridePath)
	require.NoError(t, err)

	require.Greater(t, res2.Payload.Size, res1.Payload.Size)
	require.Greater(t, res3.Payload.Size, res2.Payload.Size)
}

// getFolderChunks gets all the chunks for a given backupID from the specified S3 bucket
func getTotalSize(t *testing.T, minioURL, bucketName, backupId string) (int64, error) {
	t.Helper()

	client, err := minio.New(minioURL, &minio.Options{
		Creds:  credentials.NewStaticV4(s3BackupJourneyAccessKey, s3BackupJourneySecretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// List objects with the specified prefix
	if bucketName == "" {
		bucketName = S3BucketName
	}
	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	totalSize := int64(0)
	for object := range objectCh {
		require.NoError(t, object.Err)
		if !strings.Contains(object.Key, backupId) {
			continue
		}
		t.Logf("found chunk: %s with size: %v", object.Key, object.Size)
		totalSize += object.Size
	}

	return totalSize, nil
}
