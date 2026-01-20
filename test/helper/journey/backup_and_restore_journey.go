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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/backup"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"

	gofakeit "github.com/brianvoe/gofakeit/v6"
)

type vectorsConfigType string

const (
	vectorsLegacy vectorsConfigType = "legacy"
	vectorsNamed  vectorsConfigType = "named"
	vectorsMixed  vectorsConfigType = "mixed"
)

func backupAndRestoreJourneyTest(t *testing.T, weaviateEndpoint, backend string, vectorsConfigType vectorsConfigType, overrideName, overridePath string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	var booksClass *models.Class
	switch vectorsConfigType {
	case vectorsNamed:
		booksClass = books.ClassNamedContextionaryVectorizer()
	case vectorsMixed:
		booksClass = books.ClassMixedContextionaryVectorizer()
	default:
		booksClass = books.ClassContextionaryVectorizer()
	}
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	verifyThatAllBooksExist := func(t *testing.T) {
		book := helper.AssertGetObject(t, booksClass.Class, books.Dune)
		require.Equal(t, books.Dune, book.ID)
		book = helper.AssertGetObject(t, booksClass.Class, books.ProjectHailMary)
		require.Equal(t, books.ProjectHailMary, book.ID)
		book = helper.AssertGetObject(t, booksClass.Class, books.TheLordOfTheIceGarden)
		require.Equal(t, books.TheLordOfTheIceGarden, book.ID)
	}

	vectorsForDune := func() map[string][]float32 {
		vectors := map[string][]float32{}
		duneBook := helper.AssertGetObject(t, booksClass.Class, books.Dune)

		if vectorsConfigType == vectorsNamed || vectorsConfigType == vectorsMixed {
			for name := range booksClass.VectorConfig {
				switch vec := duneBook.Vectors[name].(type) {
				case []float32:
					vectors[name] = vec
				case [][]float32:
					// do nothing
				default:
					// do nothing
				}
			}
		}
		if vectorsConfigType == vectorsLegacy || vectorsConfigType == vectorsMixed {
			vectors["vector"] = duneBook.Vector
		}
		return vectors
	}

	backupID := "backup-1_named_vectors_" + string(vectorsConfigType)
	t.Run("add data to Books schema", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("verify that Books objects exist", func(t *testing.T) {
		verifyThatAllBooksExist(t)
	})
	initialVectors := vectorsForDune()

	t.Run("verify invalid compression config", func(t *testing.T) {
		// unknown compression level
		resp, err := helper.CreateBackup(t, &models.BackupConfig{
			CompressionLevel: "some-weird-config",
		}, booksClass.Class, backend, backupID)

		helper.AssertRequestFail(t, resp, err, func() {
			var customErr *backups.BackupsCreateUnprocessableEntity
			require.True(t, errors.As(err, &customErr), "not backups.BackupsCreateUnprocessableEntity")
		})

		// out of band cpu %
		resp, err = helper.CreateBackup(t, &models.BackupConfig{
			CPUPercentage: 120,
		}, booksClass.Class, backend, backupID)
		helper.AssertRequestFail(t, resp, err, func() {
			var customErr *backups.BackupsCreateUnprocessableEntity
			require.True(t, errors.As(err, &customErr), "not backups.BackupsCreateUnprocessableEntity")
		})
	})

	t.Run("start backup process", func(t *testing.T) {
		params := backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:      backupID,
				Include: []string{booksClass.Class},
				Config: &models.BackupConfig{
					CPUPercentage:    80,
					CompressionLevel: models.BackupConfigCompressionLevelDefaultCompression,
					Bucket:           overrideName,
					Path:             overridePath,
				},
			})
		resp, err := helper.Client(t).Backups.BackupsCreate(params, nil)

		helper.AssertRequestOk(t, resp, err, func() {
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			require.Equal(t, models.BackupCreateStatusResponseStatusSTARTED, *meta.Status)
		})
	})

	t.Run("verify that backup process is completed", func(t *testing.T) {
		params := backups.NewBackupsCreateStatusParams().
			WithBackend(backend).
			WithID(backupID).
			WithBucket(&overrideName).
			WithPath(&overridePath)

		var startTime time.Time
		var completedTime time.Time

		for {
			resp, err := helper.Client(t).Backups.BackupsCreateStatus(params, nil)
			require.Nil(t, err)
			require.NotNil(t, resp)

			meta := resp.GetPayload()
			require.NotNil(t, meta)
			require.NotNil(t, meta.StartedAt)

			// Capture start time on first iteration
			if startTime.IsZero() {
				startTime = time.Time(meta.StartedAt)
				t.Logf("Backup started at: %v", startTime)
			}

			if err != nil {
				t.Logf("failed to get backup status: %+v", err)
			}

			switch *meta.Status {
			case models.BackupCreateStatusResponseStatusSUCCESS:
				require.NotNil(t, meta.CompletedAt)
				completedTime = time.Time(meta.CompletedAt)
				t.Logf("Backup completed at: %v", completedTime)

				// Verify timestamps are reasonable
				require.True(t, !startTime.IsZero(), "Start time should not be zero")
				require.True(t, !completedTime.IsZero(), "Completed time should not be zero")
				require.True(t, completedTime.After(startTime), "Completed time should be after start time")

				// Verify timestamps are recent (within last hour)
				now := time.Now()
				require.True(t, startTime.After(now.Add(-1*time.Hour)), "Start time should be within the last hour")
				require.True(t, completedTime.After(now.Add(-1*time.Hour)), "Completed time should be within the last hour")

				t.Logf("Backup duration: %v", completedTime.Sub(startTime))
				return
			case models.BackupCreateStatusResponseStatusFAILED:
				t.Errorf("failed to create backup, got response: %+v", meta)
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	})

	t.Run("verify that Books objects still exist", func(t *testing.T) {
		verifyThatAllBooksExist(t)
	})

	t.Run("remove Books class", func(t *testing.T) {
		helper.DeleteClass(t, booksClass.Class)
	})

	t.Run("verify that objects don't exist", func(t *testing.T) {
		err := helper.AssertGetObjectFailsEventually(t, booksClass.Class, books.Dune)
		require.NotNil(t, err)
		err = helper.AssertGetObjectFailsEventually(t, booksClass.Class, books.ProjectHailMary)
		require.NotNil(t, err)
		err = helper.AssertGetObjectFailsEventually(t, booksClass.Class, books.TheLordOfTheIceGarden)
		require.NotNil(t, err)
	})

	// out of band cpu %
	t.Run("invalid restore request", func(t *testing.T) {
		resp, err := helper.RestoreBackup(t, &models.RestoreConfig{
			CPUPercentage: 180,
			Bucket:        overrideName,
			Path:          overridePath,
		}, booksClass.Class, backend, backupID, map[string]string{}, false)
		helper.AssertRequestFail(t, resp, err, func() {
			var customErr *backups.BackupsRestoreUnprocessableEntity
			require.True(t, errors.As(err, &customErr), "not backups.BackupsRestoreUnprocessableEntity")
		})
	})

	t.Run("start restore process", func(t *testing.T) {
		params := backups.NewBackupsRestoreParams().
			WithBackend(backend).
			WithID(backupID).
			WithBody(&models.BackupRestoreRequest{
				Include: []string{booksClass.Class},
				Config: &models.RestoreConfig{
					CPUPercentage: 80,
					Bucket:        overrideName,
					Path:          overridePath,
				},
			})
		resp, err := helper.Client(t).Backups.BackupsRestore(params, nil)
		helper.AssertRequestOk(t, resp, err, func() {
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			require.Equal(t, models.BackupCreateStatusResponseStatusSTARTED, *meta.Status)
		})
	})
	t.Run("verify that restore process is completed", func(t *testing.T) {
		params := backups.NewBackupsRestoreStatusParams().
			WithBackend(backend).
			WithID(backupID).
			WithBucket(&overrideName).
			WithPath(&overridePath)
		for {
			resp, err := helper.Client(t).Backups.BackupsRestoreStatus(params, nil)
			require.Nil(t, err)
			require.NotNil(t, resp)
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			switch *meta.Status {
			case models.BackupRestoreStatusResponseStatusSUCCESS:
				return
			case models.BackupRestoreStatusResponseStatusFAILED:
				t.Errorf("failed to restore backup, got response: %+v", meta)
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	})

	t.Run("verify that Books objects exist after restore", func(t *testing.T) {
		verifyThatAllBooksExist(t)
	})

	t.Run("verify that vectors are the same after restore", func(t *testing.T) {
		restoredVectors := vectorsForDune()
		require.Equal(t, initialVectors, restoredVectors)
	})
}

type book struct {
	Title       string   `fake:"{sentence:10}"`
	Description string   `fake:"{paragraph:10}"`
	Tags        []string `fake:"{words:3}"`
}

func (b *book) toObject(class string) *models.Object {
	return &models.Object{
		Class: class,
		Properties: map[string]interface{}{
			"title":       b.Title,
			"description": b.Description,
			"tags":        b.Tags,
		},
	}
}

func backupAndRestoreLargeCollectionJourneyTest(t *testing.T, weaviateEndpoint, backend string, overrideName, overridePath string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	booksClass := books.ClassMixedContextionaryVectorizer()
	booksClass.Class = "BookLargeCollection"
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
	checkCount(t, []string{weaviateEndpoint}, booksClass.Class, numObjects)

	// wait for compactions
	time.Sleep(30 * time.Second)

	backupID := "backup-large-collection"

	cfg := helper.DefaultBackupConfig()

	resp, err := helper.CreateBackup(t, cfg, booksClass.Class, backend, backupID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	assert.Equal(t, backupID, resp.Payload.ID)

	assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, overrideName, overridePath)
		t.Logf("backup status response: %+v, err: %v", resp, err)
		assert.NoError(t1, err, "expected nil, got: %v", err)

		assert.NotNil(t1, resp)
		assert.NotNil(t1, resp.Payload)
		assert.NotNil(t1, resp.Payload.Status)
		assert.Equal(t1, backupID, resp.Payload.ID)
		assert.Equal(t1, backend, resp.Payload.Backend)
		assert.Contains(t1, resp.Payload.Path, overrideName)
		assert.Contains(t1, resp.Payload.Path, overridePath)

		assert.True(t1, *resp.Payload.Status == "SUCCESS")
	}, 120*time.Second, 1000*time.Millisecond)

	time.Sleep(time.Second)
	helper.DeleteClass(t, booksClass.Class)

	respRest, err := helper.RestoreBackup(t, nil, booksClass.Class, backend, backupID, nil, false)
	require.NoError(t, err, "expected nil, got: %v", err)
	assert.Equal(t, backupID, respRest.Payload.ID)
	assert.Equal(t, backend, respRest.Payload.Backend)

	assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
		resp, err := helper.RestoreBackupStatus(t, backend, backupID, overrideName, overridePath)
		assert.Nil(t1, err, "expected nil, got: %v", err)

		assert.NotNil(t1, resp)
		assert.NotNil(t1, resp.Payload)
		assert.NotNil(t1, resp.Payload.Status)

		assert.True(t1, *resp.Payload.Status == string(backup.Success))
	}, 120*time.Second, 1000*time.Millisecond)

	checkCount(t, []string{weaviateEndpoint}, booksClass.Class, numObjects)
}

func checkCount(t *testing.T, nodeEndpoints []string, classname string, numObjects int) {
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
