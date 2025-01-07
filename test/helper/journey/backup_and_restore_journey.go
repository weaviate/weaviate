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

package journey

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func backupAndRestoreJourneyTest(t *testing.T, weaviateEndpoint, backend string, namedVectors, override bool, overrideName, overridePath string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	var booksClass *models.Class
	if namedVectors {
		booksClass = books.ClassNamedContextionaryVectorizer()
	} else {
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

		if namedVectors {
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
		} else {
			vectors["vector"] = duneBook.Vector
		}
		return vectors
	}

	backupID := "backup-1_named_vectors_" + strconv.FormatBool(namedVectors)
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
			_, ok := err.(*backups.BackupsCreateUnprocessableEntity)
			require.True(t, ok, "not backups.BackupsCreateUnprocessableEntity")
		})

		// out of band cpu %
		resp, err = helper.CreateBackup(t, &models.BackupConfig{
			CPUPercentage: 120,
		}, booksClass.Class, backend, backupID)
		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*backups.BackupsCreateUnprocessableEntity)
			require.True(t, ok, "not backups.BackupsCreateUnprocessableEntity")
		})

		// out of band chunkSize
		resp, err = helper.CreateBackup(t, &models.BackupConfig{
			ChunkSize: 1024,
		}, booksClass.Class, backend, backupID)
		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*backups.BackupsCreateUnprocessableEntity)
			require.True(t, ok, "not backups.BackupsCreateUnprocessableEntity")
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
					ChunkSize:        512,
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
		for {
			resp, err := helper.Client(t).Backups.BackupsCreateStatus(params, nil)
			require.Nil(t, err)
			require.NotNil(t, resp)
			t.Logf("Backup create response: %+v\n", resp)

			meta := resp.GetPayload()
			require.NotNil(t, meta)
			t.Logf("Backup create response meta: %+v\n", meta)

			if err != nil {
				t.Logf("failed to get backup status: %+v", err)
			}

			t.Logf("backup status: %+v\n", meta)
			switch *meta.Status {
			case models.BackupCreateStatusResponseStatusSUCCESS:
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
		}, booksClass.Class, backend, backupID, map[string]string{})

		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*backups.BackupsRestoreUnprocessableEntity)
			require.True(t, ok, "not backups.BackupsRestoreUnprocessableEntity")
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
