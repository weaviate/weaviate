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

package journey

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/books"
	"github.com/stretchr/testify/require"
)

func backupAndRestoreJourneyTest(t *testing.T, weaviateEndpoint, storage string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}
	booksClass := books.ClassContextionaryVectorizer()
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

	snapshotID := "snapshot-1"
	t.Run("add data to Books schema", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("verify that Books objects exist", func(t *testing.T) {
		verifyThatAllBooksExist(t)
	})

	t.Run("start backup process", func(t *testing.T) {
		params := schema.NewSchemaObjectsSnapshotsCreateParams().
			WithClassName(booksClass.Class).
			WithStorageName(storage).
			WithID(snapshotID)
		resp, err := helper.Client(t).Schema.SchemaObjectsSnapshotsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, func() {
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			require.Equal(t, models.SnapshotMetaStatusSTARTED, *meta.Status)
		})
	})

	t.Run("verify that backup process is completed", func(t *testing.T) {
		params := schema.NewSchemaObjectsSnapshotsCreateStatusParams().
			WithClassName(booksClass.Class).
			WithStorageName(storage).
			WithID(snapshotID)
		for {
			resp, err := helper.Client(t).Schema.SchemaObjectsSnapshotsCreateStatus(params, nil)
			require.Nil(t, err)
			require.NotNil(t, resp)
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			switch *meta.Status {
			case models.SnapshotMetaStatusSUCCESS:
				return
			case models.SnapshotMetaStatusFAILED:
				t.Errorf("failed to create snapshot, got response: %+v", meta)
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

	t.Run("start restore process", func(t *testing.T) {
		params := schema.NewSchemaObjectsSnapshotsRestoreParams().
			WithClassName(booksClass.Class).
			WithStorageName(storage).
			WithID(snapshotID)
		resp, err := helper.Client(t).Schema.SchemaObjectsSnapshotsRestore(params, nil)
		helper.AssertRequestOk(t, resp, err, func() {
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			require.Equal(t, models.SnapshotMetaStatusSTARTED, *meta.Status)
		})
	})

	t.Run("verify that restore process is completed", func(t *testing.T) {
		params := schema.NewSchemaObjectsSnapshotsRestoreStatusParams().
			WithClassName(booksClass.Class).
			WithStorageName(storage).
			WithID(snapshotID)
		for {
			resp, err := helper.Client(t).Schema.SchemaObjectsSnapshotsRestoreStatus(params, nil)
			require.Nil(t, err)
			require.NotNil(t, resp)
			meta := resp.GetPayload()
			require.NotNil(t, meta)
			switch *meta.Status {
			case models.SnapshotRestoreMetaStatusSUCCESS:
				return
			case models.SnapshotRestoreMetaStatusFAILED:
				t.Errorf("failed to create snapshot, got response: %+v", meta)
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	})

	t.Run("verify that Books objects exist after restore", func(t *testing.T) {
		verifyThatAllBooksExist(t)
	})
}
