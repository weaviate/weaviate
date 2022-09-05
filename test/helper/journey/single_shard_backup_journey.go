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
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	moduleshelper "github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func singleShardBackupJourneyTest(t *testing.T, weaviateEndpoint, storage, className, snapshotID string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	t.Run("add test data", func(t *testing.T) {
		addTestClass(t, className)
		addTestObjects(t, className)
	})

	t.Run("single shard backup", func(t *testing.T) {
		singleShardBackupJourney(t, className, storage, snapshotID)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func singleShardBackupJourney(t *testing.T, className, storage, snapshotID string) {
	t.Run("create backup", func(t *testing.T) {
		resp, err := helper.CreateBackup(t, className, storage, snapshotID)
		helper.AssertRequestOk(t, resp, err, nil)

		// wait for create success
		createTime := time.Now()
		for {
			if time.Now().After(createTime.Add(10 * time.Second)) {
				break
			}

			resp, err := helper.CreateBackupStatus(t, storage, snapshotID)
			helper.AssertRequestOk(t, resp, err, func() {
				require.NotNil(t, resp)
				require.NotNil(t, resp.Payload)
				require.NotNil(t, resp.Payload.Status)
			})

			if *resp.Payload.Status == string(backup.Success) {
				break
			}
		}

		statusResp, err := helper.CreateBackupStatus(t, storage, snapshotID)
		helper.AssertRequestOk(t, resp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
		})

		require.Equal(t, *statusResp.Payload.Status, string(backup.Success))
	})

	t.Run("delete class for restoration", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})

	t.Run("restore backup", func(t *testing.T) {
		helper.RestoreBackup(t, className, storage, snapshotID)

		// wait for restore success
		restoreTime := time.Now()
		for {
			if time.Now().After(restoreTime.Add(10 * time.Second)) {
				break
			}

			resp, err := helper.RestoreBackupStatus(t, className, storage, snapshotID)
			helper.AssertRequestOk(t, resp, err, func() {
				require.NotNil(t, resp)
				require.NotNil(t, resp.Payload)
				require.NotNil(t, resp.Payload.Status)
			})

			if *resp.Payload.Status == string(backup.Success) {
				break
			}

			time.Sleep(time.Second)
		}

		statusResp, err := helper.RestoreBackupStatus(t, className, storage, snapshotID)
		helper.AssertRequestOk(t, statusResp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
		})

		require.Equal(t, *statusResp.Payload.Status, string(backup.Success))
	})

	// assert class exists again it its entirety
	count := moduleshelper.GetClassCount(t, className)
	assert.Equal(t, int64(500), count)
}

func addTestClass(t *testing.T, className string) {
	class := &models.Class{
		Class: className,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "contents",
				DataType: []string{"string"},
			},
		},
	}

	helper.CreateClass(t, class)
}

func addTestObjects(t *testing.T, className string) {
	const (
		noteLengthMin = 4
		noteLengthMax = 1024

		batchSize  = 10
		numBatches = 50
	)

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			contentsLength := noteLengthMin + seededRand.Intn(noteLengthMax-noteLengthMin+1)
			contents := helper.GetRandomString(contentsLength)

			batch[j] = &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"contents": contents},
			}
		}

		helper.CreateObjectsBatch(t, batch)
	}
}
