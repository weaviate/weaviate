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
	// create
	helper.CreateBackup(t, className, storage, snapshotID)

	// wait for create success
	{
		createTime := time.Now()
		for {
			if time.Now().After(createTime.Add(10 * time.Second)) {
				break
			}

			status := helper.CreateBackupStatus(t, className, storage, snapshotID)
			require.NotNil(t, status)
			if *status.Status == string(backup.CreateSuccess) {
				break
			}
		}

		createStatus := helper.CreateBackupStatus(t, className, storage, snapshotID)
		require.NotNil(t, createStatus)
		require.Equal(t, *createStatus.Status, string(backup.CreateSuccess))
	}

	// remove the class so we can restore it
	helper.DeleteClass(t, className)

	// restore
	helper.RestoreBackup(t, className, storage, snapshotID)

	// wait for restore success
	{
		restoreTime := time.Now()
		for {
			if time.Now().After(restoreTime.Add(10 * time.Second)) {
				break
			}

			status := helper.RestoreBackupStatus(t, className, storage, snapshotID)
			require.NotNil(t, status)
			if *status.Status == string(backup.CreateSuccess) {
				break
			}

			time.Sleep(time.Second)
		}

		restoreStatus := helper.RestoreBackupStatus(t, className, storage, snapshotID)
		require.NotNil(t, restoreStatus)
		require.Equal(t, *restoreStatus.Status, string(backup.CreateSuccess))
	}

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
