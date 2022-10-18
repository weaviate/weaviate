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

type journeyType int

const (
	singleNodeJourney journeyType = iota
	clusterJourney
)

func backupJourney(t *testing.T, className, backend, backupID string, journeyType journeyType) {
	if journeyType == clusterJourney && backend == "filesystem" {
		t.Run("should fail backup/restore with local filesystem backend", func(t *testing.T) {
			backupResp, err := helper.CreateBackup(t, className, backend, backupID)
			assert.Nil(t, backupResp)
			assert.Error(t, err)

			restoreResp, err := helper.RestoreBackup(t, className, backend, backupID)
			assert.Nil(t, restoreResp)
			assert.Error(t, err)
		})
		return
	}

	t.Run("create backup", func(t *testing.T) {
		resp, err := helper.CreateBackup(t, className, backend, backupID)
		helper.AssertRequestOk(t, resp, err, nil)
		// wait for create success
		createTime := time.Now()
		for {
			if time.Now().After(createTime.Add(21 * time.Second)) {
				break
			}

			resp, err := helper.CreateBackupStatus(t, backend, backupID)
			helper.AssertRequestOk(t, resp, err, func() {
				require.NotNil(t, resp)
				require.NotNil(t, resp.Payload)
				require.NotNil(t, resp.Payload.Status)
			})

			if *resp.Payload.Status == string(backup.Success) {
				break
			}
			time.Sleep(time.Second * 1)
		}

		statusResp, err := helper.CreateBackupStatus(t, backend, backupID)
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
		_, err := helper.RestoreBackup(t, className, backend, backupID)
		require.Nil(t, err, "expected nil, got: %v", err)

		// wait for restore success
		restoreTime := time.Now()
		for {
			if time.Now().After(restoreTime.Add(21 * time.Second)) {
				break
			}

			resp, err := helper.RestoreBackupStatus(t, backend, backupID)
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

		statusResp, err := helper.RestoreBackupStatus(t, backend, backupID)
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
