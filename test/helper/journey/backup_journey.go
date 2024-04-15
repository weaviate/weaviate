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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

type journeyType int

const (
	singleNodeJourney journeyType = iota
	clusterJourney
)

type dataIntegrityCheck int

const (
	checkClassPresenceOnly = iota
	checkClassAndDataPresence
)

const (
	singleTenant = ""
	multiTenant  = true
)

func backupJourney(t *testing.T, className, backend, backupID string,
	journeyType journeyType, dataIntegrityCheck dataIntegrityCheck,
	tenantNames []string, pqEnabled bool, nodeMapping map[string]string,
) {
	if journeyType == clusterJourney && backend == "filesystem" {
		t.Run("should fail backup/restore with local filesystem backend", func(t *testing.T) {
			backupResp, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backend, backupID)
			assert.Nil(t, backupResp)
			assert.Error(t, err)

			restoreResp, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, backupID, map[string]string{})
			assert.Nil(t, restoreResp)
			assert.Error(t, err)
		})
		return
	}

	t.Run("create backup", func(t *testing.T) {
		// Ensure cluster is in sync
		if journeyType == clusterJourney {
			time.Sleep(3 * time.Second)
		}
		resp, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backend, backupID)
		helper.AssertRequestOk(t, resp, err, nil)

		// wait for create success
		ticker := time.NewTicker(90 * time.Second)

	wait:
		for {
			select {
			case <-ticker.C:
				break wait
			default:
				resp, err := helper.CreateBackupStatus(t, backend, backupID)
				helper.AssertRequestOk(t, resp, err, func() {
					require.NotNil(t, resp)
					require.NotNil(t, resp.Payload)
					require.NotNil(t, resp.Payload.Status)
				})

				if *resp.Payload.Status == string(backup.Success) {
					break wait
				}
				time.Sleep(1 * time.Second)
			}
		}

		statusResp, err := helper.CreateBackupStatus(t, backend, backupID)
		helper.AssertRequestOk(t, resp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
		})

		require.Equal(t, *statusResp.Payload.Status,
			string(backup.Success), statusResp.Payload.Error)
	})

	t.Run("delete class for restoration", func(t *testing.T) {
		helper.DeleteClass(t, className)
		time.Sleep(time.Second)
	})

	t.Run("restore backup", func(t *testing.T) {
		_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, backupID, nodeMapping)
		require.Nil(t, err, "expected nil, got: %v", err)

		// wait for restore success
		ticker := time.NewTicker(90 * time.Second)
	wait:
		for {
			select {
			case <-ticker.C:
				break wait
			default:
				resp, err := helper.RestoreBackupStatus(t, backend, backupID)
				helper.AssertRequestOk(t, resp, err, func() {
					require.NotNil(t, resp)
					require.NotNil(t, resp.Payload)
					require.NotNil(t, resp.Payload.Status)
				})

				if *resp.Payload.Status == string(backup.Success) {
					break wait
				}
				time.Sleep(1 * time.Second)
			}
		}

		statusResp, err := helper.RestoreBackupStatus(t, backend, backupID)
		helper.AssertRequestOk(t, statusResp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
		})

		require.Equal(t, string(backup.Success), *statusResp.Payload.Status)
	})

	// Ensure that on restoring the class it is consistent on the followers
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		if tenantNames != nil {
			for _, name := range tenantNames {
				moduleshelper.EnsureClassExists(t, className, name)
				if dataIntegrityCheck == checkClassAndDataPresence {
					count := moduleshelper.GetClassCount(t, className, name)
					assert.Equal(t, int64(500/len(tenantNames)), count)
				}
			}
		} else {
			moduleshelper.EnsureClassExists(t, className, singleTenant)
			if dataIntegrityCheck == checkClassAndDataPresence {
				count := moduleshelper.GetClassCount(t, className, singleTenant)
				assert.Equal(t, int64(500), count)
				if pqEnabled {
					moduleshelper.EnsureCompressedVectorsRestored(t, className)
				}
			}
		}
	}, 5*time.Second, 500*time.Microsecond, "class doesn't exists in follower nodes")
}

func addTestClass(t *testing.T, className string, multiTenant bool) {
	// TODO shall be removed with the DB is idempotent
	// delete class before trying to create in case it was existing.
	helper.DeleteClass(t, className)

	class := &models.Class{
		Class: className,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "contents",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	if multiTenant {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
	}

	helper.CreateClass(t, class)
}

func addTestObjects(t *testing.T, className string, tenantNames []string) {
	const (
		noteLengthMin = 4
		noteLengthMax = 1024

		batchSize  = 10
		numBatches = 50
	)

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	multiTenant := len(tenantNames) > 0

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			contentsLength := noteLengthMin + seededRand.Intn(noteLengthMax-noteLengthMin+1)
			contents := helper.GetRandomString(contentsLength)

			obj := models.Object{
				Class:      className,
				Properties: map[string]interface{}{"contents": contents},
			}
			if multiTenant {
				obj.Tenant = tenantNames[i]
			}
			batch[j] = &obj
		}
		helper.CreateObjectsBatch(t, batch)
	}
}
