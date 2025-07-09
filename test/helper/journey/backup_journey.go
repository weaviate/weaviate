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
	"fmt"
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

func backupJourney(t *testing.T, className, backend, basebackupID string,
	journeyType journeyType, dataIntegrityCheck dataIntegrityCheck,
	tenantNames []string, pqEnabled bool, nodeMapping map[string]string,
	override bool, overrideBucket, overridePath string,
) {
	backupID := basebackupID
	if override {
		backupID = fmt.Sprintf("%s_%s", backupID, overrideBucket)
	}

	overrideString := ""

	if override {
		overrideString = fmt.Sprintf(" with override bucket: %s, path: %s", overrideBucket, overridePath)
	}

	if journeyType == clusterJourney && backend == "filesystem" {
		t.Run("should fail backup/restore with local filesystem backend"+overrideString, func(t *testing.T) {
			backupResp, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backend, backupID)
			assert.Nil(t, backupResp)
			assert.Error(t, err)

			restoreResp, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, backupID, map[string]string{})
			assert.Nil(t, restoreResp)
			assert.Error(t, err)
		})
		return
	}

	t.Run("create backup"+overrideString, func(t *testing.T) {
		// Ensure cluster is in sync
		if journeyType == clusterJourney {
			time.Sleep(3 * time.Second)
		}

		cfg := helper.DefaultBackupConfig()

		if override {
			cfg.Bucket = overrideBucket
			cfg.Path = overridePath
		}

		assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
			resp, err := helper.CreateBackup(t, cfg, className, backend, backupID)
			helper.AssertRequestOk(t, resp, err, nil)
			assert.Equal(t1, cfg.Bucket, resp.Payload.Bucket)
			if cfg.Bucket != "" {
				assert.Contains(t, resp.Payload.Path, cfg.Bucket)
			}
			if cfg.Path != "" {
				assert.Contains(t, resp.Payload.Path, cfg.Path)
			}
			assert.Equal(t1, backupID, resp.Payload.ID)
			assert.Equal(t1, className, resp.Payload.Classes[0])
			assert.Equal(t1, "", resp.Payload.Error)
			assert.Equal(t1, string(backup.Started), *resp.Payload.Status)
		}, 240*time.Second, 500*time.Millisecond)

		assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
			resp, err := helper.CreateBackupStatus(t, backend, backupID, overrideBucket, overridePath)
			assert.Nil(t, err, "expected nil, got: %v", err)

			assert.NotNil(t1, resp)
			assert.NotNil(t1, resp.Payload)
			assert.NotNil(t1, resp.Payload.Status)
			assert.Equal(t1, backupID, resp.Payload.ID)
			assert.Equal(t1, backend, resp.Payload.Backend)
			assert.Contains(t1, resp.Payload.Path, overrideBucket)
			assert.Contains(t1, resp.Payload.Path, overridePath)

			assert.True(t1, (*resp.Payload.Status == "STARTED") || (*resp.Payload.Status == "SUCCESS"))
		}, 120*time.Second, 1000*time.Millisecond)

		assert.EventuallyWithT(t, func(t1 *assert.CollectT) {
			statusResp, err := helper.CreateBackupStatus(t, backend, backupID, overrideBucket, overridePath)

			helper.AssertRequestOk(t, statusResp, err, func() {
				assert.NotNil(t1, statusResp)
				assert.NotNil(t1, statusResp.Payload)
				assert.NotNil(t1, statusResp.Payload.Status)
				assert.Equal(t1, backupID, statusResp.Payload.ID)
				assert.Equal(t1, backend, statusResp.Payload.Backend)
				assert.Contains(t1, statusResp.Payload.Path, overrideBucket)
				assert.Contains(t1, statusResp.Payload.Path, overridePath)
			})

			assert.Equal(t1, string(backup.Success), *statusResp.Payload.Status,
				statusResp.Payload.Error)
		}, 120*time.Second, 1000*time.Millisecond)
	})

	t.Run("delete class for restoration"+overrideString, func(t *testing.T) {
		helper.DeleteClass(t, className)
		time.Sleep(time.Second)
	})

	t.Run("restore backup"+overrideString, func(t *testing.T) {
		cfg := helper.DefaultRestoreConfig()

		if override {
			cfg.Bucket = overrideBucket
			cfg.Path = overridePath
		}

		t.Logf("cfg: %+v, className: %s, backend: %s, backupID: %s, nodeMapping: %+v\n", cfg, className, backend, backupID, nodeMapping)
		resp, err := helper.RestoreBackup(t, cfg, className, backend, backupID, nodeMapping)
		require.Nil(t, err, "expected nil, got: %v", err)
		assert.Equal(t, backupID, resp.Payload.ID)
		assert.Equal(t, backend, resp.Payload.Backend)
		assert.Contains(t, resp.Payload.Path, overrideBucket)
		assert.Contains(t, resp.Payload.Path, overridePath)

		// wait for restore success
		ticker := time.NewTicker(90 * time.Second)
	wait:
		for {
			select {
			case <-ticker.C:
				break wait
			default:
				resp, err := helper.RestoreBackupStatus(t, backend, backupID, overrideBucket, overridePath)
				helper.AssertRequestOk(t, resp, err, func() {
					require.NotNil(t, resp)
					require.NotNil(t, resp.Payload)
					require.NotNil(t, resp.Payload.Status)
					assert.Equal(t, backupID, resp.Payload.ID)
					assert.Equal(t, backend, resp.Payload.Backend)
					assert.Contains(t, resp.Payload.Path, overrideBucket)
					assert.Contains(t, resp.Payload.Path, overridePath)
				})

				if *resp.Payload.Status == string(backup.Success) {
					break wait
				}
				time.Sleep(1 * time.Second)
			}
		}

		statusResp, err := helper.RestoreBackupStatus(t, backend, backupID, overrideBucket, overridePath)
		helper.AssertRequestOk(t, statusResp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
			assert.Equal(t, backupID, resp.Payload.ID)
			assert.Equal(t, backend, resp.Payload.Backend)
			assert.Contains(t, resp.Payload.Path, overrideBucket)
			assert.Contains(t, resp.Payload.Path, overridePath)
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

func backupJourneyWithCancellation(t *testing.T, className, backend, basebackupID string, journeyType journeyType, overrideBucket, overridePath string) {
	backupID := basebackupID
	if overridePath != "" {
		backupID = fmt.Sprintf("%s_%s", backupID, overrideBucket)
	}
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

	t.Run("create and cancel backup", func(t *testing.T) {
		// Ensure cluster is in sync
		if journeyType == clusterJourney {
			time.Sleep(5 * time.Second)
		}
		cfg := helper.DefaultBackupConfig()
		cfg.Bucket = overrideBucket
		cfg.Path = overridePath

		resp, err := helper.CreateBackup(t, cfg, className, backend, backupID)
		helper.AssertRequestOk(t, resp, err, nil)

		t.Run("cancel backup", func(t *testing.T) {
			require.Nil(t, helper.CancelBackup(t, backend, backupID))
		})

		// wait for cancellation
		ticker := time.NewTicker(20 * time.Second)
	wait:
		for {
			select {
			case <-ticker.C:
				break wait
			default:
				statusResp, err := helper.CreateBackupStatus(t, backend, backupID, overrideBucket, overridePath)
				helper.AssertRequestOk(t, statusResp, err, func() {
					require.NotNil(t, statusResp)
					require.NotNil(t, statusResp.Payload)
					require.NotNil(t, statusResp.Payload.Status)
				})

				if *statusResp.Payload.Status == string(backup.Cancelled) {
					break wait
				}
				time.Sleep(500 * time.Millisecond)
			}
		}

		statusResp, err := helper.CreateBackupStatus(t, backend, backupID, overrideBucket, overridePath)
		helper.AssertRequestOk(t, statusResp, err, func() {
			require.NotNil(t, statusResp)
			require.NotNil(t, statusResp.Payload)
			require.NotNil(t, statusResp.Payload.Status)
			require.Equal(t, string(backup.Cancelled), *statusResp.Payload.Status)
		})
	})
}

func backupJourneyWithListing(t *testing.T, journeyType journeyType, className, backend, backupID string, overrideBucket, overridePath string) {
	if journeyType == clusterJourney && backend == "filesystem" || overrideBucket != "" {
		return
	}
	if overridePath != "" {
		backupID = fmt.Sprintf("%s_%s", backupID, overrideBucket)
	}
	// Create a backup first
	cfg := helper.DefaultBackupConfig()
	if overrideBucket != "" {
		cfg.Bucket = overrideBucket
		cfg.Path = overridePath
	}
	resp, err := helper.CreateBackup(t, cfg, className, backend, fmt.Sprintf("%s_for_listing", backupID))
	helper.AssertRequestOk(t, resp, err, nil)

	// Wait for backup to complete
	ticker := time.NewTicker(90 * time.Second)
wait:
	for {
		select {
		case <-ticker.C:
			break wait
		default:
			resp, err := helper.CreateBackupStatus(t, backend, fmt.Sprintf("%s_for_listing", backupID), overrideBucket, overridePath)
			helper.AssertRequestOk(t, resp, err, nil)
			if *resp.Payload.Status == string(backup.Success) {
				break wait
			}
			time.Sleep(1 * time.Second)
		}
	}

	// List backups and verify
	listResp, err := helper.ListBackup(t, backend)
	helper.AssertRequestOk(t, listResp, err, func() {
		require.NotNil(t, listResp)
		require.NotNil(t, listResp.Payload)
		// Verify that our backup is in the list
		found := false
		for _, b := range listResp.Payload {
			if b.ID == fmt.Sprintf("%s_for_listing", backupID) {
				found = true
				assert.Equal(t, string(backup.Success), b.Status)
				assert.Contains(t, b.Classes, className)
				break
			}
		}
		assert.True(t, found, "backup not found in list")
	})
}

func addTestClass(t *testing.T, className string, multiTenant bool) {
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
