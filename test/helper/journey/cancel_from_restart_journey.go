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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func CancelFromRestartJourney(t *testing.T, cluster *docker.DockerCompose, nodeName, backend string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	nodeStopTimeout := 10 * time.Second
	className := "CancelClass"
	backupID := fmt.Sprintf("%s-backup-cancel", backend)
	numObjects := 20_000
	batchSize := 200

	t.Run("create class", func(t *testing.T) {
		start := time.Now()
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "textProp", DataType: []string{"text"}},
				{Name: "largeProp", DataType: []string{"text"}},
			},
		})
		t.Logf("Class created in %v", time.Since(start))
	})

	t.Run("import data", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < numObjects; i += batchSize {
			batch := make([]*models.Object, batchSize)
			for j := range batch {
				batch[j] = &models.Object{
					Class: className,
					Properties: map[string]interface{}{
						"textProp":  fmt.Sprintf("object-%d", i+j),
						"largeProp": randomString(5000), // 5KB of random data per object
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
		}
		t.Logf("Imported %d objects in %v", numObjects, time.Since(start))
	})

	var backupStartTime time.Time

	t.Run("create backup", func(t *testing.T) {
		backupStartTime = time.Now()
		t.Logf("→ Starting backup at %v", backupStartTime.Format("15:04:05.000"))

		resp, err := helper.CreateBackup(t, &models.BackupConfig{CPUPercentage: 1, ChunkSize: 32}, className, backend, backupID)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Status)
		require.Equal(t, string(backup.Started), *resp.Payload.Status)

		t.Logf("Backup started (status: %s) in %v", *resp.Payload.Status, time.Since(backupStartTime))
	})

	t.Run("restart node mid-backup", func(t *testing.T) {
		timeSinceBackupStart := time.Since(backupStartTime)
		t.Logf("Time since backup started: %v", timeSinceBackupStart)

		stopStart := time.Now()
		t.Logf("Stopping node at %v", stopStart.Format("15:04:05.000"))
		err := cluster.Stop(ctx, nodeName, &nodeStopTimeout)
		require.Nil(t, err)
		stopDuration := time.Since(stopStart)
		t.Logf("Node stopped in %v", stopDuration)

		startStart := time.Now()
		t.Logf("Starting node at %v", startStart.Format("15:04:05.000"))
		err = cluster.Start(ctx, nodeName)
		require.Nil(t, err)
		helper.SetupClient(cluster.GetWeaviate().URI())
		startDuration := time.Since(startStart)
		t.Logf("Node started in %v", startDuration)

		totalRestartTime := time.Since(stopStart)
		t.Logf("Total restart time: %v", totalRestartTime)
		t.Logf("Time elapsed since backup creation: %v", time.Since(backupStartTime))
	})

	t.Run("validate that backup was set to CANCELED on restart", func(t *testing.T) {
		checkTime := time.Now()
		t.Logf("Checking backup status at %v (%.1fs after backup started)",
			checkTime.Format("15:04:05.000"),
			time.Since(backupStartTime).Seconds())

		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Status)

		actualStatus := *resp.Payload.Status
		t.Logf("Backup status: %s", actualStatus)

		require.Equal(t, string(backup.Cancelled), actualStatus,
			"backup should be CANCELED, not %s (elapsed time: %v)", actualStatus, time.Since(backupStartTime))
	})
}
