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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func CancelFromRestartJourney(t *testing.T, cluster *docker.DockerCompose, nodeName, backend string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	nodeStopTimeout := 10 * time.Second
	className := "CancelClass"
	backupID := fmt.Sprintf("%s-backup-cancel", backend)
	numObjects := 20_000
	batchSize := 200

	t.Run("create class", func(t *testing.T) {
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "textProp", DataType: []string{"text"}},
			},
		})
	})

	t.Run("import data", func(t *testing.T) {
		for i := 0; i < numObjects; i += batchSize {
			batch := make([]*models.Object, batchSize)
			for j := range batch {
				batch[j] = &models.Object{
					Class: className,
					Properties: map[string]interface{}{
						"textProp": fmt.Sprintf("object-%d", i+j),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
		}
	})

	t.Run("create backup", func(t *testing.T) {
		resp, err := helper.CreateBackup(t, &models.BackupConfig{}, className, backend, backupID)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Status)
		require.Equal(t, string(backup.Started), *resp.Payload.Status)
	})

	t.Run("restart node mid-backup", func(t *testing.T) {
		err := cluster.Stop(ctx, nodeName, &nodeStopTimeout)
		require.Nil(t, err)
		err = cluster.Start(ctx, nodeName)
		require.Nil(t, err)
		helper.SetupClient(cluster.GetWeaviate().URI())
	})

	t.Run("validate that backup was set to CANCELED on restart", func(t *testing.T) {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Status)
		require.Equal(t, string(backup.Cancelled), *resp.Payload.Status)
	})
}
