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

package backup_dedupe_replicas_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/helper"
)

// TestBackupDedupeCancelViaNonCoordinator pins non-coordinator DELETE cancelling a planning create.
func TestBackupDedupeCancelViaNonCoordinator(t *testing.T) {
	ctx := context.Background()

	compose := startDedupeCluster(ctx, t)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	host := compose.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()
	defer dumpNodeLogs(t, compose)

	const (
		className = "DedupeCancelNonCoord"
		backupID  = "dedupe-cancel-noncoord-1"
	)
	helper.CreateClass(t, newReplicatedClass(className))
	seedObjects(t, host, className, 500)
	shards := common.DiscoverShards(t, host, className)
	require.NotEmpty(t, shards)
	waitForCheckpointCapability(t, compose, className, shards)

	errCh := make(chan error, 1)
	go func() {
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		errCh <- err
	}()

	time.Sleep(3 * time.Second)
	cancelURL := fmt.Sprintf("http://%s/v1/backups/%s/%s", compose.GetWeaviateNode(3).URI(), backendS3, backupID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, cancelURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	select {
	case err := <-errCh:
		require.Error(t, err, "create must not report success after an acknowledged cancel")
	case <-time.After(60 * time.Second):
		t.Fatal("create still blocked 60s after the cancel was acknowledged")
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, err := helper.CreateBackupStatus(t, backendS3, backupID, "", "")
		require.NoError(ct, err)
		require.NotNil(ct, status.Payload)
		require.NotNil(ct, status.Payload.Status)
		require.Equal(ct, string(entbackup.Cancelled), *status.Payload.Status)
	}, 30*time.Second, time.Second)
}
