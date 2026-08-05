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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestFreezeAbortRestoresShardOnUploadFailure: a freeze whose Upload fails must fully restore the shard.
func TestFreezeAbortRestoresShardOnUploadFailure(t *testing.T) {
	ctx := context.Background()
	const class = "FreezeAbortRestoresShard"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	cfg := minAsyncReplicationConfig()
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	logger, _ := test.NewNullLogger()
	m := NewMigrator(nil, logger, "node1")
	m.SetNode("node1")
	proc := &recordingProcessor{}
	m.SetCluster(proc)
	m.cloud = &failingOffloadCloud{uploadErr: fmt.Errorf("simulated upload failure")}

	// Planted as a pre-fix binary could leave it; the abort must discard it.
	stale := filepath.Join(s.pathHashTree(), "hashtree-0000000000000001.ht")
	require.NoError(t, os.WriteFile(stale, []byte("stale snapshot"), 0o600))

	ec := errorcompounder.New()
	m.freeze(ctx, idx, class, []*schemaUC.UpdateTenantPayload{
		{Name: s.name, PreFreezeStatus: models.TenantActivityStatusHOT},
	}, ec)

	require.Equal(t, 0, s.haltForTransferCount, "freeze abort must resume maintenance")
	require.Empty(t, htFilesInDir(t, s.pathHashTree()), "freeze abort must discard the stale snapshot")
	awaitHashtreeInitialized(t, s)
	require.Error(t, ec.ToError(), "the upload error must be recorded")

	require.Eventually(t, func() bool {
		proc.mu.Lock()
		defer proc.mu.Unlock()
		if proc.req == nil || len(proc.req.TenantsProcesses) != 1 {
			return false
		}
		tp := proc.req.TenantsProcesses[0]
		return tp.Op == command.TenantsProcess_OP_ABORT && tp.Tenant.Status == models.TenantActivityStatusHOT
	}, 5*time.Second, 20*time.Millisecond, "freeze must record OP_ABORT back to HOT")
}
