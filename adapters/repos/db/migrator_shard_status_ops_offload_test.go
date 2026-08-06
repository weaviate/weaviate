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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	entbackup "github.com/weaviate/weaviate/entities/backup"
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

	ec := errorcompounder.New()
	m.freeze(ctx, idx, class, []*schemaUC.UpdateTenantPayload{
		{Name: s.name, PreFreezeStatus: models.TenantActivityStatusHOT},
	}, ec)

	requireTotal(t, s, 0, "freeze abort must resume maintenance")
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

// A freeze goroutine that panics after the halt must abort like the upload-failure
// path: every node's FSM skips a nil slot, leaving the pre-recorded OP_START
// standing, the tenant FREEZING, and this node's halt leaked with no watchdog armed.
func TestFreezeAbortRestoresShardOnUploadPanic(t *testing.T) {
	// The abort exists only on the recovered-panic path, the production posture. The
	// CI suite exports DISABLE_RECOVERY_ON_PANIC=true globally
	// (test/integration/run.sh) and the error-group wrapper reads it at recover time,
	// so without this pin the injected panic kills the whole test binary.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	ctx := context.Background()
	const class = "FreezeAbortPanicRestoresShard"

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
	m.cloud = &failingOffloadCloud{uploadPanic: "simulated upload panic"}

	ec := errorcompounder.New()
	m.freeze(ctx, idx, class, []*schemaUC.UpdateTenantPayload{
		{Name: s.name, PreFreezeStatus: models.TenantActivityStatusHOT},
	}, ec)

	requireTotal(t, s, 0, "a panicking freeze must lift its own halt")
	_, err := s.ListBackupFiles(ctx, &entbackup.ShardDescriptor{})
	require.ErrorContains(t, err, "not paused for transfer",
		"compaction must be resumed after the panicking freeze")
	require.ErrorContains(t, ec.ToError(), "panic occurred",
		"the discarded group error is what made this silent")

	require.Eventually(t, func() bool {
		proc.mu.Lock()
		defer proc.mu.Unlock()
		if proc.req == nil || len(proc.req.TenantsProcesses) != 1 {
			return false
		}
		tp := proc.req.TenantsProcesses[0]
		return tp.Op == command.TenantsProcess_OP_ABORT && tp.Tenant.Status == models.TenantActivityStatusHOT
	}, 5*time.Second, 20*time.Millisecond, "a panicking freeze must record OP_ABORT back to HOT")
}
