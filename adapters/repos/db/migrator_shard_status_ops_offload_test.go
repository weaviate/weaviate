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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
)

// failingOffloadCloud is an OffloadCloud whose Upload always fails.
type failingOffloadCloud struct{ uploadErr error }

func (f *failingOffloadCloud) VerifyBucket(context.Context) error { return nil }

func (f *failingOffloadCloud) Upload(context.Context, string, string, string) error {
	return f.uploadErr
}

func (f *failingOffloadCloud) Download(context.Context, string, string, string) error { return nil }

func (f *failingOffloadCloud) Delete(context.Context, string, string, string) error { return nil }

// recordingProcessor captures the RAFT command freeze produces.
type recordingProcessor struct {
	mu  sync.Mutex
	req *command.TenantProcessRequest
}

func (p *recordingProcessor) UpdateTenantsProcess(_ context.Context, _ string, req *command.TenantProcessRequest) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.req = req
	return 0, nil
}

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
	m.freeze(ctx, idx, class, []string{s.name}, ec)

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
