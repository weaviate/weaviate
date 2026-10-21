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
	require.NoError(t, os.MkdirAll(s.pathHashTree(), os.ModePerm))
	stale := filepath.Join(s.pathHashTree(), "hashtree-0000000000000001.ht")
	require.NoError(t, os.WriteFile(stale, []byte("stale snapshot"), 0o600))

	ec := errorcompounder.New()
	m.freeze(ctx, idx, class, []*schemaUC.UpdateTenantPayload{
		{Name: s.name, PreFreezeStatus: models.TenantActivityStatusHOT},
	}, ec)

	require.EqualValues(t, 0, s.haltForTransferCount.Load(), "freeze abort must resume maintenance")
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

// plantingOffloadCloud downloads an artifact that carries a .ht (frozen-from-COLD tenants and pre-fix uploads), then fails with err if set.
type plantingOffloadCloud struct {
	dir   string
	plant bool
	err   error
}

func (c *plantingOffloadCloud) VerifyBucket(context.Context) error { return nil }

func (c *plantingOffloadCloud) Upload(context.Context, string, string, string) error { return nil }

func (c *plantingOffloadCloud) Download(context.Context, string, string, string) error {
	if c.plant {
		if err := os.MkdirAll(c.dir, os.ModePerm); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(c.dir, "hashtree-0000000000000001.ht"), []byte("stale snapshot"), 0o600); err != nil {
			return err
		}
	}
	return c.err
}

func (c *plantingOffloadCloud) Delete(context.Context, string, string, string) error { return nil }

// TestUnfreezeDiscardsDownloadedHashtree: activation trusts a .ht verbatim, so none may survive the download on either leg.
// A partial download that keeps its .ht reports the full root while missing data; the next dedupe backup would archive it alone.
func TestUnfreezeDiscardsDownloadedHashtree(t *testing.T) {
	cases := []struct {
		name       string
		plant      bool
		preexist   bool
		downloadEr error
		wantOp     command.TenantsProcess_Op
	}{
		{name: "success leg discards the downloaded snapshot", plant: true, wantOp: command.TenantsProcess_OP_DONE},
		{name: "abort leg discards the downloaded snapshot", plant: true, downloadEr: fmt.Errorf("simulated download failure"), wantOp: command.TenantsProcess_OP_ABORT},
		{name: "abort leg discards a snapshot left by an earlier attempt", preexist: true, downloadEr: fmt.Errorf("simulated download failure"), wantOp: command.TenantsProcess_OP_ABORT},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			class := fmt.Sprintf("UnfreezeDiscardsHashtree%d", i)

			sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
			s := concreteShard(t, sl)
			t.Cleanup(func() { require.NoError(t, sl.Shutdown(context.Background())) })
			if tc.preexist {
				require.NoError(t, os.MkdirAll(s.pathHashTree(), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(s.pathHashTree(), "hashtree-0000000000000002.ht"), []byte("stale snapshot"), 0o600))
			}

			logger, _ := test.NewNullLogger()
			m := NewMigrator(nil, logger, "node1")
			m.SetNode("node1")
			proc := &recordingProcessor{}
			m.SetCluster(proc)
			m.cloud = &plantingOffloadCloud{dir: s.pathHashTree(), plant: tc.plant, err: tc.downloadEr}

			ec := errorcompounder.New()
			m.unfreeze(ctx, idx, class, []string{s.name + "#node1"}, ec)

			if tc.downloadEr != nil {
				require.Error(t, ec.ToError())
			} else {
				require.NoError(t, ec.ToError())
			}
			require.Empty(t, htFilesInDir(t, s.pathHashTree()))
			require.Eventually(t, func() bool {
				proc.mu.Lock()
				defer proc.mu.Unlock()
				return proc.req != nil && len(proc.req.TenantsProcesses) == 1 &&
					proc.req.TenantsProcesses[0].Op == tc.wantOp
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}
