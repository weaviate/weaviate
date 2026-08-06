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
	esync "github.com/weaviate/weaviate/entities/sync"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// failingOffloadCloud is an OffloadCloud whose Upload and Download panic with
// uploadPanic/downloadPanic when those fields are set, and otherwise fail with
// uploadErr/downloadErr.
type failingOffloadCloud struct {
	uploadErr     error
	uploadPanic   string
	downloadErr   error
	downloadPanic string
}

func (f *failingOffloadCloud) VerifyBucket(context.Context) error { return nil }

func (f *failingOffloadCloud) Upload(context.Context, string, string, string) error {
	if f.uploadPanic != "" {
		panic(f.uploadPanic)
	}
	return f.uploadErr
}

func (f *failingOffloadCloud) Download(context.Context, string, string, string) error {
	if f.downloadPanic != "" {
		panic(f.downloadPanic)
	}
	return f.downloadErr
}

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

// TestFreezeAbortReportsPreFreezeStatus: a tenant can be HOT in the schema with no local
// shard, e.g. after an activation whose shard load failed. Reporting COLD for it on an
// aborted freeze deactivates it with no user action, so the reported status must be the
// one the schema handed over.
func TestFreezeAbortReportsPreFreezeStatus(t *testing.T) {
	const class = "FreezeAbortStatus"

	cases := []struct {
		name  string
		given []*schemaUC.UpdateTenantPayload
		want  map[string]string // tenant name -> reported abort status
	}{
		{
			name:  "HOT is reported back",
			given: []*schemaUC.UpdateTenantPayload{{Name: "t1", PreFreezeStatus: models.TenantActivityStatusHOT}},
			want:  map[string]string{"t1": models.TenantActivityStatusHOT},
		},
		{
			name:  "COLD is reported back",
			given: []*schemaUC.UpdateTenantPayload{{Name: "t1", PreFreezeStatus: models.TenantActivityStatusCOLD}},
			want:  map[string]string{"t1": models.TenantActivityStatusCOLD},
		},
		{
			name:  "a missing record falls back to HOT",
			given: []*schemaUC.UpdateTenantPayload{{Name: "t1", PreFreezeStatus: ""}},
			want:  map[string]string{"t1": models.TenantActivityStatusHOT},
		},
		{
			name: "each tenant of a batch keeps its own status",
			given: []*schemaUC.UpdateTenantPayload{
				{Name: "t1", PreFreezeStatus: models.TenantActivityStatusHOT},
				{Name: "t2", PreFreezeStatus: models.TenantActivityStatusCOLD},
				{Name: "t3", PreFreezeStatus: ""},
			},
			want: map[string]string{
				"t1": models.TenantActivityStatusHOT,
				"t2": models.TenantActivityStatusCOLD,
				"t3": models.TenantActivityStatusHOT,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			// no shard is stored, so freeze takes the "not resident on this node" path
			idx := &Index{
				logger:           logger,
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
			}

			m := NewMigrator(nil, logger, "node1")
			m.SetNode("node1")
			proc := &recordingProcessor{}
			m.SetCluster(proc)
			m.cloud = &failingOffloadCloud{uploadErr: fmt.Errorf("simulated upload failure")}

			// freeze fans out per tenant, so it needs the concurrency-safe compounder
			// its production caller passes
			ec := errorcompounder.NewSafe()
			m.freeze(context.Background(), idx, class, tc.given, ec)
			require.Error(t, ec.ToError(), "the upload error must be recorded")

			require.Eventually(t, func() bool {
				proc.mu.Lock()
				defer proc.mu.Unlock()
				return proc.req != nil
			}, 5*time.Second, 10*time.Millisecond, "freeze must report the abort")

			proc.mu.Lock()
			defer proc.mu.Unlock()
			require.Len(t, proc.req.TenantsProcesses, len(tc.want))
			for _, tp := range proc.req.TenantsProcesses {
				require.Equal(t, command.TenantsProcess_OP_ABORT, tp.Op)
				require.Equal(t, tc.want[tp.Tenant.Name], tp.Tenant.Status, "tenant %q", tp.Tenant.Name)
			}
		})
	}
}

// TestFreezeAbortOnPanicReportsPreFreezeStatus: the abort a recovered panic synthesises
// fills the same slot as the reporting paths above, so it must read the same pre-freeze
// status. Reporting a hardcoded HOT here would reactivate a COLD tenant.
func TestFreezeAbortOnPanicReportsPreFreezeStatus(t *testing.T) {
	// The synthesised abort exists only on the recovered-panic path, the production
	// posture. The CI suite exports DISABLE_RECOVERY_ON_PANIC=true globally
	// (test/integration/run.sh) and the error-group wrapper reads it at recover time,
	// so without this pin the injected panic kills the whole test binary.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	logger, _ := test.NewNullLogger()
	// no shard is stored, so the panic is the only thing that can fill the slot
	idx := &Index{
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
	}

	m := NewMigrator(nil, logger, "node1")
	m.SetNode("node1")
	proc := &recordingProcessor{}
	m.SetCluster(proc)
	m.cloud = &failingOffloadCloud{uploadPanic: "simulated upload panic"}

	ec := errorcompounder.NewSafe()
	m.freeze(context.Background(), idx, "FreezeAbortPanicStatus", []*schemaUC.UpdateTenantPayload{
		{Name: "t1", PreFreezeStatus: models.TenantActivityStatusCOLD},
	}, ec)
	require.ErrorContains(t, ec.ToError(), "panic occurred",
		"the discarded group error is what made this silent")

	require.Eventually(t, func() bool {
		proc.mu.Lock()
		defer proc.mu.Unlock()
		return proc.req != nil
	}, 5*time.Second, 10*time.Millisecond, "a panicking freeze must report the abort")

	proc.mu.Lock()
	defer proc.mu.Unlock()
	require.Len(t, proc.req.TenantsProcesses, 1)
	tp := proc.req.TenantsProcesses[0]
	require.Equal(t, command.TenantsProcess_OP_ABORT, tp.Op)
	require.Equal(t, models.TenantActivityStatusCOLD, tp.Tenant.Status)
}

// TestUnfreezeAlwaysFillsItsCommandSlot: unfreeze pre-sizes its command slice and its
// workers fill it by index, so a worker that dies leaves a nil slot. That slot
// round-trips as a message with a nil Tenant, which every node's FSM skips: the
// pre-recorded OP_START stands and the tenant stays UNFREEZING forever, with no
// watchdog covering it. Every way a worker can end must therefore leave an abort.
//
// The abort carries no status: on ACTION_UNFREEZING the FSM fills it from the
// process it recorded when the unfreeze started (findRequestedStatus).
func TestUnfreezeAlwaysFillsItsCommandSlot(t *testing.T) {
	const class = "UnfreezeAbortSlot"

	cases := []struct {
		name       string
		tenant     string // the "tenant#node" entry the FSM hands the DB layer
		cloud      *failingOffloadCloud
		wantName   string
		wantErrMsg string
	}{
		{
			name:       "a recovered panic still reports the abort",
			tenant:     "t1#node1",
			cloud:      &failingOffloadCloud{downloadPanic: "simulated download panic"},
			wantName:   "t1",
			wantErrMsg: "panic occurred",
		},
		{
			name:       "a download failure reports the abort",
			tenant:     "t1#node1",
			cloud:      &failingOffloadCloud{downloadErr: fmt.Errorf("simulated download failure")},
			wantName:   "t1",
			wantErrMsg: "downloading error",
		},
		{
			name:       "an entry without a node name reports the abort",
			tenant:     "nodeless",
			cloud:      &failingOffloadCloud{},
			wantName:   "nodeless",
			wantErrMsg: "can't detect the old node name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The synthesised abort exists only on the recovered-panic path, the
			// production posture. The CI suite exports DISABLE_RECOVERY_ON_PANIC=true
			// globally (test/integration/run.sh) and the error-group wrapper reads it at
			// recover time, so without this pin the injected panic kills the whole test
			// binary.
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			logger, _ := test.NewNullLogger()
			// unfreeze never touches a shard, so an index with only its locks suffices
			idx := &Index{
				logger:           logger,
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
			}

			m := NewMigrator(nil, logger, "node1")
			m.SetNode("node1")
			proc := &recordingProcessor{}
			m.SetCluster(proc)
			m.cloud = tc.cloud

			// unfreeze fans out per tenant, so it needs the concurrency-safe compounder
			// its production caller passes
			ec := errorcompounder.NewSafe()
			m.unfreeze(context.Background(), idx, class, []string{tc.tenant}, ec)

			require.ErrorContains(t, ec.ToError(), tc.wantErrMsg)
			require.Equal(t, 1, ec.Len(),
				"each failure is reported once: the workers report through ec and the group error carries only panics")

			require.Eventually(t, func() bool {
				proc.mu.Lock()
				defer proc.mu.Unlock()
				return proc.req != nil
			}, 5*time.Second, 10*time.Millisecond, "unfreeze must report the abort")

			proc.mu.Lock()
			defer proc.mu.Unlock()
			require.Len(t, proc.req.TenantsProcesses, 1)
			tp := proc.req.TenantsProcesses[0]
			require.NotNil(t, tp, "a nil slot is skipped by every node's FSM")
			require.Equal(t, command.TenantsProcess_OP_ABORT, tp.Op)
			require.Equal(t, tc.wantName, tp.Tenant.Name)
			require.Empty(t, tp.Tenant.Status)
		})
	}
}
