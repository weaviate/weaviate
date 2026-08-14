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
