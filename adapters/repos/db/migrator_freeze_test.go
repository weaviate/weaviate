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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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

type downloadingOffloadCloud struct {
	failingOffloadCloud
	download func(shardName, nodeName string) error
	deleted  chan struct{}
}

func (c *downloadingOffloadCloud) Download(_ context.Context, _, shardName, nodeName string) error {
	return c.download(shardName, nodeName)
}

func (c *downloadingOffloadCloud) Delete(context.Context, string, string, string) error {
	close(c.deleted)
	return nil
}

// Unfreezing onto a different node must adopt the freezing node's records,
// or the load serves an empty index.
func TestUnfreezeOntoAnotherNodeAdoptsTheFreezingNodesRecords(t *testing.T) {
	const class, tenant, frozenOn, node = "UnfreezeAdopt", "t1", "node9", "node1"
	logger, _ := test.NewNullLogger()
	idx := &Index{
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		schemaReader:     local.NewMockSchemaReader(t),
		Config:           IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName(class)},
	}
	lsmPath := shardPathLSM(idx.path(), tenant)
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	subject.Key.UnitID = MigrationUnitID(tenant, frozenOn)

	m := NewMigrator(nil, logger, node)
	m.SetNode(node)
	proc := &recordingProcessor{}
	m.SetCluster(proc)
	cloud := &downloadingOffloadCloud{deleted: make(chan struct{}), download: func(shardName, nodeName string) error {
		require.Equal(t, frozenOn, nodeName, "fixture: the download reads the freezing node's copy")
		if err := os.MkdirAll(lsmPath, 0o777); err != nil {
			return err
		}
		return NewMigrationRecordStore(lsmPath, logger).Put(newMigrationRecordAt(t, subject, MigrationStateSwapped))
	}}
	m.cloud = cloud

	ec := errorcompounder.NewSafe()
	m.unfreeze(context.Background(), idx, class, []string{tenant + "#" + frozenOn}, ec)
	require.NoError(t, ec.ToError())
	require.Eventually(t, func() bool {
		proc.mu.Lock()
		defer proc.mu.Unlock()
		return proc.req != nil
	}, 5*time.Second, 10*time.Millisecond, "unfreeze must report")
	select {
	case <-cloud.deleted:
		t.Fatal("deleted the freezing node's copy")
	case <-time.After(100 * time.Millisecond):
	}

	local := NewMigrationRecordStoreForUnit(lsmPath, MigrationUnitID(tenant, node), logger)
	require.NoError(t, local.Load())
	require.Len(t, local.Records(), 1, "the freezing node's record is this node's to finalize")
	require.Equal(t, MigrationStateSwapped, local.Records()[0].State())
}

type deleteRecordingCloud struct {
	mu      sync.Mutex
	deletes []string
	copies  map[string]bool
	entered chan struct{}
	block   chan struct{}
}

func (c *deleteRecordingCloud) VerifyBucket(context.Context) error { return nil }

func (c *deleteRecordingCloud) Upload(_ context.Context, class, tenant, node string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.copies == nil {
		c.copies = map[string]bool{}
	}
	c.copies[class+"/"+tenant+"/"+node] = true
	return nil
}

func (c *deleteRecordingCloud) has(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.copies[path]
}

func (c *deleteRecordingCloud) Download(context.Context, string, string, string) error { return nil }

func (c *deleteRecordingCloud) Delete(_ context.Context, class, tenant, node string) error {
	if c.block != nil {
		close(c.entered)
		<-c.block
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, class+"/"+tenant+"/"+node)
	delete(c.copies, class+"/"+tenant+"/"+node)
	return nil
}

func (c *deleteRecordingCloud) deleted() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deletes...)
}

type gatedProcessor struct {
	release chan struct{}
	called  chan struct{}
	err     error
}

func (p *gatedProcessor) UpdateTenantsProcess(context.Context, string, *command.TenantProcessRequest) (uint64, error) {
	close(p.called)
	<-p.release
	if p.err != nil {
		return 0, p.err
	}
	return unfreezeReportVersion, nil
}

const unfreezeReportVersion = uint64(7)

// unfreezeSchema holds the tenant's status; empty means the tenant is gone.
type unfreezeSchema struct {
	status  atomic.Value
	waitErr error
	readErr error
}

func newUnfreezeSchema(status string) *unfreezeSchema {
	s := &unfreezeSchema{}
	s.status.Store(status)
	return s
}

func (s *unfreezeSchema) reader(t *testing.T) *local.MockSchemaReader {
	r := local.NewMockSchemaReader(t)
	r.EXPECT().WaitForUpdate(mock.Anything, unfreezeReportVersion).Return(s.waitErr).Maybe()
	r.EXPECT().Read("C", false, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			if s.readErr != nil {
				return s.readErr
			}
			state := &sharding.State{Physical: map[string]sharding.Physical{}}
			if status := s.status.Load().(string); status != "" {
				state.Physical["t1"] = sharding.Physical{Name: "t1", Status: status}
			}
			return read(nil, state)
		}).Maybe()
	return r
}

func newUnfreezeMigrator(t *testing.T, schema *unfreezeSchema, proc processor, cloud *deleteRecordingCloud) (*Migrator, *Index) {
	logger, _ := test.NewNullLogger()
	idx := &Index{
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		schemaReader:     schema.reader(t),
	}
	idx.Config.RootPath = t.TempDir()

	m := NewMigrator(nil, logger, "node1")
	m.SetNode("node1")
	m.SetCluster(proc)
	m.cloud = cloud
	m.unfreezeSourceRecheck = 10 * time.Millisecond
	return m, idx
}

func TestUnfreezeDeletesSourceOnlyAfterDoneCommits(t *testing.T) {
	cases := []struct {
		name       string
		status     string
		commitErr  error
		waitErr    error
		readErr    error
		wantDelete bool
	}{
		{name: "deleted once DONE commits", status: models.TenantActivityStatusHOT, wantDelete: true},
		{name: "deleted for a COLD target", status: models.TenantActivityStatusCOLD, wantDelete: true},
		{name: "deleted when the tenant is gone", status: "", wantDelete: true},
		{name: "kept when DONE fails to commit", status: models.TenantActivityStatusHOT, commitErr: fmt.Errorf("leader lost")},
		{name: "kept when DONE does not apply locally", status: models.TenantActivityStatusHOT, waitErr: fmt.Errorf("timed out")},
		{name: "kept when the schema cannot be read", status: models.TenantActivityStatusHOT, readErr: fmt.Errorf("class gone")},
		{name: "kept when the tenant is freezing again", status: models.TenantActivityStatusFREEZING},
		{name: "kept when the tenant is frozen again", status: models.TenantActivityStatusFROZEN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := newUnfreezeSchema(tc.status)
			schema.waitErr = tc.waitErr
			schema.readErr = tc.readErr
			proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{}), err: tc.commitErr}
			cloud := &deleteRecordingCloud{}
			m, idx := newUnfreezeMigrator(t, schema, proc, cloud)

			m.unfreeze(context.Background(), idx, "C", []string{"t1#node1"}, errorcompounder.NewSafe())

			select {
			case <-proc.called:
			case <-time.After(5 * time.Second):
				t.Fatal("unfreeze did not report DONE")
			}
			require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
				100*time.Millisecond, 10*time.Millisecond, "source deleted before DONE committed")

			close(proc.release)
			if !tc.wantDelete {
				require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
					200*time.Millisecond, 10*time.Millisecond, "source must be kept")
				return
			}
			require.Eventually(t, func() bool { return len(cloud.deleted()) == 1 },
				5*time.Second, 10*time.Millisecond)
			require.Equal(t, []string{"C/t1/node1"}, cloud.deleted())
		})
	}
}

func TestUnfreezeSourceDeleteWaitsForRunningFreeze(t *testing.T) {
	schema := newUnfreezeSchema(models.TenantActivityStatusHOT)
	proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{})}
	cloud := &deleteRecordingCloud{}
	m, idx := newUnfreezeMigrator(t, schema, proc, cloud)

	// unfreeze takes this lock itself for the download
	m.unfreeze(context.Background(), idx, "C", []string{"t1#node1"}, errorcompounder.NewSafe())
	idx.shardCreateLocks.Lock("t1")
	close(proc.release)
	require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
		100*time.Millisecond, 10*time.Millisecond, "delete must wait for the running freeze")

	schema.status.Store(models.TenantActivityStatusFREEZING)
	idx.shardCreateLocks.Unlock("t1")
	require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
		200*time.Millisecond, 10*time.Millisecond, "delete must skip the re-frozen tenant")
}

func TestUnfreezeSourceDeleteHoldsOffFreezeNotLookups(t *testing.T) {
	schema := newUnfreezeSchema(models.TenantActivityStatusHOT)
	proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{})}
	cloud := &deleteRecordingCloud{entered: make(chan struct{}), block: make(chan struct{})}
	m, idx := newUnfreezeMigrator(t, schema, proc, cloud)
	close(proc.release)

	m.unfreeze(context.Background(), idx, "C", []string{"t1#node1"}, errorcompounder.NewSafe())
	select {
	case <-cloud.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not start")
	}

	lookup := idx.shardCreateLocks.TryRLock("t1")
	require.True(t, lookup, "a shard lookup must not wait for the delete")
	idx.shardCreateLocks.RUnlock("t1")

	var freezeStarted atomic.Bool
	freezeDone := make(chan struct{})
	go func() {
		defer close(freezeDone)
		idx.shardCreateLocks.Lock("t1")
		defer idx.shardCreateLocks.Unlock("t1")
		freezeStarted.Store(true)
	}()
	require.Never(t, freezeStarted.Load, 100*time.Millisecond, 10*time.Millisecond,
		"a freeze must wait for the delete")

	close(cloud.block)
	select {
	case <-freezeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("freeze did not proceed after the delete")
	}
}

// An UNFREEZING read may be a later unfreeze whose downloads still need the copy.
func TestUnfreezeSourceDeleteWaitsForTheUnfreezeToEnd(t *testing.T) {
	cases := []struct {
		name       string
		end        string
		wantDelete bool
	}{
		{name: "deleted once it ends HOT", end: models.TenantActivityStatusHOT, wantDelete: true},
		{name: "deleted once it ends COLD", end: models.TenantActivityStatusCOLD, wantDelete: true},
		{name: "deleted once the tenant is gone", end: "", wantDelete: true},
		{name: "kept when it ends FROZEN", end: models.TenantActivityStatusFROZEN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := newUnfreezeSchema(models.TenantActivityStatusUNFREEZING)
			proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{})}
			close(proc.release)
			cloud := &deleteRecordingCloud{}
			m, idx := newUnfreezeMigrator(t, schema, proc, cloud)

			m.unfreeze(context.Background(), idx, "C", []string{"t1#node1"}, errorcompounder.NewSafe())
			require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
				200*time.Millisecond, 10*time.Millisecond, "deleted while the tenant was unfreezing")

			schema.status.Store(tc.end)
			if !tc.wantDelete {
				require.Never(t, func() bool { return len(cloud.deleted()) > 0 },
					200*time.Millisecond, 10*time.Millisecond, "source must be kept")
				return
			}
			require.Eventually(t, func() bool { return len(cloud.deleted()) == 1 },
				5*time.Second, 10*time.Millisecond)
			require.Equal(t, []string{"C/t1/node1"}, cloud.deleted())
		})
	}
}

// This node cannot see the source node's newer freeze, so it must leave that node's copy alone.
func TestUnfreezeSourceDeleteSparesTheSourceNodesNewerUpload(t *testing.T) {
	schema := newUnfreezeSchema(models.TenantActivityStatusHOT)
	proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{})}
	cloud := &deleteRecordingCloud{}
	m, idx := newUnfreezeMigrator(t, schema, proc, cloud)

	m.unfreeze(context.Background(), idx, "C", []string{"t1#oldnode"}, errorcompounder.NewSafe())
	select {
	case <-proc.called:
	case <-time.After(5 * time.Second):
		t.Fatal("unfreeze did not report DONE")
	}
	require.NoError(t, cloud.Upload(context.Background(), "C", "t1", "oldnode"))
	close(proc.release)

	require.Never(t, func() bool { return !cloud.has("C/t1/oldnode") },
		300*time.Millisecond, 10*time.Millisecond, "deleted the source node's newer upload")
}

func TestUnfreezeSourceDeleteGivesUpOnAnUnfreezeThatNeverEnds(t *testing.T) {
	schema := newUnfreezeSchema(models.TenantActivityStatusUNFREEZING)
	proc := &gatedProcessor{release: make(chan struct{}), called: make(chan struct{})}
	close(proc.release)
	cloud := &deleteRecordingCloud{}
	m, idx := newUnfreezeMigrator(t, schema, proc, cloud)
	logger, hook := test.NewNullLogger()
	m.logger = logger
	m.unfreezeSourceMaxWait = 50 * time.Millisecond

	m.unfreeze(context.Background(), idx, "C", []string{"t1#node1"}, errorcompounder.NewSafe())

	require.Eventually(t, func() bool {
		for _, e := range hook.AllEntries() {
			if strings.Contains(e.Message, "did not end within") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the cleanup must stop waiting")
	require.Empty(t, cloud.deleted())
}

type orderRecordingCloud struct {
	failingOffloadCloud
	mu        sync.Mutex
	calls     []string
	deleteErr error
}

func (c *orderRecordingCloud) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *orderRecordingCloud) Delete(_ context.Context, class, tenant, node string) error {
	c.record("delete " + class + "/" + tenant + "/" + node)
	return c.deleteErr
}

func (c *orderRecordingCloud) Upload(_ context.Context, class, tenant, node string) error {
	c.record("upload " + class + "/" + tenant + "/" + node)
	return nil
}

func TestFreezeClearsItsOwnCopyBeforeUploading(t *testing.T) {
	cases := []struct {
		name      string
		deleteErr error
		wantCalls []string
		wantOp    command.TenantsProcess_Op
	}{
		{
			name:      "cleared, then uploaded",
			wantCalls: []string{"delete C/t1/node1", "upload C/t1/node1"},
			wantOp:    command.TenantsProcess_OP_DONE,
		},
		{
			name:      "a failed clear aborts without uploading",
			deleteErr: fmt.Errorf("s3 down"),
			wantCalls: []string{"delete C/t1/node1"},
			wantOp:    command.TenantsProcess_OP_ABORT,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := &Index{
				logger:           logger,
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
			}
			m := NewMigrator(nil, logger, "node1")
			m.SetNode("node1")
			proc := &recordingProcessor{}
			m.SetCluster(proc)
			cloud := &orderRecordingCloud{deleteErr: tc.deleteErr}
			m.cloud = cloud

			m.freeze(context.Background(), idx, "C",
				[]*schemaUC.UpdateTenantPayload{{Name: "t1", PreFreezeStatus: models.TenantActivityStatusHOT}},
				errorcompounder.NewSafe())

			require.Eventually(t, func() bool {
				proc.mu.Lock()
				defer proc.mu.Unlock()
				return proc.req != nil
			}, 5*time.Second, 10*time.Millisecond, "freeze must report")
			proc.mu.Lock()
			defer proc.mu.Unlock()
			require.Len(t, proc.req.TenantsProcesses, 1)
			require.Equal(t, tc.wantOp, proc.req.TenantsProcesses[0].Op)
			cloud.mu.Lock()
			defer cloud.mu.Unlock()
			require.Equal(t, tc.wantCalls, cloud.calls)
		})
	}
}
