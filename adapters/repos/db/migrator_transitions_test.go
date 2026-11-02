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
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	esync "github.com/weaviate/weaviate/entities/sync"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	transitionClass  = "C"
	transitionTenant = "t1"
	transitionNode   = "node1"
)

type fakeTransitionCloud struct {
	mu        sync.Mutex
	uploads   []string
	downloads []string
	deletes   []string
	copies    []string
	empty     map[string]bool

	uploadErr  error
	hasDataErr error
	listErr    error
	download   func(ctx context.Context, localPath string) error
	upload     func(ctx context.Context) error
}

func (c *fakeTransitionCloud) VerifyBucket(context.Context) error { return nil }

func (c *fakeTransitionCloud) Upload(ctx context.Context, _, _, copyPath string) error {
	c.mu.Lock()
	c.uploads = append(c.uploads, copyPath)
	c.mu.Unlock()
	if c.upload != nil {
		return c.upload(ctx)
	}
	return c.uploadErr
}

func (c *fakeTransitionCloud) Download(context.Context, string, string, string) error { return nil }

func (c *fakeTransitionCloud) DownloadToPath(ctx context.Context, _, _, copyPath, localPath string) error {
	c.mu.Lock()
	c.downloads = append(c.downloads, copyPath)
	c.mu.Unlock()
	if c.download != nil {
		return c.download(ctx, localPath)
	}
	return nil
}

func (c *fakeTransitionCloud) HasData(_ context.Context, _, _, copyPath string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasDataErr != nil {
		return false, c.hasDataErr
	}
	return !c.empty[copyPath], nil
}

func (c *fakeTransitionCloud) ListCopies(context.Context, string, string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.copies), c.listErr
}

func (c *fakeTransitionCloud) Delete(_ context.Context, _, _, copyPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, copyPath)
	return nil
}

func (c *fakeTransitionCloud) snapshot() (uploads, downloads, deletes []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.uploads), slices.Clone(c.downloads), slices.Clone(c.deletes)
}

type fakeTransitionCluster struct {
	mu        sync.Mutex
	records   []clusterschema.TenantTransition
	listed    int
	reports   []*command.TenantProcessRequest
	failures  int
	attempts  int
	notCaught bool
}

func (c *fakeTransitionCluster) UpdateTenantsProcess(_ context.Context, _ string, req *command.TenantProcessRequest) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	if c.failures > 0 {
		c.failures--
		return 0, errors.New("no leader")
	}
	c.reports = append(c.reports, req)
	return 1, nil
}

func (c *fakeTransitionCluster) passes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listed
}

func (c *fakeTransitionCluster) RunningTransitions() []clusterschema.TenantTransition {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listed++
	return slices.Clone(c.records)
}

func (c *fakeTransitionCluster) TransitionsCaughtUp() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.notCaught
}

func (c *fakeTransitionCluster) setRecords(records ...clusterschema.TenantTransition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = records
}

func (c *fakeTransitionCluster) sent() []*command.TenantProcessRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.reports)
}

type transitionHarness struct {
	migrator *Migrator
	idx      *Index
	cloud    *fakeTransitionCloud
	cluster  *fakeTransitionCluster

	mu     sync.Mutex
	tenant *sharding.Physical
}

// tenantReader serves current's tenant with the transition the cluster runs for it.
func tenantReader(t *testing.T, class, name string, cluster *fakeTransitionCluster, current func() *sharding.Physical) local.SchemaReader {
	reader := local.NewMockSchemaReader(t)
	reader.EXPECT().Read(class, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			state := &sharding.State{Physical: map[string]sharding.Physical{}}
			if p := current(); p != nil {
				tenant := p.DeepCopy()
				tenant.Name = name
				for _, record := range cluster.RunningTransitions() {
					if record.Class == class && record.Tenant == name {
						tenant.Transition = record.Transition
					}
				}
				state.Physical[name] = tenant
			}
			return read(nil, state)
		}).Maybe()
	return reader
}

func newTransitionHarness(t *testing.T) *transitionHarness {
	t.Helper()
	logger, _ := test.NewNullLogger()
	h := &transitionHarness{
		cloud:   &fakeTransitionCloud{empty: map[string]bool{}},
		cluster: &fakeTransitionCluster{},
		tenant:  &sharding.Physical{Status: models.TenantActivityStatusHOT},
	}

	h.idx = &Index{
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		schemaReader: tenantReader(t, transitionClass, transitionTenant, h.cluster, func() *sharding.Physical {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.tenant
		}),
	}
	h.idx.Config.RootPath = t.TempDir()
	h.idx.Config.ClassName = transitionClass
	require.NoError(t, os.MkdirAll(h.idx.path(), 0o755))

	h.migrator = NewMigrator(indexRegistry(h.idx), logger, transitionNode)
	h.migrator.SetNode(transitionNode)
	h.migrator.SetCluster(h.cluster)
	h.migrator.cloud = h.cloud
	return h
}

// indexRegistry is a DB that only resolves indexes.
func indexRegistry(indexes ...*Index) *DB {
	db := &DB{indices: map[string]*Index{}}
	for _, idx := range indexes {
		db.indices[idx.ID()] = idx
	}
	return db
}

func (h *transitionHarness) setTenant(p *sharding.Physical) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tenant = p
}

func (h *transitionHarness) reconcile(t *testing.T) {
	t.Helper()
	h.migrator.transitions.reconcile(context.Background())
}

func (h *transitionHarness) workers() map[transitionKey]*transitionWorker {
	h.migrator.transitions.mu.Lock()
	defer h.migrator.transitions.mu.Unlock()
	return maps.Clone(h.migrator.transitions.workers)
}

func (h *transitionHarness) writeLocalShard(t *testing.T) {
	t.Helper()
	dir := path.Join(h.idx.path(), transitionTenant)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "segment"), []byte("old"), 0o600))
}

func (h *transitionHarness) localFile(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(h.idx.path(), transitionTenant, "segment"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	require.NoError(t, err)
	return string(content)
}

func (h *transitionHarness) awaitReport(t *testing.T) *command.TenantProcessRequest {
	t.Helper()
	require.Eventually(t, func() bool { return len(h.cluster.sent()) > 0 }, 5*time.Second, 5*time.Millisecond)
	return h.cluster.sent()[0]
}

func (h *transitionHarness) stagingFolders(t *testing.T) []string {
	t.Helper()
	staged, err := filepath.Glob(filepath.Join(h.idx.path(), unfreezeStagingPrefix+"*"))
	require.NoError(t, err)
	return staged
}

func freezeRecord(attempt uint64) clusterschema.TenantTransition {
	return clusterschema.TenantTransition{
		Class:  transitionClass,
		Tenant: transitionTenant,
		Transition: &sharding.Transition{
			Kind:    sharding.TransitionFreeze,
			Target:  models.TenantActivityStatusFROZEN,
			Attempt: attempt,
			Participants: map[string]sharding.TransitionParticipant{
				transitionNode: {Progress: sharding.ProgressStarted},
			},
		},
	}
}

// unfreezeRecord downloads from source, or nothing when source is empty.
func unfreezeRecord(attempt, cloudAttempt uint64, source string) clusterschema.TenantTransition {
	return clusterschema.TenantTransition{
		Class:        transitionClass,
		Tenant:       transitionTenant,
		CloudAttempt: cloudAttempt,
		Transition: &sharding.Transition{
			Kind:    sharding.TransitionUnfreeze,
			Target:  models.TenantActivityStatusHOT,
			Attempt: attempt,
			Chosen:  []string{transitionNode},
			Participants: map[string]sharding.TransitionParticipant{
				transitionNode: {Source: source, Progress: sharding.ProgressStarted},
			},
		},
	}
}

func TestFreezeWorker(t *testing.T) {
	cases := []struct {
		name        string
		uploadErr   error
		hasDataErr  error
		emptyCopy   bool
		wantOp      command.TenantsProcess_Op
		wantHasData bool
	}{
		{name: "upload with data", wantOp: command.TenantsProcess_OP_DONE, wantHasData: true},
		{name: "upload of an empty tenant", emptyCopy: true, wantOp: command.TenantsProcess_OP_DONE},
		{name: "failed upload aborts", uploadErr: errors.New("s3 down"), wantOp: command.TenantsProcess_OP_ABORT},
		// Reporting no data would make unfreeze skip a copy that holds some.
		{name: "a failed data check aborts", hasDataErr: errors.New("s3 down"), wantOp: command.TenantsProcess_OP_ABORT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.writeLocalShard(t)
			h.cloud.uploadErr = tc.uploadErr
			h.cloud.hasDataErr = tc.hasDataErr
			h.cloud.empty[cloudCopyPath(7, transitionNode)] = tc.emptyCopy
			h.cluster.setRecords(freezeRecord(7))

			h.reconcile(t)

			report := h.awaitReport(t)
			require.Equal(t, command.TenantProcessRequest_ACTION_FREEZING, report.Action)
			require.Equal(t, transitionNode, report.Node)
			got := report.TenantsProcesses[0]
			require.Equal(t, tc.wantOp, got.Op)
			require.Equal(t, uint64(7), got.Attempt, "the report names its attempt")
			require.Equal(t, tc.wantHasData, got.HasData)

			uploads, _, _ := h.cloud.snapshot()
			require.Equal(t, []string{"7/" + transitionNode}, uploads, "copies live under their attempt")
		})
	}
}

func TestPanicWhileHaltingFreesTheShardLock(t *testing.T) {
	h := newTransitionHarness(t)
	shard := NewMockShardLike(t)
	shard.EXPECT().preventShutdown().Return(func() {}, nil).Maybe()
	shard.EXPECT().HaltForTransfer(mock.Anything, true, time.Duration(0)).
		RunAndReturn(func(context.Context, bool, time.Duration) error { panic("halt") })
	h.idx.shards.Store(transitionTenant, shard)
	h.cluster.setRecords(freezeRecord(7))

	h.reconcile(t)

	require.Equal(t, command.TenantsProcess_OP_ABORT, h.awaitReport(t).TenantsProcesses[0].Op)
	require.True(t, h.idx.shardCreateLocks.TryRLock(transitionTenant), "the shard lock must not outlive the panic")
	h.idx.shardCreateLocks.RUnlock(transitionTenant)
}

func TestFreezeWithoutLocalData(t *testing.T) {
	cases := []struct {
		name        string
		resumed     bool
		wantOp      command.TenantsProcess_Op
		wantUploads int
	}{
		// The folder may have gone with a crash, so an empty upload must not pass.
		{name: "resumed after a restart aborts", resumed: true, wantOp: command.TenantsProcess_OP_ABORT},
		{name: "started live is an empty tenant", wantOp: command.TenantsProcess_OP_DONE, wantUploads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			if !tc.resumed {
				h.reconcile(t) // the first pass is the restart
			}

			h.cluster.setRecords(freezeRecord(7))
			h.reconcile(t)

			require.Equal(t, tc.wantOp, h.awaitReport(t).TenantsProcesses[0].Op)
			uploads, _, _ := h.cloud.snapshot()
			require.Len(t, uploads, tc.wantUploads)
		})
	}
}

func TestUnfreezeWorker(t *testing.T) {
	stage := func(content string) func(context.Context, string) error {
		return func(_ context.Context, localPath string) error {
			if err := os.MkdirAll(path.Join(localPath, hashTreeDirName), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(localPath, hashTreeDirName, "x.ht"), []byte("stale"), 0o600); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(localPath, "segment"), []byte(content), 0o600)
		}
	}
	failAfterPartial := func(_ context.Context, localPath string) error {
		if err := os.MkdirAll(localPath, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(localPath, "segment"), []byte("partial"), 0o600); err != nil {
			return err
		}
		return errors.New("connection reset")
	}

	cases := []struct {
		name          string
		cloudAttempt  uint64
		source        string
		sourceEmpty   bool
		staleFolder   bool
		download      func(context.Context, string) error
		wantOp        command.TenantsProcess_Op
		wantLocal     string
		wantDownloads []string
	}{
		{
			name: "download is swapped in", cloudAttempt: 3, source: "oldnode", download: stage("new"),
			wantOp: command.TenantsProcess_OP_DONE, wantLocal: "new", wantDownloads: []string{"3/oldnode"},
		},
		{
			name: "download replaces a leftover folder", cloudAttempt: 3, source: "oldnode", staleFolder: true, download: stage("new"),
			wantOp: command.TenantsProcess_OP_DONE, wantLocal: "new", wantDownloads: []string{"3/oldnode"},
		},
		{
			name: "a failed download never reaches the shard", cloudAttempt: 3, source: "oldnode", staleFolder: true,
			download: failAfterPartial, wantOp: command.TenantsProcess_OP_ABORT, wantLocal: "old", wantDownloads: []string{"3/oldnode"},
		},
		{
			name: "a lost copy aborts before downloading", cloudAttempt: 3, source: "oldnode", sourceEmpty: true,
			wantOp: command.TenantsProcess_OP_ABORT,
		},
		{
			name: "a download that staged nothing from a copy with data aborts", cloudAttempt: 3, source: "oldnode", staleFolder: true,
			download: func(context.Context, string) error { return nil },
			wantOp:   command.TenantsProcess_OP_ABORT, wantLocal: "old", wantDownloads: []string{"3/oldnode"},
		},
		{
			name: "an empty tenant has nothing to download", cloudAttempt: 3, staleFolder: true,
			wantOp: command.TenantsProcess_OP_DONE,
		},
		{name: "no recorded attempt aborts", source: "oldnode", wantOp: command.TenantsProcess_OP_ABORT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			if tc.staleFolder {
				h.writeLocalShard(t)
			}
			h.cloud.download = tc.download
			h.cloud.empty[cloudCopyPath(tc.cloudAttempt, "oldnode")] = tc.sourceEmpty
			h.cluster.setRecords(unfreezeRecord(9, tc.cloudAttempt, tc.source))

			h.reconcile(t)

			report := h.awaitReport(t)
			require.Equal(t, command.TenantProcessRequest_ACTION_UNFREEZING, report.Action)
			require.Equal(t, tc.wantOp, report.TenantsProcesses[0].Op)
			require.Equal(t, tc.wantLocal, h.localFile(t))
			_, downloads, _ := h.cloud.snapshot()
			require.Equal(t, tc.wantDownloads, downloads)
			require.Empty(t, h.stagingFolders(t))
			require.NoDirExists(t, path.Join(h.idx.path(), transitionTenant, hashTreeDirName))
		})
	}
}

// An unfreeze onto another node that skips the freezing node's records serves an empty index.
func TestUnfreezeAdoptsTheFreezingNodesRecords(t *testing.T) {
	logger, _ := test.NewNullLogger()
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	subject.Key.UnitID = MigrationUnitID(transitionTenant, "oldnode")

	h := newTransitionHarness(t)
	h.cloud.download = func(_ context.Context, localPath string) error {
		lsmPath := path.Join(localPath, "lsm")
		if err := os.MkdirAll(lsmPath, 0o755); err != nil {
			return err
		}
		return NewMigrationRecordStore(lsmPath, logger).Put(newMigrationRecordAt(t, subject, MigrationStateSwapped))
	}
	h.cluster.setRecords(unfreezeRecord(9, 3, "oldnode"))

	h.reconcile(t)

	require.Equal(t, command.TenantsProcess_OP_DONE, h.awaitReport(t).TenantsProcesses[0].Op)
	records := NewMigrationRecordStoreForUnit(shardPathLSM(h.idx.path(), transitionTenant),
		MigrationUnitID(transitionTenant, transitionNode), logger)
	require.NoError(t, records.Load())
	require.Len(t, records.Records(), 1, "the freezing node's record is this node's to finalize")
	require.Equal(t, MigrationStateSwapped, records.Records()[0].State())
}

func TestCancelledUnfreezeKeepsTheLocalFolder(t *testing.T) {
	h := newTransitionHarness(t)
	h.writeLocalShard(t)
	entered := make(chan struct{})
	h.cloud.download = func(ctx context.Context, localPath string) error {
		if err := os.MkdirAll(localPath, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(localPath, "segment"), []byte("partial"), 0o600); err != nil {
			return err
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	h.cluster.setRecords(unfreezeRecord(9, 3, "oldnode"))
	h.reconcile(t)
	<-entered

	h.cluster.setRecords()
	h.reconcile(t)

	require.Eventually(t, func() bool { return len(h.stagingFolders(t)) == 0 }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, "old", h.localFile(t))
	require.Empty(t, h.cluster.sent(), "an ended attempt reports nothing")
}

// The cancel lands before the reconciler stops the worker, which already holds a finished download.
func TestUnfreezeEndedAfterItsDownloadDoesNotSwap(t *testing.T) {
	h := newTransitionHarness(t)
	h.writeLocalShard(t)
	h.cloud.download = func(_ context.Context, localPath string) error {
		if err := os.MkdirAll(localPath, 0o755); err != nil {
			return err
		}
		h.cluster.setRecords()
		return os.WriteFile(filepath.Join(localPath, "segment"), []byte("new"), 0o600)
	}
	h.cluster.setRecords(unfreezeRecord(9, 3, "oldnode"))

	h.reconcile(t)

	require.Equal(t, command.TenantsProcess_OP_ABORT, h.awaitReport(t).TenantsProcesses[0].Op)
	require.Equal(t, "old", h.localFile(t))
	require.Empty(t, h.stagingFolders(t))
}

func TestReportRetriesUntilItCommits(t *testing.T) {
	h := newTransitionHarness(t)
	h.writeLocalShard(t)
	h.cluster.failures = 2
	h.cluster.setRecords(freezeRecord(7))

	h.reconcile(t)

	require.Eventually(t, func() bool { return len(h.cluster.sent()) == 1 }, 10*time.Second, 10*time.Millisecond)
	h.cluster.mu.Lock()
	defer h.cluster.mu.Unlock()
	require.Equal(t, 3, h.cluster.attempts)
}

func TestFinishedWorkIsNotRedoneWhileTheFSMLags(t *testing.T) {
	h := newTransitionHarness(t)
	h.writeLocalShard(t)
	h.cluster.setRecords(freezeRecord(7))
	h.reconcile(t)
	h.awaitReport(t)
	worker := h.workers()[transitionKey{transitionClass, transitionTenant}]
	<-worker.done

	h.reconcile(t) // the record still shows START

	require.Equal(t, map[transitionKey]*transitionWorker{{transitionClass, transitionTenant}: worker}, h.workers())
}

func TestReconcilerStartsNoWorker(t *testing.T) {
	cases := []struct {
		name      string
		notCaught bool
		node      string
		progress  string
	}{
		{name: "until the FSM caught up", notCaught: true, node: transitionNode, progress: sharding.ProgressStarted},
		{name: "for a transition it takes no part in", node: "node2", progress: sharding.ProgressStarted},
		// A restart forgets the workers, so the record is all it has.
		{name: "for its part that is done", node: transitionNode, progress: sharding.ProgressDone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.writeLocalShard(t)
			h.cluster.notCaught = tc.notCaught
			record := freezeRecord(7)
			record.Transition.Participants = map[string]sharding.TransitionParticipant{tc.node: {Progress: tc.progress}}
			h.cluster.setRecords(record)

			h.reconcile(t)

			require.Empty(t, h.workers())
		})
	}
}

func TestFirstPassClearsStaging(t *testing.T) {
	h := newTransitionHarness(t)
	leftover := h.idx.unfreezeStagingPath(transitionTenant, 4)
	require.NoError(t, os.MkdirAll(leftover, 0o755))

	h.reconcile(t)

	require.NoDirExists(t, leftover)
}

func TestEndedTransitionDeletesOldAttempts(t *testing.T) {
	cases := []struct {
		name         string
		record       clusterschema.TenantTransition
		tenant       *sharding.Physical
		classDeleted bool
		copies       []string
		wantDelete   []string
	}{
		{
			name:       "an ended transition that left the tenant unfrozen deletes every attempt up to its own",
			record:     unfreezeRecord(9, 3, "oldnode"),
			tenant:     &sharding.Physical{Status: models.TenantActivityStatusHOT},
			copies:     []string{"3", "9"},
			wantDelete: []string{"3", "9"},
		},
		{
			name:       "a frozen tenant keeps the attempt that holds it",
			record:     freezeRecord(7),
			tenant:     &sharding.Physical{Status: models.TenantActivityStatusFROZEN, CloudAttempt: 7},
			copies:     []string{"5", "7"},
			wantDelete: []string{"5"},
		},
		{
			name:       "a newer attempt is never touched",
			record:     freezeRecord(7),
			tenant:     &sharding.Physical{Status: models.TenantActivityStatusHOT},
			copies:     []string{"7", "12"},
			wantDelete: []string{"7"},
		},
		{
			name:       "a tenant deleted before the upload stopped",
			record:     freezeRecord(7),
			copies:     []string{"7", "12"},
			wantDelete: []string{"7"},
		},
		{
			name:         "a class deleted before the upload stopped",
			record:       freezeRecord(7),
			tenant:       &sharding.Physical{Status: models.TenantActivityStatusHOT},
			classDeleted: true,
			copies:       []string{"7"},
			wantDelete:   []string{"7"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.writeLocalShard(t)
			h.cloud.download = func(context.Context, string) error { return nil }
			h.cluster.setRecords(tc.record)
			h.reconcile(t)
			h.awaitReport(t)

			h.cloud.mu.Lock()
			h.cloud.copies = tc.copies
			h.cloud.deletes = nil
			h.cloud.mu.Unlock()
			h.setTenant(tc.tenant)
			if tc.classDeleted {
				h.migrator.db = indexRegistry()
			}
			h.cluster.setRecords()
			h.reconcile(t)

			require.Eventually(t, func() bool {
				_, _, deletes := h.cloud.snapshot()
				return len(deletes) == len(tc.wantDelete)
			}, 5*time.Second, 5*time.Millisecond)
			_, _, deletes := h.cloud.snapshot()
			assert.ElementsMatch(t, tc.wantDelete, deletes)
		})
	}
}

func TestNextTransitionDeletesLowerAttempts(t *testing.T) {
	cases := []struct {
		name       string
		record     clusterschema.TenantTransition
		wantDelete []string
	}{
		{name: "a freeze owns no older copy", record: freezeRecord(7), wantDelete: []string{"3", "5"}},
		{name: "an unfreeze keeps its source", record: unfreezeRecord(7, 5, "oldnode"), wantDelete: []string{"3"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.writeLocalShard(t)
			h.cloud.copies = []string{"3", "5", "not-an-attempt"}
			h.cluster.setRecords(tc.record)

			h.reconcile(t)
			h.awaitReport(t)

			_, _, deletes := h.cloud.snapshot()
			assert.ElementsMatch(t, tc.wantDelete, deletes)
		})
	}
}

func TestDeletingOldAttemptsFailingDoesNotStopTheWork(t *testing.T) {
	h := newTransitionHarness(t)
	h.writeLocalShard(t)
	h.cloud.listErr = errors.New("s3 down")
	h.cluster.setRecords(freezeRecord(7))

	h.reconcile(t)

	require.Equal(t, command.TenantsProcess_OP_DONE, h.awaitReport(t).TenantsProcesses[0].Op)
}

// Each pass reads every tenant under its class lock, so a burst of kicks must cost one pass.
func TestReconcilerBatchesKicks(t *testing.T) {
	h := newTransitionHarness(t)
	h.migrator.transitions.kickDelay = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.migrator.RunTransitions(ctx)
	}()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return h.cluster.passes() == 1 }, time.Second, 5*time.Millisecond, "the first pass")

	for range 50 {
		h.migrator.transitions.kick()
		time.Sleep(time.Millisecond)
	}

	require.Eventually(t, func() bool { return h.cluster.passes() >= 2 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return h.cluster.passes() > 3 }, 500*time.Millisecond, 10*time.Millisecond)
}

func TestOnlyATransitionStartWakesTheReconciler(t *testing.T) {
	for status, wake := range map[string]bool{
		models.TenantActivityStatusHOT:        false,
		models.TenantActivityStatusCOLD:       false,
		models.TenantActivityStatusFREEZING:   true,
		models.TenantActivityStatusUNFREEZING: true,
	} {
		t.Run(status, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m := NewMigrator(&DB{indices: map[string]*Index{}}, logger, transitionNode)

			_ = m.UpdateTenants(context.Background(), &models.Class{Class: transitionClass},
				[]*schemaUC.UpdateTenantPayload{{Name: transitionTenant, Status: status}}, false)

			assert.Equal(t, wake, len(m.transitions.wake) == 1)
		})
	}
}

func TestShardLoadRefusedDuringTransition(t *testing.T) {
	cases := []struct {
		name    string
		tenant  *sharding.Physical
		caller  shardLoadCaller
		wantErr error
	}{
		{name: "no transition loads", tenant: &sharding.Physical{Status: models.TenantActivityStatusHOT}, caller: callerUserRequest},
		{name: "unknown tenant is not refused here", caller: callerUserRequest},
		{
			name:    "a running transition refuses",
			tenant:  &sharding.Physical{Status: models.TenantActivityStatusHOT, Transition: freezeRecord(7).Transition},
			caller:  callerUserRequest,
			wantErr: errTenantInTransition,
		},
		{
			name:   "replica movement is exempt",
			tenant: &sharding.Physical{Status: models.TenantActivityStatusHOT, Transition: freezeRecord(7).Transition},
			caller: callerMovement,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.setTenant(tc.tenant)

			err := h.idx.refuseLoadDuringTransition(transitionTenant, tc.caller)

			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestDeleteCloudAttemptsNamesExactAttempts(t *testing.T) {
	cloud := &fakeTransitionCloud{copies: []string{"3", "8", "9", "12", "legacy"}}

	require.NoError(t, deleteCloudAttempts(context.Background(), cloud, transitionClass, transitionTenant, 8, 10))

	_, _, deletes := cloud.snapshot()
	assert.ElementsMatch(t, []string{"3", "9"}, deletes, fmt.Sprintf("got %v", deletes))
}

// Without the module a participant never reports, so its transition would wait forever.
func TestParticipantWithoutTheModuleAborts(t *testing.T) {
	tests := []struct {
		name   string
		record clusterschema.TenantTransition
		action command.TenantProcessRequest_Action
	}{
		{name: "freeze", record: freezeRecord(7), action: command.TenantProcessRequest_ACTION_FREEZING},
		{name: "unfreeze", record: unfreezeRecord(9, 3, "oldnode"), action: command.TenantProcessRequest_ACTION_UNFREEZING},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTransitionHarness(t)
			h.writeLocalShard(t)
			logger, hook := test.NewNullLogger()
			h.migrator.logger = logger
			h.migrator.cloud = nil
			h.cluster.setRecords(tt.record)

			h.reconcile(t)
			h.reconcile(t)

			report := h.awaitReport(t)
			require.Equal(t, tt.action, report.Action)
			require.Equal(t, command.TenantsProcess_OP_ABORT, report.TenantsProcesses[0].Op)
			require.Equal(t, "old", h.localFile(t))
			var logged int
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, errOffloadDisabled.Error()) {
					logged++
				}
			}
			require.Equal(t, 1, logged, "logged once per attempt")
		})
	}
}

type fakeOffloadProvider struct {
	cloud modulecapabilities.OffloadCloud
}

func (p fakeOffloadProvider) OffloadBackend(string) (modulecapabilities.OffloadCloud, bool) {
	return p.cloud, p.cloud != nil
}

func TestSetOffloadProviderLogsWhatItFound(t *testing.T) {
	tests := []struct {
		name            string
		cloud           modulecapabilities.OffloadCloud
		wantEnabledLogs int
	}{
		{name: "enabled", cloud: &fakeTransitionCloud{}, wantEnabledLogs: 1},
		{name: "not enabled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			m := NewMigrator(&DB{}, logger, transitionNode)

			m.SetOffloadProvider(fakeOffloadProvider{cloud: tt.cloud}, "offload-s3")

			require.Equal(t, tt.cloud, m.cloud)
			var enabledLogs int
			for _, entry := range hook.AllEntries() {
				if entry.Message == "module offload-s3 is enabled" {
					enabledLogs++
				}
			}
			require.Equal(t, tt.wantEnabledLogs, enabledLogs)
		})
	}
}
