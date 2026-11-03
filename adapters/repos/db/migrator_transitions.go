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
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	transitionReconcileInterval = 10 * time.Second
	transitionReportMaxDelay    = 30 * time.Second
	unfreezeStagingPrefix       = ".unfreeze-"
)

var errCloudCopyMissing = errors.New("cloud copy is missing")

type transitionKey struct {
	class  string
	tenant string
}

// transitionWorker stays registered after it reports, so a lagging FSM cannot
// restart finished work.
type transitionWorker struct {
	attempt uint64
	cancel  context.CancelFunc
	done    chan struct{}
	halted  atomic.Bool
}

// transitionManager runs this node's share of freezes and unfreezes off the
// apply path.
type transitionManager struct {
	m     *Migrator
	wake  chan struct{}
	slots chan struct{}

	mu       sync.Mutex
	workers  map[transitionKey]*transitionWorker
	reloaded bool
}

func newTransitionManager(m *Migrator) *transitionManager {
	return &transitionManager{
		m:       m,
		wake:    make(chan struct{}, 1),
		slots:   make(chan struct{}, _NUMCPU*2),
		workers: make(map[transitionKey]*transitionWorker),
	}
}

func (t *transitionManager) kick() {
	if t == nil {
		return
	}
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *transitionManager) run(ctx context.Context) {
	ticker := time.NewTicker(transitionReconcileInterval)
	defer ticker.Stop()
	for {
		t.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-t.wake:
		}
	}
}

func (t *transitionManager) reconcile(ctx context.Context) {
	if t.m.cloud == nil || t.m.cluster == nil || !t.m.cluster.TransitionsCaughtUp() {
		return
	}

	records := make(map[transitionKey]clusterschema.TenantTransition)
	for _, record := range t.m.cluster.RunningTransitions() {
		records[transitionKey{record.Class, record.Tenant}] = record
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	resumed := !t.reloaded
	if resumed {
		t.removeStaging()
		t.reloaded = true
	}

	for key, worker := range t.workers {
		if record, ok := records[key]; ok && record.Transition.Attempt == worker.attempt {
			continue
		}
		worker.cancel()
		delete(t.workers, key)
		enterrors.GoWrapper(func() { t.finish(ctx, key, worker) }, t.m.logger)
	}

	for key, record := range records {
		participant, ok := record.Transition.Participants[t.m.nodeId]
		if !ok || participant.Progress != sharding.ProgressStarted {
			continue
		}
		if _, running := t.workers[key]; running {
			continue
		}
		t.start(ctx, key, record, resumed)
	}
}

func (t *transitionManager) start(ctx context.Context, key transitionKey, record clusterschema.TenantTransition, resumed bool) {
	workerCtx, cancel := context.WithCancel(ctx)
	worker := &transitionWorker{attempt: record.Transition.Attempt, cancel: cancel, done: make(chan struct{})}
	t.workers[key] = worker
	enterrors.GoWrapper(func() {
		defer close(worker.done)
		t.work(workerCtx, record, worker, resumed)
	}, t.m.logger)
}

func (t *transitionManager) work(ctx context.Context, record clusterschema.TenantTransition, worker *transitionWorker, resumed bool) {
	select {
	case t.slots <- struct{}{}:
		defer func() { <-t.slots }()
	case <-ctx.Done():
		return
	}

	var hasData bool
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				enterrors.PrintStack(t.logger(record))
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		if record.Transition.Kind == sharding.TransitionFreeze {
			hasData, err = t.freeze(ctx, record, worker, resumed)
		} else {
			err = t.unfreeze(ctx, record)
		}
	}()
	if ctx.Err() != nil {
		return
	}

	op := command.TenantsProcess_OP_DONE
	if err != nil {
		op = command.TenantsProcess_OP_ABORT
		t.logger(record).Errorf("aborting %s: %v", record.Transition.Kind, err)
	}
	t.report(ctx, record, op, hasData)
}

func (t *transitionManager) logger(record clusterschema.TenantTransition) logrus.FieldLogger {
	return t.m.logger.WithFields(logrus.Fields{
		"action":  "tenant_transition",
		"class":   record.Class,
		"tenant":  record.Tenant,
		"attempt": record.Transition.Attempt,
	})
}

func (t *transitionManager) freeze(ctx context.Context, record clusterschema.TenantTransition, worker *transitionWorker, resumed bool) (bool, error) {
	name := record.Tenant
	idx := t.m.db.registeredIndex(schema.ClassName(record.Class))
	if idx == nil {
		return false, fmt.Errorf("cannot find index for %q", record.Class)
	}
	attempt := record.Transition.Attempt
	t.deleteOldAttempts(ctx, record.Class, name, 0, attempt)

	shard, release, err := idx.GetShard(ctx, name)
	if err != nil && !errors.Is(err, errAlreadyShutdown) {
		return false, fmt.Errorf("get local shard: %w", err)
	}
	defer release()

	if shard != nil {
		// The write lock waits out writes in flight, so the halt flushes them
		// and any later one is refused.
		err := func() error {
			idx.backupLock.Lock(name)
			defer idx.backupLock.Unlock(name)
			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)
			phase := idx.nodeOverlay().PhaseOf(record.Class, name)
			if phase != nil && phase.Transferring() {
				// An ended attempt's fence stays up until its cleanup runs, and
				// the halt cannot flush a paused store.
				phase.SetTransferring(false)
				if err := shard.syncStorePause(); err != nil {
					return err
				}
			}
			if err := shard.HaltForTransfer(ctx, true, 0); err != nil {
				return err
			}
			worker.halted.Store(true)
			if phase != nil {
				phase.SetTransferring(true)
			}
			return shard.syncStorePause()
		}()
		if err != nil {
			if !worker.halted.Load() {
				// The halt stops async replication before any step that can fail.
				if err := idx.rebuildAsyncReplication(ctx, name); err != nil {
					t.logger(record).Errorf("restart async replication after a failed halt: %v", err)
				}
			}
			return false, fmt.Errorf("halt for transfer: %w", err)
		}
	} else if resumed {
		entries, err := os.ReadDir(path.Join(idx.path(), name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("read local shard folder: %w", err)
		}
		if len(entries) == 0 {
			return false, fmt.Errorf("resumed freeze found no local data")
		}
	}

	idx.backupLock.RLock(name)
	defer idx.backupLock.RUnlock(name)

	copyPath := cloudCopyPath(attempt, t.m.nodeId)
	if err := t.m.cloud.Upload(ctx, record.Class, name, copyPath); err != nil {
		return false, fmt.Errorf("upload: %w", err)
	}
	return t.m.cloud.HasData(ctx, record.Class, name, copyPath)
}

func (t *transitionManager) unfreeze(ctx context.Context, record clusterschema.TenantTransition) error {
	name := record.Tenant
	idx := t.m.db.registeredIndex(schema.ClassName(record.Class))
	if idx == nil {
		return fmt.Errorf("cannot find index for %q", record.Class)
	}
	idx.backupLock.RLock(name)
	defer idx.backupLock.RUnlock(name)

	if record.CloudAttempt == 0 {
		return fmt.Errorf("%w: no attempt recorded", errCloudCopyMissing)
	}
	attempt := record.Transition.Attempt
	t.deleteOldAttempts(ctx, record.Class, name, record.CloudAttempt, attempt)

	staging := idx.unfreezeStagingPath(name, attempt)
	defer os.RemoveAll(staging)
	if err := os.RemoveAll(staging); err != nil {
		return fmt.Errorf("clear staging: %w", err)
	}

	// A participant without a source unfreezes a tenant that held no data.
	if sourceNode := record.Transition.Participants[t.m.nodeId].Source; sourceNode != "" {
		source := cloudCopyPath(record.CloudAttempt, sourceNode)
		hasData, err := t.m.cloud.HasData(ctx, record.Class, name, source)
		if err != nil {
			return fmt.Errorf("check source: %w", err)
		}
		if !hasData {
			return fmt.Errorf("%w: %s", errCloudCopyMissing, source)
		}
		if err := t.m.cloud.DownloadToPath(ctx, record.Class, name, source, staging); err != nil {
			return fmt.Errorf("download: %w", err)
		}
		// An artifact's .ht would be trusted verbatim on load.
		if err := os.RemoveAll(path.Join(staging, hashTreeDirName)); err != nil {
			return fmt.Errorf("discard downloaded hashtree snapshot: %w", err)
		}
		refusal, err := AdoptArrivedMigrationRecordsInDir(idx.path(), path.Base(staging), name, t.m.nodeId, sourceNode)
		if err != nil {
			return fmt.Errorf("adopt the migration records of %s: %w", sourceNode, err)
		}
		if refusal != "" {
			t.logger(record).Warnf("the migration records of %s stay set aside: %s", sourceNode, refusal)
		}
	}

	idx.shardCreateLocks.Lock(name)
	defer idx.shardCreateLocks.Unlock(name)
	if err := ctx.Err(); err != nil {
		return err
	}
	// The abort's cleanup takes this lock too, so a swap cannot land after it.
	tenant, ok, err := tenantPhysical(idx, record.Class, name)
	if err != nil {
		return fmt.Errorf("read tenant: %w", err)
	}
	if !ok || tenant.Transition == nil || tenant.Transition.Attempt != attempt {
		return fmt.Errorf("attempt %d ended before the swap", attempt)
	}
	target := path.Join(idx.path(), name)
	if err := os.RemoveAll(target); err != nil {
		return fmt.Errorf("replace local folder: %w", err)
	}
	if _, err := os.Stat(staging); errors.Is(err, os.ErrNotExist) {
		return nil // nothing was frozen
	}
	if err := os.Rename(staging, target); err != nil {
		return fmt.Errorf("swap in download: %w", err)
	}
	return nil
}

func (t *transitionManager) report(ctx context.Context, record clusterschema.TenantTransition, op command.TenantsProcess_Op, hasData bool) {
	action := command.TenantProcessRequest_ACTION_UNFREEZING
	if record.Transition.Kind == sharding.TransitionFreeze {
		action = command.TenantProcessRequest_ACTION_FREEZING
	}
	req := &command.TenantProcessRequest{
		Node:   t.m.nodeId,
		Action: action,
		TenantsProcesses: []*command.TenantsProcess{{
			Op:      op,
			Attempt: record.Transition.Attempt,
			HasData: hasData,
			Tenant:  &command.Tenant{Name: record.Tenant},
		}},
	}

	delay := time.Second
	for {
		_, err := t.m.cluster.UpdateTenantsProcess(ctx, record.Class, req)
		if err == nil {
			return
		}
		t.logger(record).Warnf("report %s: %v", op, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, transitionReportMaxDelay)
	}
}

func (t *transitionManager) finish(ctx context.Context, key transitionKey, worker *transitionWorker) {
	<-worker.done

	logger := t.m.logger.WithFields(logrus.Fields{"class": key.class, "tenant": key.tenant})
	idx := t.m.db.registeredIndex(schema.ClassName(key.class))
	if idx == nil {
		// The class delete ran before the upload stopped.
		t.deleteOldAttempts(ctx, key.class, key.tenant, 0, worker.attempt+1)
		return
	}

	var tenant sharding.Physical
	var ok, newer, aborted, fenced bool
	// A newer attempt fences writes under this lock, so its fence stays up.
	err := func() (err error) {
		idx.shardCreateLocks.Lock(key.tenant)
		defer idx.shardCreateLocks.Unlock(key.tenant)
		if tenant, ok, err = tenantPhysical(idx, key.class, key.tenant); err != nil {
			return err
		}
		newer = tenant.Transition != nil
		aborted = ok && !newer && tenant.Status != models.TenantActivityStatusFROZEN
		if !aborted {
			return nil
		}
		if phase := idx.nodeOverlay().PhaseOf(key.class, key.tenant); phase != nil {
			fenced = phase.Transferring()
			phase.SetTransferring(false)
		}
		if shard := idx.shards.Loaded(key.tenant); shard != nil {
			return shard.syncStorePause()
		}
		return nil
	}()
	if err != nil {
		logger.Errorf("finish transition: %v", err)
		return
	}
	if !ok {
		// The tenant delete ran before the upload stopped.
		t.deleteOldAttempts(ctx, key.class, key.tenant, 0, worker.attempt+1)
		return
	}

	switch {
	case aborted && (fenced || worker.halted.Load()):
		if err := idx.resumeAfterAbortedOffload(ctx, key.tenant); err != nil {
			logger.Errorf("resume after aborted freeze: %v", err)
		}
	case newer && worker.halted.Load():
		if shard := idx.shards.Loaded(key.tenant); shard != nil {
			if err := shard.resumeMaintenanceCycles(ctx); err != nil {
				logger.Errorf("release the halt of attempt %d: %v", worker.attempt, err)
			}
		}
	}
	t.deleteOldAttempts(ctx, key.class, key.tenant, tenant.CloudAttempt, worker.attempt+1)
}

func tenantPhysical(idx *Index, class, name string) (sharding.Physical, bool, error) {
	var phys sharding.Physical
	var found bool
	err := idx.schemaReader.Read(class, false, func(_ *models.Class, state *sharding.State) error {
		if state != nil {
			phys, found = state.Physical[name]
		}
		return nil
	})
	return phys, found, err
}

// deleteOldAttempts leaves a copy it fails to delete for the next transition.
func (t *transitionManager) deleteOldAttempts(ctx context.Context, class, tenant string, keep, below uint64) {
	if err := deleteCloudAttempts(ctx, t.m.cloud, class, tenant, keep, below); err != nil {
		t.m.logger.WithFields(logrus.Fields{
			"action": "delete_old_attempts",
			"class":  class,
			"tenant": tenant,
		}).Errorf("delete old cloud attempts: %v", err)
	}
}

// deleteCloudAttempts deletes attempts lower than below, except keep and those a
// backup restores from. Attempts only go up, so a lagging node cannot touch a
// newer copy.
func deleteCloudAttempts(ctx context.Context, cloud modulecapabilities.OffloadCloud, class, tenant string, keep, below uint64) error {
	copies, err := cloud.ListCopies(ctx, class, tenant)
	if err != nil {
		return fmt.Errorf("list cloud copies: %w", err)
	}
	old := slices.DeleteFunc(copies, func(name string) bool {
		attempt, err := strconv.ParseUint(name, 10, 64)
		return err != nil || attempt == keep || attempt >= below
	})
	if len(old) == 0 {
		return nil
	}
	retained, err := cloud.Retained(ctx, class, tenant)
	if err != nil {
		return fmt.Errorf("list retained cloud copies: %w", err)
	}
	ec := errorcompounder.New()
	for _, name := range old {
		if !slices.Contains(retained, name) {
			ec.Add(cloud.Delete(ctx, class, tenant, name))
		}
	}
	return ec.ToError()
}

// removeStaging runs before any worker has started.
func (t *transitionManager) removeStaging() {
	t.m.db.forEachIndexOutsideIndexLock(func(idx *Index) {
		staged, err := filepath.Glob(filepath.Join(idx.path(), unfreezeStagingPrefix+"*"))
		if err != nil {
			t.m.logger.WithField("index", idx.ID()).Warnf("find unfreeze staging: %v", err)
			return
		}
		for _, dir := range staged {
			if err := os.RemoveAll(dir); err != nil {
				t.m.logger.WithField("path", dir).Warnf("remove unfreeze staging: %v", err)
			}
		}
	})
}

func cloudCopyPath(attempt uint64, node string) string {
	return strconv.FormatUint(attempt, 10) + "/" + node
}

func (i *Index) unfreezeStagingPath(name string, attempt uint64) string {
	return path.Join(i.path(), fmt.Sprintf("%s%s-%d", unfreezeStagingPrefix, name, attempt))
}

var errTenantInTransition = errors.New("tenant has a running freeze or unfreeze")

// refuseLoadDuringTransition keeps a shard's files still during an upload or
// swap.
func (i *Index) refuseLoadDuringTransition(name string, caller shardLoadCaller) error {
	if caller == callerMovement || i.schemaReader == nil {
		return nil
	}
	phys, ok, err := tenantPhysical(i, i.Config.ClassName.String(), name)
	if err != nil || !ok || phys.Transition == nil {
		return nil
	}
	return fmt.Errorf("load shard %q: %w", name, errTenantInTransition)
}
