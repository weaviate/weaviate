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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication/types"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/config"
)

// draftHeartbeatInterval is the time between progress reports of a running
// draft. Each report restarts the scheduler's stale-unit timer. Keep this
// interval well below the draft task's stale timeout.
const draftHeartbeatInterval = time.Minute

// draftShardConcurrency caps how many shards one draft reads at once, counted
// across all collections.
const draftShardConcurrency = 4

// rebalanceDraftSizer is the part of *DB that the draft provider uses to size
// shards.
type rebalanceDraftSizer interface {
	UsageForIndex(ctx context.Context, className schema.ClassName, shardReadSem *semaphore.Weighted,
		exactObjectCount bool, vectorsConfig map[string]models.VectorConfig) (*usagetypes.CollectionUsage, error)
}

// rebalanceDraft is the file a node saves once it has sized its shards.
type rebalanceDraft struct {
	TaskID  string                     `json:"taskId"`
	Version uint64                     `json:"version"`
	Shards  []types.RebalanceShardSize `json:"shards"`
}

// RebalanceDraftProvider runs rebalance draft tasks. Each unit is one listed
// node, with the node name as its ID. The local unit sizes this node's shards
// and saves them under {dataPath}/.dtm/replication_rebalance_draft/. The unit
// completes only once that file is durable.
type RebalanceDraftProvider struct {
	recorder  distributedtask.TaskCompletionRecorder
	sizer     rebalanceDraftSizer
	classes   func() []*models.Class
	dir       string
	localNode string
	logger    logrus.FieldLogger
	serverCtx context.Context
	// syncDir fsyncs a directory. It is a field so a test can assert which
	// directories are synced before a completion is recorded.
	syncDir func(string) error
}

// NewRebalanceDraftProvider builds the provider. classes returns the
// collections to size. serverCtx bounds every draft: a shutdown aborts sizing
// and records no result.
func NewRebalanceDraftProvider(sizer rebalanceDraftSizer, classes func() []*models.Class,
	dataPath, localNode string, logger logrus.FieldLogger, serverCtx context.Context,
) *RebalanceDraftProvider {
	return &RebalanceDraftProvider{
		sizer:     sizer,
		classes:   classes,
		dir:       draftDir(dataPath),
		localNode: localNode,
		logger:    logger.WithField("namespace", types.RebalanceDraftNamespace),
		serverCtx: serverCtx,
		syncDir:   diskio.Fsync,
	}
}

func (p *RebalanceDraftProvider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.recorder = recorder
}

// GetLocalTasks returns nil on purpose. On restart, the scheduler calls
// CleanupTask for each task listed here that is not STARTED with local work.
// Listing none keeps a draft on disk after its completion is recorded, until
// the leader collects it.
func (p *RebalanceDraftProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	return nil
}

// CleanupTask does nothing. StartTask deletes every draft except its own
// task's.
func (p *RebalanceDraftProvider) CleanupTask(distributedtask.TaskDescriptor) error {
	return nil
}

func (p *RebalanceDraftProvider) StartTask(task *distributedtask.Task) (distributedtask.TaskHandle, error) {
	p.pruneDrafts(task.TaskDescriptor)

	unit, ok := task.Units[p.localNode]
	if !ok || unit.Status == distributedtask.UnitStatusCompleted || unit.Status == distributedtask.UnitStatusFailed {
		return newDoneDraftHandle(), nil
	}

	ctx, cancel := context.WithCancel(p.serverCtx)
	handle := &rebalanceDraftHandle{cancel: cancel, doneCh: make(chan struct{})}
	enterrors.GoWrapper(func() {
		defer func() {
			cancel()
			close(handle.doneCh)
		}()
		p.run(ctx, task)
	}, p.logger)
	return handle, nil
}

func (p *RebalanceDraftProvider) run(ctx context.Context, task *distributedtask.Task) {
	logger := p.logger.WithField("task", task.ID).WithField("version", task.Version)
	name := draftFileName(task.TaskDescriptor)

	// A draft file found here has synced content, but its dir entry may not
	// be synced. An earlier attempt can rename the file into place and then
	// fail the dir fsync. Syncing the dirs again makes the file durable
	// without sizing again.
	if _, err := os.Stat(filepath.Join(p.dir, name)); err == nil {
		if err := p.syncDirs(); err != nil {
			p.recordRetryableFailure(ctx, task, logger, err)
			return
		}
		p.recordCompletion(ctx, task, logger)
		return
	}

	if err := p.recorder.UpdateDistributedTaskUnitProgress(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, p.localNode, 0); err != nil {
		logger.Warnf("rebalance draft: claim progress report failed: %v", err)
	}

	shards, err := p.sizeWithHeartbeat(ctx, task, logger)
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = p.writeDraft(name, rebalanceDraft{TaskID: task.ID, Version: task.Version, Shards: shards})
	}
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		p.recordRetryableFailure(ctx, task, logger, err)
		return
	}
	// A Terminate that arrives after the ctx check above can still be followed
	// by this completion record. The record is true: the draft file is already
	// durable.
	p.recordCompletion(ctx, task, logger)
}

func (p *RebalanceDraftProvider) recordCompletion(ctx context.Context, task *distributedtask.Task, logger logrus.FieldLogger) {
	if err := p.recorder.RecordDistributedTaskUnitCompletion(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, p.localNode); err != nil {
		logger.Errorf("rebalance draft: record unit completion failed: %v", err)
	}
}

func (p *RebalanceDraftProvider) recordRetryableFailure(ctx context.Context, task *distributedtask.Task,
	logger logrus.FieldLogger, err error,
) {
	logger.Errorf("rebalance draft: drafting failed: %v", err)
	if rerr := p.recorder.RecordDistributedTaskRetryableUnitFailure(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, p.localNode, err.Error()); rerr != nil {
		logger.Errorf("rebalance draft: record unit failure failed: %v", rerr)
	}
}

// sizeWithHeartbeat sizes every collection. While sizing runs, it reports
// progress once per draftHeartbeatInterval, as the fraction of collections
// done.
func (p *RebalanceDraftProvider) sizeWithHeartbeat(ctx context.Context, task *distributedtask.Task,
	logger logrus.FieldLogger,
) ([]types.RebalanceShardSize, error) {
	classes := p.classes()
	var (
		done     atomic.Int64
		shards   []types.RebalanceShardSize
		err      error
		finished = make(chan struct{})
	)
	enterrors.GoWrapper(func() {
		defer close(finished)
		shards, err = p.size(ctx, classes, &done)
	}, logger)

	ticker := time.NewTicker(draftHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-finished:
			return shards, err
		case <-ticker.C:
			if ctx.Err() != nil {
				continue
			}
			progress := float32(done.Load()) / float32(len(classes))
			if rerr := p.recorder.UpdateDistributedTaskUnitProgress(ctx, task.Namespace, task.ID,
				task.Version, p.localNode, p.localNode, progress); rerr != nil {
				logger.Warnf("rebalance draft: progress report failed: %v", rerr)
			}
		}
	}
}

// size returns the full storage size of every local shard. It skips a
// collection with no index on this node, and one deleted from the schema
// during sizing.
func (p *RebalanceDraftProvider) size(ctx context.Context, classes []*models.Class, done *atomic.Int64,
) ([]types.RebalanceShardSize, error) {
	shardReadSem := semaphore.NewWeighted(draftShardConcurrency)
	var (
		mu     sync.Mutex
		shards []types.RebalanceShardSize
	)
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(p.logger, ctx)
	eg.SetLimit(draftShardConcurrency)
	for _, class := range classes {
		eg.Go(func() error {
			defer done.Add(1)
			vectorConfigs, err := config.ExtractVectorConfigs(class)
			if err != nil {
				return fmt.Errorf("collection %s: %w", class.Class, err)
			}
			usage, err := p.sizer.UsageForIndex(egCtx, schema.ClassName(class.Class), shardReadSem, false, vectorConfigs)
			if errors.Is(err, clusterSchema.ErrClassNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("collection %s: %w", class.Class, err)
			}
			if usage == nil {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			for _, shard := range usage.Shards {
				shards = append(shards, types.RebalanceShardSize{
					Collection: class.Class,
					Shard:      shard.Name,
					Bytes:      int64(shard.FullShardStorageBytes),
				})
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return shards, nil
}

// writeDraft saves the draft as name in the draft dir and makes it durable.
// It creates {dataPath}/.dtm and the draft dir, and fsyncs the parent of
// each. Then it writes a temp file, fsyncs it, renames it into place and
// fsyncs the draft dir.
func (p *RebalanceDraftProvider) writeDraft(name string, draft rebalanceDraft) error {
	content, err := json.Marshal(draft)
	if err != nil {
		return fmt.Errorf("marshal draft: %w", err)
	}
	if err := p.mkdirSynced(); err != nil {
		return fmt.Errorf("create draft dir: %w", err)
	}
	if err := writeFileAtomic(p.dir, name, content); err != nil {
		return fmt.Errorf("write draft: %w", err)
	}
	return nil
}

// mkdirSynced creates {dataPath}/.dtm, then the draft dir, and fsyncs the
// parent of each. It matches MigrationRecordStore.mkdirSynced. It syncs the
// parent even when the dir already exists, because the process that created
// it may have crashed before syncing.
func (p *RebalanceDraftProvider) mkdirSynced() error {
	for _, dir := range []string{filepath.Dir(p.dir), p.dir} {
		if err := os.Mkdir(dir, 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		if err := p.syncDir(filepath.Dir(dir)); err != nil {
			return fmt.Errorf("sync %s: %w", filepath.Dir(dir), err)
		}
	}
	return nil
}

// syncDirs fsyncs {dataPath}, {dataPath}/.dtm and the draft dir, in that order.
func (p *RebalanceDraftProvider) syncDirs() error {
	for _, dir := range []string{filepath.Dir(filepath.Dir(p.dir)), filepath.Dir(p.dir), p.dir} {
		if err := p.syncDir(dir); err != nil {
			return fmt.Errorf("sync %s: %w", dir, err)
		}
	}
	return nil
}

// pruneDrafts deletes every entry of the draft dir except keep's draft file.
func (p *RebalanceDraftProvider) pruneDrafts(keep distributedtask.TaskDescriptor) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			p.logger.WithField("path", p.dir).Warnf("rebalance draft: list drafts to prune: %v", err)
		}
		return
	}
	keepName := draftFileName(keep)
	for _, entry := range entries {
		if entry.Name() == keepName {
			continue
		}
		path := filepath.Join(p.dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			p.logger.WithField("path", path).Warnf("rebalance draft: prune draft: %v", err)
		}
	}
}

// ReadRebalanceDraft returns the shard sizes this node saved for draft task
// taskID at version. When no such draft exists, the error matches os.ErrNotExist.
func ReadRebalanceDraft(dataPath, taskID string, version uint64) ([]types.RebalanceShardSize, error) {
	name := draftFileName(distributedtask.TaskDescriptor{ID: taskID, Version: version})
	content, err := os.ReadFile(filepath.Join(draftDir(dataPath), name))
	if err != nil {
		return nil, fmt.Errorf("read draft: %w", err)
	}
	var draft rebalanceDraft
	if err := json.Unmarshal(content, &draft); err != nil {
		return nil, fmt.Errorf("unmarshal draft %s: %w", name, err)
	}
	return draft.Shards, nil
}

func draftDir(dataPath string) string {
	return filepath.Join(dataPath, ".dtm", types.RebalanceDraftNamespace)
}

func draftFileName(desc distributedtask.TaskDescriptor) string {
	return fmt.Sprintf("%s-%d.json", desc.ID, desc.Version)
}

var _ distributedtask.TaskHandle = (*rebalanceDraftHandle)(nil)

type rebalanceDraftHandle struct {
	cancel context.CancelFunc
	doneCh chan struct{}
}

func newDoneDraftHandle() *rebalanceDraftHandle {
	h := &rebalanceDraftHandle{cancel: func() {}, doneCh: make(chan struct{})}
	close(h.doneCh)
	return h
}

func (h *rebalanceDraftHandle) Terminate()            { h.cancel() }
func (h *rebalanceDraftHandle) Done() <-chan struct{} { return h.doneCh }
