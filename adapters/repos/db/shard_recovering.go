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
	"io/fs"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// RecoveringShard blocks Load until promotion swaps it out of the map; mustLoad paths PANIC by design (docs/self-recovery.md).
type RecoveringShard struct {
	*LazyLoadShard
}

func NewRecoveringShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	memMonitor memwatch.AllocChecker,
	shardLoadLimiter *loadlimiter.LoadLimiter, shardReindexer ShardReindexerV3,
	bitmapBufPool roaringset.BitmapBufPool,
) *RecoveringShard {
	inner := NewLazyLoadShard(ctx, promMetrics, shardName, index, class, jobQueueCh,
		memMonitor, shardLoadLimiter, shardReindexer,
		false, bitmapBufPool)
	inner.blockLoad(enterrors.ErrShardRecovering)
	return &RecoveringShard{LazyLoadShard: inner}
}

// asLazyLoadShard unwraps both wrappers; a concrete *LazyLoadShard assert misclassifies a *RecoveringShard.
func asLazyLoadShard(s ShardLike) (*LazyLoadShard, bool) {
	switch v := s.(type) {
	case *LazyLoadShard:
		return v, true
	case *RecoveringShard:
		return v.LazyLoadShard, true
	}
	return nil, false
}

// unblock: only once the copy is renamed into the live dir.
func (r *RecoveringShard) unblock() *LazyLoadShard {
	r.clearLoadBlock()
	return r.LazyLoadShard
}

func (r *RecoveringShard) IsRecovering() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return !r.loaded
}

// GetStatus reports RECOVERING (not LAZY_LOADING) while unloaded.
func (r *RecoveringShard) GetStatus() storagestate.Status {
	r.mutex.Lock()
	loaded := r.loaded
	inner := r.shard
	r.mutex.Unlock()
	if loaded {
		return inner.GetStatus()
	}
	return storagestate.StatusRecovering
}

func (r *RecoveringShard) GetStatusReason() string {
	r.mutex.Lock()
	loaded := r.loaded
	inner := r.shard
	r.mutex.Unlock()
	if loaded {
		return inner.GetStatusReason()
	}
	return storagestate.StatusRecovering.String()
}

// DemoteRecoveredLocalShard undoes a SELF_RECOVERY promote before a rewound
// re-copy: it shuts the promoted shard down, renames "<shard>/" back to
// "<shard>.recovering/" and re-installs the load block. The rename keeps the
// files for an incremental re-copy and leaves no torn live dir on a crash.
// No-op without a live dir; a shard absent from the map (cold tenant) stays absent.
func (i *Index) DemoteRecoveredLocalShard(ctx context.Context, shardName string) error {
	if err := i.enterRead(); err != nil {
		return err
	}
	defer i.exitRead()

	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	livePath := shardPath(i.path(), shardName)
	if _, err := os.Stat(livePath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("demote local shard %q: stat live dir: %w", shardName, err)
	}

	reinstall, err := i.shutDownPromotedShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("demote local shard %q: %w", shardName, err)
	}

	recoveryPath := livePath + api.RecoveryFolderSuffix
	if err := os.RemoveAll(recoveryPath); err != nil {
		return fmt.Errorf("demote local shard %q: remove stale %q: %w", shardName, recoveryPath, err)
	}
	if err := os.Rename(livePath, recoveryPath); err != nil {
		return fmt.Errorf("demote local shard %q: rename %q -> %q: %w", shardName, livePath, recoveryPath, err)
	}
	if err := diskio.Fsync(filepath.Dir(livePath)); err != nil {
		return fmt.Errorf("demote local shard %q: fsync parent: %w", shardName, err)
	}

	if reinstall {
		var promMetrics *monitoring.PrometheusMetrics
		if i.metrics != nil {
			promMetrics = i.metrics.baseMetrics
		}
		i.installRecoveringShard(ctx, i.getSchema.ReadOnlyClass(i.Config.ClassName.String()), shardName, promMetrics)
	}
	return nil
}

// shutDownPromotedShard shuts a promoted shard down and takes it out of the
// map, reporting whether a load block must replace it. A still-blocked
// RecoveringShard holds nothing open and stays. Caller holds shardCreateLocks.
func (i *Index) shutDownPromotedShard(ctx context.Context, shardName string) (bool, error) {
	existing := i.shards.Load(shardName)
	if existing == nil {
		return false, nil
	}
	if rec, ok := existing.(*RecoveringShard); ok && rec.IsRecovering() {
		return false, nil
	}
	shard, ok := i.shards.LoadAndDelete(shardName)
	if !ok {
		return false, nil
	}
	shutdownCtx, done := i.cancelOnCloseRequested(ctx)
	defer done()
	if err := shutdownOrRestoreShard(shutdownCtx, i, shardName, shard); err != nil && !errors.Is(err, errAlreadyShutdown) {
		return false, fmt.Errorf("shut down promoted shard: %w", err)
	}
	return true, nil
}
