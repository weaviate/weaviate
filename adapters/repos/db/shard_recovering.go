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

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// RecoveringShard wraps a LazyLoadShard for SELF_RECOVERY. Until promoted via
// Load, inner Load is blocked with ErrShardRecovering so a lazy load can't
// MkdirAll an empty shard before the copy-and-rename completes, and GetStatus
// reports RECOVERING.
//
// While blocked, any inherited data-path method going through mustLoad PANICS
// by design; iterating callers must skip recovering shards. See
// docs/self-recovery.md ("Limitations").
type RecoveringShard struct {
	*LazyLoadShard
}

func NewRecoveringShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints, memMonitor memwatch.AllocChecker,
	shardLoadLimiter *loadlimiter.LoadLimiter, shardReindexer ShardReindexerV3,
	bitmapBufPool roaringset.BitmapBufPool,
) *RecoveringShard {
	inner := NewLazyLoadShard(ctx, promMetrics, shardName, index, class, jobQueueCh,
		indexCheckpoints, memMonitor, shardLoadLimiter, shardReindexer,
		false, bitmapBufPool)
	inner.blockLoad(enterrors.ErrShardRecovering)
	return &RecoveringShard{LazyLoadShard: inner}
}

// Load clears the recovery block, then loads.
func (r *RecoveringShard) Load(ctx context.Context) error {
	r.clearLoadBlock()
	return r.LazyLoadShard.Load(ctx)
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
