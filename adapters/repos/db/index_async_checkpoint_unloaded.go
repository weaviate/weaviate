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
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// unloadedCheckpoint: an unloaded shard takes no writes, so its persisted root is its root at any later cutoff.
type unloadedCheckpoint struct {
	cutoffMs  int64
	createdAt time.Time
	root      hashtree.Digest
	// filename is consumed by any load, so its presence proves the shard stayed unloaded since create.
	filename string
	// activatedAt is local-clock, like Shard.asyncCheckpointActivatedAt.
	activatedAt time.Time
}

// unloadedCheckpointLogThrottle keeps a class of orphaned snapshots, refused on every backup, to one Warn per window.
var unloadedCheckpointLogThrottle = replica.NewLogThrottle(time.Minute)

// unloadedCheckpointSweepInterval bounds put's full expiry sweep so a create fan-out over N tenants stays O(N).
const unloadedCheckpointSweepInterval = time.Minute

type unloadedCheckpointRegistry struct {
	mu        sync.Mutex
	entries   map[string]unloadedCheckpoint
	lastSweep time.Time
}

// put mirrors Shard.CreateAsyncCheckpoint's stale and past-cutoff checks.
func (r *unloadedCheckpointRegistry) put(shardName string, cp unloadedCheckpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if now.Sub(r.lastSweep) >= unloadedCheckpointSweepInterval {
		r.lastSweep = now
		for name, prev := range r.entries {
			if prev.expired(now) {
				delete(r.entries, name)
			}
		}
	}
	if prev, ok := r.entries[shardName]; ok && !prev.expired(now) && !cp.createdAt.After(prev.createdAt) {
		return errAsyncCheckpointStale
	}
	if cp.cutoffMs <= time.Now().UnixMilli() {
		return errAsyncCheckpointCutoffInPast
	}
	if r.entries == nil {
		r.entries = make(map[string]unloadedCheckpoint)
	}
	cp.activatedAt = now
	r.entries[shardName] = cp
	return nil
}

// get treats an expired entry as absent and drops it.
func (r *unloadedCheckpointRegistry) get(shardName string) (unloadedCheckpoint, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp, ok := r.entries[shardName]
	if ok && cp.expired(time.Now()) {
		delete(r.entries, shardName)
		return unloadedCheckpoint{}, false
	}
	return cp, ok
}

func (cp unloadedCheckpoint) expired(now time.Time) bool {
	return now.Sub(cp.activatedAt) > replica.AsyncCheckpointMaxLifetime
}

func (r *unloadedCheckpointRegistry) delete(shardName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, shardName)
}

type unloadedShardState int

const (
	shardNotInMap unloadedShardState = iota
	shardUnloadedInMap
	shardLoaded
	shardRecovering
)

// withUnloadedShard pins the shard unloaded for fn: teardown and activation hold shardCreateLocks, a lazy load holds its mutex.
func (i *Index) withUnloadedShard(shardName string, fn func(state unloadedShardState) error) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()
	if i.closed {
		return fmt.Errorf("local shard %q: %w", shardName, errAlreadyShutdown)
	}
	i.shardCreateLocks.RLock(shardName)
	defer i.shardCreateLocks.RUnlock(shardName)

	switch sl := i.shards.Load(shardName).(type) {
	case nil:
		return fn(shardNotInMap)
	case *RecoveringShard:
		return fn(shardRecovering)
	case *LazyLoadShard:
		release := sl.blockLoading()
		defer release()
		if sl.loaded {
			return fn(shardLoaded)
		}
		return fn(shardUnloadedInMap)
	default:
		return fn(shardLoaded)
	}
}

// createUnloadedAsyncCheckpoint keeps today's answers when no persisted root is usable: nil off-map, 412 in-map.
func (i *Index) createUnloadedAsyncCheckpoint(ctx context.Context, shardName string, cutoffMs int64, createdAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	logger := i.logger.WithFields(logrus.Fields{
		"action": "async_checkpoint_local",
		"op":     "create",
		"class":  i.Config.ClassName,
		"shard":  shardName,
	})
	return i.withUnloadedShard(shardName, func(state unloadedShardState) error {
		if state == shardRecovering {
			return fmt.Errorf("%w: shard %q: %w", errAsyncReplicationNotActive, shardName, enterrors.ErrShardRecovering)
		}
		if state == shardLoaded {
			return fmt.Errorf("%w: shard %q loaded concurrently", errAsyncReplicationNotActive, shardName)
		}
		notActive := func(reason string) error {
			if state == shardNotInMap {
				return nil
			}
			return fmt.Errorf("%w: shard %q not loaded on this node: %s", errAsyncReplicationNotActive, shardName, reason)
		}

		root, filename, err := newestPersistedHashTreeRoot(i.shardPathHashTree(shardName))
		if err != nil {
			if !errors.Is(err, errNoPersistedHashtree) {
				logger.Debugf("persisted hashtree unusable for checkpoint: %v", err)
			}
			return notActive(err.Error())
		}
		if err := persistedHashtreeHasObjectStore(i.path(), shardName); err != nil {
			if ok, suppressed := unloadedCheckpointLogThrottle.Allow(i.Config.ClassName.String()); ok {
				logger.WithField("suppressed", suppressed).Warnf("persisted hashtree refused for checkpoint: %v", err)
			} else {
				logger.Debugf("persisted hashtree refused for checkpoint: %v", err)
			}
			return notActive(err.Error())
		}
		if enabled, _ := i.asyncReplicationStateForShard(shardName); !enabled {
			return notActive("async replication disabled")
		}
		if err := i.unloadedCheckpoints.put(shardName, unloadedCheckpoint{
			cutoffMs: cutoffMs, createdAt: createdAt, root: root, filename: filename,
		}); err != nil {
			return err
		}
		logger.WithField("cutoff_ms", cutoffMs).WithField("file", filename).
			Debug("async checkpoint answered from persisted hashtree")
		return nil
	})
}

// unloadedAsyncCheckpointStatus omits the entry unless the shard is still unloaded and its .ht still exists.
func (i *Index) unloadedAsyncCheckpointStatus(shardName string) (replica.AsyncCheckpointShardStatus, bool) {
	cp, ok := i.unloadedCheckpoints.get(shardName)
	if !ok {
		return replica.AsyncCheckpointShardStatus{}, false
	}
	logger := i.logger.WithFields(logrus.Fields{
		"action": "async_checkpoint_local",
		"op":     "status",
		"class":  i.Config.ClassName,
		"shard":  shardName,
	})
	found := false
	err := i.withUnloadedShard(shardName, func(state unloadedShardState) error {
		if state == shardLoaded || state == shardRecovering {
			i.unloadedCheckpoints.delete(shardName)
			return nil
		}
		if _, err := os.Stat(cp.filename); err != nil {
			logger.Debugf("persisted hashtree gone since checkpoint create; omitting: %v", err)
			return nil
		}
		found = true
		return nil
	})
	if err != nil {
		logger.Debugf("get shard failed; skipping: %v", err)
		return replica.AsyncCheckpointShardStatus{}, false
	}
	if !found {
		return replica.AsyncCheckpointShardStatus{}, false
	}
	return replica.AsyncCheckpointShardStatus{Root: cp.root, CutoffMs: cp.cutoffMs, CreatedAt: cp.createdAt}, true
}
