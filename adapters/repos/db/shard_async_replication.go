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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entreplication "github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/storobj"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	defaultAsyncReplicationMaxWorkersSingleTenant = 3
	defaultAsyncReplicationMaxWorkersMultiTenant  = 5

	defaultHashtreeHeightSingleTenant  = 16
	defaultHashtreeHeightMultiTenant   = 10
	defaultFrequency                   = 30 * time.Second
	defaultFrequencyWhilePropagating   = 5 * time.Second
	defaultAliveNodesCheckingFrequency = 10 * time.Second
	defaultLoggingFrequency            = 60 * time.Second
	defaultDiffBatchSize               = 1_000
	defaultDiffPerNodeTimeout          = 10 * time.Second
	defaultPrePropagationTimeout       = 300 * time.Second
	defaultPropagationTimeout          = 180 * time.Second
	defaultPropagationLimit            = 5_000
	defaultPropagationConcurrency      = 1
	defaultPropagationBatchSize        = 100

	minMaxWorkers = 1
	maxMaxWorkers = 10

	minHashtreeHeight = 0
	maxHashtreeHeight = 20

	minDiffBatchSize = 1
	maxDiffBatchSize = 10_000

	minPropagationLimit = 1
	maxPropagationLimit = 100_000

	minPropagationConcurrency = 1
	maxPropagationConcurrency = 20

	minPropagationBatchSize = 1
	maxPropagationBatchSize = 1_000

	defaultInitShieldCPUEveryN = 10_000
	minInitShieldCPUEveryN     = 1
	maxInitShieldCPUEveryN     = 1_000_000
)

// asyncReplicationClassOverrides holds per-class API values that take
// precedence over both code defaults and global runtime config. A nil pointer
// means "not set via API; fall back to global or code default".
type asyncReplicationClassOverrides struct {
	maxWorkers                  *int
	hashtreeHeight              *int
	frequency                   *time.Duration
	frequencyWhilePropagating   *time.Duration
	aliveNodesCheckingFrequency *time.Duration
	loggingFrequency            *time.Duration
	diffBatchSize               *int
	diffPerNodeTimeout          *time.Duration
	prePropagationTimeout       *time.Duration
	propagationTimeout          *time.Duration
	propagationLimit            *int
	propagationConcurrency      *int
	propagationBatchSize        *int
	initShieldCPUEveryN         *int
}

type AsyncReplicationConfig struct {
	maxWorkers                  int
	hashtreeHeight              int
	frequency                   time.Duration
	frequencyWhilePropagating   time.Duration
	aliveNodesCheckingFrequency time.Duration
	loggingFrequency            time.Duration
	diffBatchSize               int
	diffPerNodeTimeout          time.Duration
	prePropagationTimeout       time.Duration
	propagationTimeout          time.Duration
	propagationLimit            int
	propagationConcurrency      int
	propagationBatchSize        int
	// initShieldCPUEveryN is the number of objects processed between
	// runtime.Gosched() calls during the on-disk phase of hashtree
	// initialization. Yielding periodically lets query goroutines make forward
	// progress and prevents the init scan from monopolising CPU during the
	// (potentially long) full-shard scan.
	initShieldCPUEveryN int

	// classOverrides holds per-class API values. They are merged in by
	// Effective() and cleared in the returned snapshot.
	classOverrides asyncReplicationClassOverrides
}

// Effective merges the three-tier config hierarchy and returns a snapshot
// ready for use in a single hashbeat cycle:
//
//  1. Code defaults (already set in the receiver)
//  2. Per-class schema/API overrides (non-nil pointer beats the code default)
//  3. Global runtime-config DynamicValues (> 0 beats everything — highest priority)
//
// This means an operator can use a runtime-config override to temporarily force
// a value cluster-wide; removing it (resetting to 0) restores the per-class
// schema value without any permanent schema mutation.
//
// The returned config has classOverrides cleared (they have been applied).
func (c AsyncReplicationConfig) Effective(globals entreplication.GlobalConfig) AsyncReplicationConfig {
	result := c

	// Step 1: apply per-class schema/API overrides on top of code defaults.
	co := c.classOverrides
	if co.maxWorkers != nil {
		result.maxWorkers = *co.maxWorkers
	}
	if co.hashtreeHeight != nil {
		result.hashtreeHeight = *co.hashtreeHeight
	}
	if co.frequency != nil {
		result.frequency = *co.frequency
	}
	if co.frequencyWhilePropagating != nil {
		result.frequencyWhilePropagating = *co.frequencyWhilePropagating
	}
	if co.aliveNodesCheckingFrequency != nil {
		result.aliveNodesCheckingFrequency = *co.aliveNodesCheckingFrequency
	}
	if co.loggingFrequency != nil {
		result.loggingFrequency = *co.loggingFrequency
	}
	if co.diffBatchSize != nil {
		result.diffBatchSize = *co.diffBatchSize
	}
	if co.diffPerNodeTimeout != nil {
		result.diffPerNodeTimeout = *co.diffPerNodeTimeout
	}
	if co.prePropagationTimeout != nil {
		result.prePropagationTimeout = *co.prePropagationTimeout
	}
	if co.propagationTimeout != nil {
		result.propagationTimeout = *co.propagationTimeout
	}
	if co.propagationLimit != nil {
		result.propagationLimit = *co.propagationLimit
	}
	if co.propagationConcurrency != nil {
		result.propagationConcurrency = *co.propagationConcurrency
	}
	if co.propagationBatchSize != nil {
		result.propagationBatchSize = *co.propagationBatchSize
	}
	if co.initShieldCPUEveryN != nil {
		result.initShieldCPUEveryN = *co.initShieldCPUEveryN
	}

	// Step 2: apply global runtime-config DynamicValues (highest priority).
	// A zero value means "not configured at the cluster level"; the schema
	// value from step 1 is preserved in that case.
	//
	// Integer values are clamped to [minVal, maxVal] — the same bounds
	// enforced for API overrides — so a misconfigured runtime knob (e.g.
	// hashtreeHeight=1000) cannot cause excessive memory or an outright
	// failure when building the hashtree.
	applyInt := func(dv *configRuntime.DynamicValue[int], field *int, minVal, maxVal int) {
		if dv == nil {
			return
		}
		v := dv.Get()
		if v <= 0 {
			return
		}
		if v < minVal {
			v = minVal
		} else if v > maxVal {
			v = maxVal
		}
		*field = v
	}
	applyDur := func(dv *configRuntime.DynamicValue[time.Duration], field *time.Duration) {
		if dv == nil {
			return
		}
		if v := dv.Get(); v > 0 {
			*field = v
		}
	}
	applyInt(globals.AsyncReplicationMaxWorkers, &result.maxWorkers, minMaxWorkers, maxMaxWorkers)
	applyInt(globals.AsyncReplicationHashtreeHeight, &result.hashtreeHeight, minHashtreeHeight, maxHashtreeHeight)
	applyDur(globals.AsyncReplicationFrequency, &result.frequency)
	applyDur(globals.AsyncReplicationFrequencyWhilePropagating, &result.frequencyWhilePropagating)
	applyDur(globals.AsyncReplicationLoggingFrequency, &result.loggingFrequency)
	applyInt(globals.AsyncReplicationDiffBatchSize, &result.diffBatchSize, minDiffBatchSize, maxDiffBatchSize)
	applyDur(globals.AsyncReplicationDiffPerNodeTimeout, &result.diffPerNodeTimeout)
	applyDur(globals.AsyncReplicationPrePropagationTimeout, &result.prePropagationTimeout)
	applyDur(globals.AsyncReplicationPropagationTimeout, &result.propagationTimeout)
	applyInt(globals.AsyncReplicationPropagationLimit, &result.propagationLimit, minPropagationLimit, maxPropagationLimit)
	applyInt(globals.AsyncReplicationPropagationConcurrency, &result.propagationConcurrency, minPropagationConcurrency, maxPropagationConcurrency)
	applyInt(globals.AsyncReplicationPropagationBatchSize, &result.propagationBatchSize, minPropagationBatchSize, maxPropagationBatchSize)
	applyInt(globals.AsyncReplicationInitShieldCPUEveryN, &result.initShieldCPUEveryN, minInitShieldCPUEveryN, maxInitShieldCPUEveryN)

	// Clear classOverrides in the snapshot — they have been applied.
	result.classOverrides = asyncReplicationClassOverrides{}
	return result
}

// initAsyncReplication initialises async-replication state for the shard.
// It must be called while holding asyncReplicationRWMux for writing.
//
// scheduler.Register is always called from a separate goroutine so that it
// never blocks while the write lock is held (the dispatcher acquires
// asyncReplicationRWMux.RLock inside onResultLocked, which would deadlock if
// we held the write lock across the channel send).
func (s *Shard) initAsyncReplication(config AsyncReplicationConfig) error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.asyncReplicationCancelFunc = cancelFunc
	s.asyncRepCtx = ctx

	// Initialise (or reset) per-cycle state so a restarted hashbeat starts
	// fresh without inheriting stale root snapshots from a previous run.
	s.asyncRepLastLocalRootByTarget = make(map[string]hashtree.Digest)
	s.asyncRepLastRemoteRootByTarget = make(map[string]hashtree.Digest)
	s.asyncRepLastPropagatedToTarget = make(map[string]bool)
	s.asyncRepLastLog = time.Time{}

	// Store the base config (with class overrides) so future Effective() calls
	// can re-merge it against updated runtime globals.
	s.asyncReplicationConfig = config

	// Compute the effective config to pick the correct hashtree height.
	// globals may be nil during tests that do not set up a full index.
	effectiveConfig := config
	if s.index != nil && s.index.globalreplicationConfig != nil {
		effectiveConfig = config.Effective(*s.index.globalreplicationConfig)
	}

	// Register flush-time hooks on the objects bucket:
	//   - objectFlushCallback updates the hashtree with exactly the objects
	//     that were durably persisted in the flush (before the new segment is
	//     visible to readers), keeping the hashtree consistent with on-disk data.
	//   - flushCallback wakes the hashbeater after the segment is added so that
	//     newly flushed objects are propagated without waiting for the next tick.
	if bucket != nil {
		bucket.SetObjectFlushCallback(s.updateHashtreeOnFlush)
		bucket.SetFlushCallback(s.notifyHashbeat)
	}

	start := time.Now()

	cached, err := s.tryLoadHashtreeFromDisk(effectiveConfig.hashtreeHeight)
	if err != nil {
		return err
	}

	if cached != nil {
		s.hashtree = cached
		s.hashtreeFullyInitialized = true
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("took", fmt.Sprintf("%v", time.Since(start))).
			Info("hashtree successfully initialized")

		// scheduler.Register must NOT be called under the write lock: it sends
		// on addCh which the dispatcher reads while it may hold
		// asyncReplicationRWMux.RLock (in onResultLocked), causing a deadlock.
		// Spawning a goroutine mirrors the pattern used by the new-hashtree path.
		//
		// hashbeatRegistered is set inside the goroutine (under the write lock)
		// immediately before Register() is called. This prevents a race where
		// disableAsyncReplication runs after initAsyncReplication returns but
		// before the goroutine executes: if the hashtree has been cleared (nil)
		// by then, registration is skipped entirely.
		enterrors.GoWrapper(func() {
			s.asyncReplicationRWMux.Lock()
			if s.hashtree == nil {
				// Async replication was disabled before this goroutine ran.
				s.asyncReplicationRWMux.Unlock()
				return
			}
			s.hashbeatRegistered = true
			s.asyncReplicationRWMux.Unlock()

			s.index.asyncReplicationScheduler.Register(s)

			// Guard against disableAsyncReplication running between the lock
			// release above and Register() returning. If disable ran in that
			// window, it called Deregister as a no-op (shard wasn't registered
			// yet) and set hashtree to nil. Self-deregister now so the disabled
			// shard is not left in the scheduler.
			s.asyncReplicationRWMux.RLock()
			stillRunning := s.hashtree != nil
			s.asyncReplicationRWMux.RUnlock()
			if !stillRunning {
				s.index.asyncReplicationScheduler.Deregister(s)
			}
		}, s.index.logger)
		return nil
	}

	s.hashtree, err = hashtree.NewHashTree(effectiveConfig.hashtreeHeight)
	if err != nil {
		return err
	}

	s.hashtreeFullyInitialized = false

	enterrors.GoWrapper(func() {
		for i := 0; ; i++ {
			err := s.initHashtree(ctx, effectiveConfig, bucket)
			if err == nil {
				break
			}

			if ctx.Err() != nil {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Info("hashtree initialization stopped")
				return
			}

			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree initialization attempt %d failure: %v", i, err)

			// exponential backoff: min(2^i * 100ms, 5s)
			backoff := min(time.Duration(1<<i)*100*time.Millisecond, 5*time.Second)
			time.Sleep(backoff)

			s.asyncReplicationRWMux.Lock()
			s.hashtree.Reset()
			s.asyncReplicationRWMux.Unlock()
		}
	}, s.index.logger)

	return nil
}

// tryLoadHashtreeFromDisk scans the shard's hashtree directory for a persisted
// hashtree file, loads the most-recent one, and removes it from disk (along
// with any older duplicates). Returns nil without error when no file is found
// or when the cached height does not match expectedHeight (the old tree is
// simply discarded in that case).
func (s *Shard) tryLoadHashtreeFromDisk(expectedHeight int) (hashtree.AggregatedHashTree, error) {
	if err := os.MkdirAll(s.pathHashTree(), os.ModePerm); err != nil {
		return nil, err
	}

	dirEntries, err := os.ReadDir(s.pathHashTree())
	if err != nil {
		return nil, err
	}

	var loaded hashtree.AggregatedHashTree

	for i := len(dirEntries) - 1; i >= 0; i-- {
		dirEntry := dirEntries[i]
		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".ht" {
			continue
		}

		filename := filepath.Join(s.pathHashTree(), dirEntry.Name())

		if loaded != nil {
			// A newer file was already loaded; remove this older duplicate.
			if err := os.Remove(filename); err != nil {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Warnf("deleting older hashtree file %q: %v", filename, err)
			} else {
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Debugf("deleted older hashtree file %q", filename)
			}
			continue
		}

		f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("opening hashtree file %q: %v", filename, err)
			continue
		}

		loaded, err = hashtree.DeserializeHashTree(bufio.NewReader(f))
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("deserializing hashtree file %q: %v", filename, err)
		}

		if err := f.Close(); err != nil {
			return nil, err
		}
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
		if err := diskio.Fsync(s.pathHashTree()); err != nil {
			return nil, fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
		}
	}

	if loaded != nil && loaded.Height() != expectedHeight {
		// Height mismatch — discard; a fresh scan will be performed instead.
		return nil, nil
	}

	return loaded, nil
}

func (s *Shard) initHashtree(ctx context.Context, config AsyncReplicationConfig, bucket *lsmkv.Bucket) (err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationHashTreeInitCount()
	s.metrics.IncAsyncReplicationHashTreeInitRunning()

	defer func() {
		s.metrics.DecAsyncReplicationHashTreeInitRunning()

		if err != nil {
			s.metrics.IncAsyncReplicationHashTreeInitFailure()
			return
		}

		s.metrics.ObserveAsyncReplicationHashTreeInitDuration(time.Since(start))
	}()

	// Scan only on-disk segments: the hashtree now reflects durable (flushed)
	// data exclusively. In-memory objects will be added when their memtables are
	// flushed via updateHashtreeOnFlush.
	objCount := 0
	prevProgressLogging := time.Now()

	err = bucket.ApplyToOnDiskObjectDigests(ctx, func(uuidBytes []byte, updateTime int64) error {
		if time.Since(prevProgressLogging) >= config.loggingFrequency {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				WithField("object_count", objCount).
				WithField("took", fmt.Sprintf("%v", time.Since(start))).
				Infof("hashtree initialization in progress...")
			prevProgressLogging = time.Now()
		}

		obj := &storobj.Object{}
		obj.Object.LastUpdateTimeUnix = updateTime
		s.asyncReplicationRWMux.RLock()
		err := s.mayUpsertObjectHashTree(obj, uuidBytes, objectInsertStatus{})
		s.asyncReplicationRWMux.RUnlock()
		if err != nil {
			return err
		}

		objCount++

		// Yield to the Go scheduler periodically: on-disk scans can run for
		// minutes on large shards and starve query goroutines on the same thread.
		if config.initShieldCPUEveryN > 0 && objCount%config.initShieldCPUEveryN == 0 {
			runtime.Gosched()
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("iterating objects: %w", err)
	}

	s.asyncReplicationRWMux.Lock()

	if s.hashtree == nil {
		s.asyncReplicationRWMux.Unlock()
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree initialization stopped")
		return nil
	}

	s.hashtreeFullyInitialized = true
	s.hashbeatRegistered = true
	s.asyncReplicationRWMux.Unlock()

	// Register is called outside the write lock: it sends on a channel received
	// by the dispatcher goroutine, and concurrent object-write goroutines acquire
	// the read lock — holding the write lock here would stall them.
	s.index.logger.
		WithField("action", "async_replication").
		WithField("class_name", s.class.Class).
		WithField("shard_name", s.name).
		WithField("object_count", objCount).
		WithField("took", fmt.Sprintf("%v", time.Since(start))).
		Info("hashtree successfully initialized")

	s.index.asyncReplicationScheduler.Register(s)

	// Guard against disableAsyncReplication running between the lock release
	// above and Register() returning. If disable ran in that window, it called
	// Deregister as a no-op (shard wasn't registered yet) and set hashtree to
	// nil. Self-deregister now so the disabled shard is not left in the scheduler.
	s.asyncReplicationRWMux.RLock()
	stillRunning := s.hashtree != nil
	s.asyncReplicationRWMux.RUnlock()
	if !stillRunning {
		s.index.asyncReplicationScheduler.Deregister(s)
	}

	return nil
}

// updateHashtreeOnFlush is called by the lsmkv objects bucket inside
// FlushAndSwitch, after the flushing memtable has been durably written to disk
// but before the new segment is added to the segment group.
//
// For each entry in the flushed memtable it computes the XOR delta needed to
// keep the hashtree consistent with on-disk data:
//   - XOR out the previous on-disk digest (if the key existed on disk before).
//   - XOR in the new digest (unless the entry is a tombstone / deletion).
//
// Deltas are collected without holding any lock (lookupOnDisk is safe for
// concurrent reads) and then applied atomically under the write lock to
// minimise the time writes are stalled.
func (s *Shard) updateHashtreeOnFlush(
	forEachFlushedObject func(fn func(key []byte, value []byte, tombstone bool)),
	lookupOnDisk func(key []byte) ([]byte, bool),
) {
	type leafDelta struct {
		leaf   uint64
		digest [16 + 8]byte
	}

	var deltas []leafDelta

	forEachFlushedObject(func(key []byte, value []byte, tombstone bool) {
		if len(key) != 16 {
			return
		}

		leaf := s.hashtreeLeafFor(key)

		// XOR out old on-disk value (if any).
		if oldValue, found := lookupOnDisk(key); found {
			_, oldUpdateTime, err := storobj.DocIDAndTimeFromBinary(oldValue)
			if err == nil && oldUpdateTime > 0 {
				var d [16 + 8]byte
				copy(d[:16], key)
				binary.BigEndian.PutUint64(d[16:], uint64(oldUpdateTime))
				deltas = append(deltas, leafDelta{leaf, d})
			}
		}

		// XOR in new value unless this entry is a deletion tombstone.
		if !tombstone {
			_, newUpdateTime, err := storobj.DocIDAndTimeFromBinary(value)
			if err == nil && newUpdateTime > 0 {
				var d [16 + 8]byte
				copy(d[:16], key)
				binary.BigEndian.PutUint64(d[16:], uint64(newUpdateTime))
				deltas = append(deltas, leafDelta{leaf, d})
			}
		}
	})

	if len(deltas) == 0 {
		return
	}

	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if s.hashtree == nil {
		return
	}

	for _, delta := range deltas {
		s.hashtree.AggregateLeafWith(delta.leaf, delta.digest[:])
	}
}

func (s *Shard) mayStopAsyncReplication() {
	s.asyncReplicationRWMux.Lock()

	if s.hashtree == nil {
		s.asyncReplicationRWMux.Unlock()
		return
	}

	s.asyncReplicationCancelFunc()

	// Capture the hashtree pointer and dump eligibility before clearing state
	// so that dumpHashTreeOf can be called outside the write lock. Holding the
	// write lock during I/O would block all concurrent reads (e.g. hashbeat)
	// for the duration of the serialization.
	var capturedHT hashtree.AggregatedHashTree
	if s.hashtreeFullyInitialized && !s.hashtreeFlushFailed {
		capturedHT = s.hashtree
	}

	s.hashtree = nil
	s.hashtreeFullyInitialized = false
	s.hashtreeFlushFailed = false
	s.hashbeatRegistered = false

	if bucket := s.store.Bucket(helpers.ObjectsBucketLSM); bucket != nil {
		bucket.SetObjectFlushCallback(nil)
		bucket.SetFlushCallback(nil)
	}
	s.asyncReplicationRWMux.Unlock()

	// Remove from the scheduler and wait for any in-flight cycle to finish
	// before touching the hashtree (which the cycle may be reading).
	s.index.asyncReplicationScheduler.Deregister(s)
	s.asyncRepWg.Wait()

	if capturedHT != nil {
		// the hashtree needs to be fully in sync with stored data before it can be persisted
		if err := s.dumpHashTreeOf(capturedHT); err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("store hashtree failed: %v", err)
		}
	}
}

func (s *Shard) enableAsyncReplication(_ context.Context, config AsyncReplicationConfig) error {
	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if s.index.asyncReplicationScheduler == nil {
		return fmt.Errorf("async replication scheduler is nil for index %q", s.index.ID())
	}

	if s.hashtree != nil {
		// Already running. Update the stored base config so the next hashbeat
		// cycle picks up any changed settings (e.g. classOverrides cleared via
		// API). The scheduler's height-detection in runEntry will signal a
		// rebuild via asyncRepNeedsRebuild if the effective hashtreeHeight
		// changed; non-height settings take effect on the next Effective() call.
		s.asyncReplicationConfig = config
		return nil
	}
	return s.initAsyncReplication(config)
}

func (s *Shard) disableAsyncReplication(_ context.Context) error {
	var afterRelease func()
	var needDeregister bool

	err := func() error {
		s.asyncReplicationRWMux.Lock()
		defer s.asyncReplicationRWMux.Unlock()

		if s.hashtree == nil {
			return nil // already stopped, idempotent
		}

		s.asyncReplicationCancelFunc()

		// Capture before clearing so afterRelease can persist the hashtree
		// outside the write lock (same pattern as mayStopAsyncReplication).
		var capturedHT hashtree.AggregatedHashTree
		if s.hashtreeFullyInitialized && !s.hashtreeFlushFailed {
			capturedHT = s.hashtree
		}

		s.hashtree = nil
		s.hashtreeFullyInitialized = false
		s.hashtreeFlushFailed = false
		s.hashbeatRegistered = false
		if bucket := s.store.Bucket(helpers.ObjectsBucketLSM); bucket != nil {
			bucket.SetObjectFlushCallback(nil)
			bucket.SetFlushCallback(nil)
		}
		s.asyncReplicationStatsMux.Lock()
		s.asyncReplicationStatsByTargetNode = nil
		s.asyncReplicationStatsMux.Unlock()

		needDeregister = true

		if capturedHT != nil {
			afterRelease = func() {
				if err := s.dumpHashTreeOf(capturedHT); err != nil {
					s.index.logger.
						WithField("action", "async_replication").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						Errorf("store hashtree failed: %v", err)
				}
			}
		}

		return nil
	}()
	if err != nil {
		return err
	}
	if needDeregister {
		// Remove from the scheduler and wait for any in-flight cycle to finish.
		s.index.asyncReplicationScheduler.Deregister(s)
		s.asyncRepWg.Wait()
	}
	if afterRelease != nil {
		afterRelease()
	}
	return nil
}

func (s *Shard) addTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling enableAsyncReplication/disableAsyncReplication because they will lock again
		defer s.asyncReplicationRWMux.Unlock()

		for i, existing := range s.targetNodeOverrides {
			if existing.Equal(&targetNodeOverride) {
				// if the collection/shard/source/target already exists, use the max
				// upper time bound between the existing/new override
				maxUpperTimeBound := existing.UpperTimeBound
				if targetNodeOverride.UpperTimeBound > maxUpperTimeBound {
					maxUpperTimeBound = targetNodeOverride.UpperTimeBound
					s.targetNodeOverrides[i].UpperTimeBound = maxUpperTimeBound
				}
				return
			}
		}

		if s.targetNodeOverrides == nil {
			s.targetNodeOverrides = make(additional.AsyncReplicationTargetNodeOverrides, 0, 1)
		}
		s.targetNodeOverrides = append(s.targetNodeOverrides, targetNodeOverride)
	}()
	// we call update async replication config here to ensure that async replication starts
	// if it's not already running
	return s.enableAsyncReplication(ctx, s.index.AsyncReplicationConfig())
}

func (s *Shard) removeTargetNodeOverride(ctx context.Context, targetNodeOverrideToRemove additional.AsyncReplicationTargetNodeOverride) error {
	targetNodeOverrideLen := 0
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling enableAsyncReplication/disableAsyncReplication because they will lock again
		defer s.asyncReplicationRWMux.Unlock()

		newTargetNodeOverrides := make(additional.AsyncReplicationTargetNodeOverrides, 0, len(s.targetNodeOverrides))
		for _, existing := range s.targetNodeOverrides {
			// only remove the existing override if the collection/shard/source/target match and the
			// existing upper time bound is <= to the override being removed (eg if the override to remove
			// is "before" the existing override, don't remove it)
			if existing.Equal(&targetNodeOverrideToRemove) && existing.UpperTimeBound <= targetNodeOverrideToRemove.UpperTimeBound {
				s.asyncReplicationStatsMux.Lock()
				delete(s.asyncReplicationStatsByTargetNode, existing.TargetNode)
				s.asyncReplicationStatsMux.Unlock()
				continue
			}
			newTargetNodeOverrides = append(newTargetNodeOverrides, existing)
		}
		s.targetNodeOverrides = newTargetNodeOverrides

		targetNodeOverrideLen = len(s.targetNodeOverrides)
	}()
	// if there are no overrides left, return the async replication config to what it
	// was before overrides were added
	if targetNodeOverrideLen == 0 {
		if s.index.AsyncReplicationEnabled() {
			return s.enableAsyncReplication(ctx, s.index.AsyncReplicationConfig())
		}
		return s.disableAsyncReplication(ctx)
	}
	return nil
}

func (s *Shard) removeAllTargetNodeOverrides(ctx context.Context) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling enableAsyncReplication/disableAsyncReplication because they will lock again
		defer s.asyncReplicationRWMux.Unlock()
		s.targetNodeOverrides = make(additional.AsyncReplicationTargetNodeOverrides, 0)
	}()
	if s.index.AsyncReplicationEnabled() {
		return s.enableAsyncReplication(ctx, s.index.AsyncReplicationConfig())
	}
	return s.disableAsyncReplication(ctx)
}

func (s *Shard) getAsyncReplicationStats(ctx context.Context) []*models.AsyncReplicationStatus {
	s.asyncReplicationStatsMux.RLock()
	defer s.asyncReplicationStatsMux.RUnlock()

	asyncReplicationStatsToReturn := make([]*models.AsyncReplicationStatus, 0, len(s.asyncReplicationStatsByTargetNode))
	for targetNodeName, asyncReplicationStats := range s.asyncReplicationStatsByTargetNode {
		asyncReplicationStatsToReturn = append(asyncReplicationStatsToReturn, &models.AsyncReplicationStatus{
			ObjectsPropagated:       uint64(max(0, asyncReplicationStats.localObjectsPropagationCount-asyncReplicationStats.objectsNotResolved)),
			StartDiffTimeUnixMillis: asyncReplicationStats.hashtreeDiffStartTime.UnixMilli(),
			TargetNode:              targetNodeName,
		})
	}

	return asyncReplicationStatsToReturn
}

// dumpHashTreeOf serializes ht to a new file in s.pathHashTree() using an
// atomic write-rename pattern: it writes to a <name>.tmp file first, syncs,
// then renames to the final name so that a crash mid-write never leaves a
// truncated or partially-written hashtree file on disk.
func (s *Shard) dumpHashTreeOf(ht hashtree.AggregatedHashTree) (err error) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))

	dir := s.pathHashTree()
	finalFilename := filepath.Join(dir, fmt.Sprintf("hashtree-%x.ht", b[:]))
	tmpFilename := finalFilename + ".tmp"

	f, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", tmpFilename, err)
	}
	// Ensure the fd is always closed even on early error returns. The explicit
	// Close before os.Rename sets closed=true so the deferred close is a no-op
	// on the happy path (double-closing an *os.File returns an error).
	// On error the .tmp file is left on disk but will be ignored on reload
	// (only .ht files without the .tmp suffix are loaded).
	var closed bool
	defer func() {
		if !closed {
			if closeErr := f.Close(); closeErr != nil && err == nil {
				err = fmt.Errorf("closing hashtree %q: %w", tmpFilename, closeErr)
			}
		}
	}()

	w := bufio.NewWriter(f)

	_, err = ht.Serialize(w)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", tmpFilename, err)
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("flushing hashtree %q: %w", tmpFilename, err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("syncing hashtree %q: %w", tmpFilename, err)
	}

	// Close explicitly before rename so the fd is released on all platforms.
	closed = true
	if err = f.Close(); err != nil {
		return fmt.Errorf("closing hashtree %q: %w", tmpFilename, err)
	}

	if err = os.Rename(tmpFilename, finalFilename); err != nil {
		return fmt.Errorf("renaming hashtree %q -> %q: %w", tmpFilename, finalFilename, err)
	}

	if err := diskio.Fsync(dir); err != nil {
		return fmt.Errorf("fsync hashtree directory %q: %w", dir, err)
	}

	return nil
}

func (s *Shard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if !s.hashtreeFullyInitialized {
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}

	// discriminant is level-local: size must equal nodesAtLevel(level) = LeavesCount(level).
	// A size mismatch indicates a height mismatch between source and replica.
	expectedSize := hashtree.LeavesCount(level)
	if discriminant.Size() != expectedSize {
		return nil, fmt.Errorf("hashtree level %d: discriminant size %d, expected %d (possible height mismatch)",
			level, discriminant.Size(), expectedSize)
	}

	digests = make([]hashtree.Digest, expectedSize)

	n, err := s.hashtree.LevelLocal(level, discriminant, digests)
	if err != nil {
		return nil, err
	}

	return digests[:n], nil
}

// notifyHashbeat asks the scheduler to reprioritise this shard to run
// immediately. It is safe to call from any goroutine, including flush
// callbacks. If the shard is not registered with the scheduler, the call
// is a no-op.
func (s *Shard) notifyHashbeat() {
	s.asyncReplicationRWMux.RLock()
	registered := s.hashbeatRegistered
	s.asyncReplicationRWMux.RUnlock()

	if !registered {
		return
	}

	s.index.asyncReplicationScheduler.NotifyShard(s)
}

// runHashbeatCycle runs one full hashbeat cycle and is called by a scheduler
// worker goroutine. It uses shard-level state maps (asyncRepLast*) which are
// only ever accessed from a single worker at a time (enforced by asyncRepWg).
// Returns (propagated, err):
//   - (false, replica.ErrNoDiffFound)         – no diff; use long interval
//   - (true,  replica.ErrHashtreeRootUnchanged) – root unchanged; keep fast poll
//   - (false, ctx.Err())                       – context cancelled
//   - (false, <other error>)                   – failure; scheduler applies backoff
//   - (propagated, nil)                        – success
func (s *Shard) runHashbeatCycle(ctx context.Context, config AsyncReplicationConfig) (propagated bool, err error) {
	targetNodeOverridesLen := func() int {
		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()
		return len(s.targetNodeOverrides)
	}()

	if (!s.index.AsyncReplicationEnabled() && targetNodeOverridesLen == 0) || s.index.maintenanceModeEnabled() {
		// skip hashbeat iteration when async replication is disabled and no target node overrides are set
		// or maintenance mode is enabled for localhost
		if s.index.maintenanceModeEnabled() {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Info("skipping async replication in maintenance mode")
		}
		return false, nil
	}

	stats, err := s.hashBeat(ctx, config, s.asyncRepLastLocalRootByTarget, s.asyncRepLastRemoteRootByTarget, s.asyncRepLastPropagatedToTarget)

	// update the shard stats for the target node
	func() {
		s.asyncReplicationStatsMux.Lock()
		defer s.asyncReplicationStatsMux.Unlock()

		if s.asyncReplicationStatsByTargetNode == nil {
			s.asyncReplicationStatsByTargetNode = make(map[string]*hashBeatHostStats)
		}
		if (err == nil || errors.Is(err, replica.ErrNoDiffFound) || errors.Is(err, replica.ErrHashtreeRootUnchanged)) && stats != nil {
			for _, stat := range stats {
				if stat != nil {
					s.index.logger.WithFields(logrus.Fields{
						"shard_name":                      s.name,
						"target_node_name":                stat.targetNodeName,
						"hashtree_diff_took":              stat.hashtreeDiffTook,
						"object_digests_diff_took":        stat.objectDigestsDiffTook,
						"local_object_digests_count":      stat.localObjectDigestsCount,
						"remote_object_digests_count":     stat.remoteObjectDigestsCount,
						"objects_diff_count":              stat.objectsDiffCount,
						"local_objects_propagation_count": stat.localObjectsPropagationCount,
						"local_objects_propagation_took":  stat.localObjectsPropagationTook,
					}).Debug("updating async replication stats")
					s.asyncReplicationStatsByTargetNode[stat.targetNodeName] = stat
				}
			}
		}
	}()

	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if errors.Is(err, replica.ErrNoDiffFound) {
			if time.Since(s.asyncRepLastLog) >= config.loggingFrequency {
				s.asyncRepLastLog = time.Now()
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("hosts", s.getLastComparedHosts()).
					Debug("hashbeat iteration successfully completed: no differences were found")
			}
			return false, replica.ErrNoDiffFound
		}

		if errors.Is(err, replica.ErrHashtreeRootUnchanged) {
			if time.Since(s.asyncRepLastLog) >= config.loggingFrequency {
				s.asyncRepLastLog = time.Now()
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Debug("hashbeat skipped: target hashtree unchanged since last propagation, awaiting flush")
			}
			return true, replica.ErrHashtreeRootUnchanged // keep fast poll cadence
		}

		if time.Since(s.asyncRepLastLog) >= config.loggingFrequency {
			s.asyncRepLastLog = time.Now()
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("hashbeat iteration failed: %v", err)
		}
		return false, err
	}

	for _, stat := range stats {
		// Use localObjectsPropagationCount (objects actually sent to target via
		// Overwrite) rather than objectsDiffCount (which also
		// counts remoteDeleted local-only deletions). This keeps the fast-poll
		// cadence consistent with the Case A skip condition in HadWork.
		if stat.localObjectsPropagationCount > 0 {
			propagated = true
		}
	}

	if time.Since(s.asyncRepLastLog) >= config.loggingFrequency {
		s.asyncRepLastLog = time.Now()

		for _, stat := range stats {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				WithField("target_node_name", stat.targetNodeName).
				WithField("hashtree_diff_took", stat.hashtreeDiffTook).
				WithField("object_digests_diff_took", stat.objectDigestsDiffTook).
				WithField("local_object_digests_count", stat.localObjectDigestsCount).
				WithField("remote_object_digests_count", stat.remoteObjectDigestsCount).
				WithField("objects_diff_count", stat.objectsDiffCount).
				WithField("local_objects_propagation_count", stat.localObjectsPropagationCount).
				WithField("local_objects_propagation_took", stat.localObjectsPropagationTook).
				Debug("hashbeat iteration successfully completed")
		}
	}

	// Record the per-target root snapshots for the next cycle so that
	// CollectShardDifferences can short-circuit after the level-0 RPC.
	for _, stat := range stats {
		if stat != nil && stat.targetNodeName != "" {
			s.asyncRepLastLocalRootByTarget[stat.targetNodeName] = stat.localHashtreeRoot
			s.asyncRepLastRemoteRootByTarget[stat.targetNodeName] = stat.remoteHashtreeRoot
			// Record whether we sent objects to this target during this cycle.
			// Local-only deletions (remoteDeleted verdicts) are excluded because they
			// do not change the remote hashtree, so frequencyWhilePropagating should
			// not be triggered when no objects were actually propagated to the target.
			s.asyncRepLastPropagatedToTarget[stat.targetNodeName] = stat.localObjectsPropagationCount > 0
		}
	}

	return propagated, nil
}

func (s *Shard) setLastComparedNodes(hosts []string) {
	s.lastComparedHostsMux.Lock()
	defer s.lastComparedHostsMux.Unlock()

	s.lastComparedHosts = hosts
}

func (s *Shard) getLastComparedHosts() []string {
	s.lastComparedHostsMux.RLock()
	defer s.lastComparedHostsMux.RUnlock()

	// Return a copy so the caller cannot mutate the live backing array.
	return slices.Clone(s.lastComparedHosts)
}

// allAliveHostnames returns the host addresses of nodes that hold a replica of
// this shard, as reported by the routing plan, and ok=true. Only nodes with a
// resolvable address are included, so the set shrinks when a node is removed
// from the cluster and grows when one is added.
//
// Using the shard-specific routing plan (rather than the cluster-wide
// AllHostnames) ensures that the topology watcher only triggers an immediate
// hashbeat for shards that actually have a replica on the changed node,
// instead of waking every registered shard on any cluster membership event.
//
// If the routing plan cannot be built, ok=false is returned and the caller
// must skip both the reprioritization trigger and the lastComparedHosts
// update, so a transient routing error does not corrupt the baseline used for
// future "newly alive" comparisons.
func (s *Shard) allAliveHostnames() (hosts []string, ok bool) {
	if s.index == nil || s.index.router == nil {
		return nil, true
	}
	opts := s.index.router.BuildRoutingPlanOptions(s.name, s.name, types.ConsistencyLevelOne, "")
	plan, err := s.index.router.BuildReadRoutingPlan(opts)
	if err != nil {
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("error", err).
			Debug("failed to build read routing plan; skipping topology-change hashbeat for this shard")
		return nil, false
	}
	return plan.HostAddresses(), true
}

// hasNewElement reports whether any element of candidates is absent from existing.
func hasNewElement(candidates, existing []string) bool {
	set := make(map[string]struct{}, len(existing))
	for _, h := range existing {
		set[h] = struct{}{}
	}
	for _, h := range candidates {
		if _, ok := set[h]; !ok {
			return true
		}
	}
	return false
}

type hashBeatHostStats struct {
	targetNodeName               string
	hashtreeDiffStartTime        time.Time
	hashtreeDiffTook             time.Duration
	objectDigestsDiffTook        time.Duration
	localObjectDigestsCount      int
	remoteObjectDigestsCount     int // digests sent to remote for comparison
	objectsDiffCount             int // total objects found different in this cycle: outbound propagations + local deletes
	localObjectsPropagationCount int
	localObjectsPropagationTook  time.Duration
	objectsNotResolved           int
	// localHashtreeRoot and remoteHashtreeRoot record the level-0 digests
	// compared in this cycle. handleHashbeatWakeup stores them so that hashBeat
	// can skip the expensive CompareDigests + propagation pass when neither side
	// has flushed its memtable since the previous propagation cycle.
	localHashtreeRoot  hashtree.Digest
	remoteHashtreeRoot hashtree.Digest
}

func (s *Shard) hashBeat(
	ctx context.Context,
	config AsyncReplicationConfig,
	lastLocalRootByTarget map[string]hashtree.Digest,
	lastRemoteRootByTarget map[string]hashtree.Digest,
	lastPropagatedToTarget map[string]bool,
) (stats []*hashBeatHostStats, err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationIterationCount()
	s.metrics.IncAsyncReplicationIterationRunning()

	defer func() {
		s.metrics.DecAsyncReplicationIterationRunning()

		if err != nil && !errors.Is(err, replica.ErrNoDiffFound) && !errors.Is(err, replica.ErrHashtreeRootUnchanged) {
			s.metrics.IncAsyncReplicationIterationFailureCount()
			return
		}

		s.metrics.ObserveAsyncReplicationIterationDuration(time.Since(start))
	}()

	var ht hashtree.AggregatedHashTree
	var targetNodeOverridesSnapshot additional.AsyncReplicationTargetNodeOverrides

	s.asyncReplicationRWMux.RLock()
	if s.hashtree == nil {
		s.asyncReplicationRWMux.RUnlock()
		// handling the case of a hashtree being explicitly set to nil
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}
	ht = s.hashtree
	// create a snapshot of targetNodeOverrides to use throughout hashbeat
	if s.targetNodeOverrides != nil {
		targetNodeOverridesSnapshot = slices.Clone(s.targetNodeOverrides)
	}
	s.asyncReplicationRWMux.RUnlock()

	hashtreeDiffStart := time.Now()

	// Build per-target skip state so CollectShardDifferences can short-circuit
	// after the level-0 RPC (1 round-trip) instead of walking the full tree
	// (up to Height RPCs) when Case A or Case B conditions are met.
	skipStates := make(map[string]replica.ShardDiffSkipState, len(lastRemoteRootByTarget))
	for target, remoteRoot := range lastRemoteRootByTarget {
		skipStates[target] = replica.ShardDiffSkipState{
			LocalRoot:  lastLocalRootByTarget[target],
			RemoteRoot: remoteRoot,
			HadWork:    lastPropagatedToTarget[target],
		}
	}

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, ht, config.diffPerNodeTimeout, targetNodeOverridesSnapshot, skipStates)
	if err != nil {
		if (errors.Is(err, replica.ErrNoDiffFound) || errors.Is(err, replica.ErrHashtreeRootUnchanged)) && len(targetNodeOverridesSnapshot) > 0 {
			stats := make([]*hashBeatHostStats, 0, len(targetNodeOverridesSnapshot))
			for _, o := range targetNodeOverridesSnapshot {
				stats = append(stats, &hashBeatHostStats{
					targetNodeName:        o.TargetNode,
					hashtreeDiffStartTime: hashtreeDiffStart,
				})
			}
			return stats, err
		}
		if errors.Is(err, replica.ErrHashtreeRootUnchanged) || errors.Is(err, replica.ErrNoDiffFound) {
			// Skip conditions were met (Case A or Case B). The reader is empty
			// (no target name or roots); the per-target skip-state maps already
			// hold the correct roots from the previous cycle and are preserved as-is.
			return []*hashBeatHostStats{{
				hashtreeDiffStartTime: hashtreeDiffStart,
				hashtreeDiffTook:      time.Since(hashtreeDiffStart),
			}}, err
		}
		return nil, fmt.Errorf("collecting hashtree differences: %w", err)
	}

	hashtreeDiffTook := time.Since(hashtreeDiffStart)
	s.metrics.ObserveAsyncReplicationHashtreeDiffDuration(hashtreeDiffTook)

	rangeReader := shardDiffReader.RangeReader

	objectDigestsDiffStart := time.Now()

	localObjectDigestsCount := 0
	remoteObjectDigestsCount := 0
	objectsDiffCount := 0
	// localDeletedCount tracks objects deleted locally due to remoteDeleted
	// verdicts from CompareDigests. It is added to len(localObjectsToPropagate)
	// when computing the remaining propagation capacity so that tombstone
	// conflict resolutions are bounded by the same limit as live propagations.
	localDeletedCount := 0

	localObjectsToPropagate := make([]strfmt.UUID, 0, config.propagationLimit)
	localUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)
	remoteStaleUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)

	objectDigestsDiffCtx, cancel := context.WithTimeout(ctx, config.prePropagationTimeout)
	defer cancel()

	for len(localObjectsToPropagate)+localDeletedCount < config.propagationLimit {
		initialLeaf, finalLeaf, err := rangeReader.Next()
		if err != nil {
			if errors.Is(err, hashtree.ErrNoMoreRanges) {
				break
			}
			return nil, fmt.Errorf("reading collected differences: %w", err)
		}

		localObjsCountWithinRange, remoteObjsCountWithinRange, objsToPropagateWithinRange, err := s.objectsToPropagateWithinRange(
			objectDigestsDiffCtx,
			config,
			shardDiffReader.TargetNodeAddress,
			shardDiffReader.TargetNodeName,
			initialLeaf,
			finalLeaf,
			config.propagationLimit-len(localObjectsToPropagate)-localDeletedCount,
			targetNodeOverridesSnapshot,
		)
		if err != nil {
			if objectDigestsDiffCtx.Err() != nil {
				// it may be the case that just pre propagation timeout was reached
				// and some objects could be propagated
				break
			}

			return nil, fmt.Errorf("collecting local objects to be propagated: %w", err)
		}

		localObjectDigestsCount += localObjsCountWithinRange
		remoteObjectDigestsCount += remoteObjsCountWithinRange
		objectsDiffCount += len(objsToPropagateWithinRange)

		for _, obj := range objsToPropagateWithinRange {
			if obj.remoteDeleted {
				// Target has already applied the deletion strategy and instructed
				// deletion; apply it locally without re-checking the strategy.
				//
				// Use time.Time{} (Go zero) when the deletion timestamp is unknown
				// (remoteStaleUpdateTime==0) so shard_write_delete.go calls
				// bucket.Delete (no timestamp) rather than bucket.DeleteWith(epoch).
				var deletionTime time.Time
				if obj.remoteStaleUpdateTime != 0 {
					deletionTime = time.UnixMilli(obj.remoteStaleUpdateTime)
				}
				if err := s.DeleteObject(ctx, obj.uuid, deletionTime); err != nil {
					return nil, fmt.Errorf("deleting locally conflicted object: %w", err)
				}
				localDeletedCount++
				continue
			}
			localObjectsToPropagate = append(localObjectsToPropagate, obj.uuid)
			localUpdateTimeByUUID[obj.uuid] = obj.lastUpdateTime
			remoteStaleUpdateTimeByUUID[obj.uuid] = obj.remoteStaleUpdateTime
		}
	}

	objectDigestsDiffTook := time.Since(objectDigestsDiffStart)
	s.metrics.ObserveAsyncReplicationObjectDigestsDiffDuration(objectDigestsDiffTook)
	s.metrics.AddAsyncReplicationObjectsDiffCount(objectsDiffCount)
	s.metrics.AddAsyncReplicationLocalDeletionsCount(localDeletedCount)

	objectsPropagationStart := time.Now()

	objectsNotResolved := 0
	if len(localObjectsToPropagate) > 0 {
		propagationCtx, cancel := context.WithTimeout(ctx, config.propagationTimeout)
		defer cancel()

		resp, propagateErr := s.propagateObjects(propagationCtx, config, shardDiffReader.TargetNodeAddress, localObjectsToPropagate, remoteStaleUpdateTimeByUUID)

		// Process conflict responses from successful batches even when some
		// batches failed, so deletion conflicts are resolved before we surface
		// the error. Failed objects will be retried on the next hashtree diff.
		conflictDeletionCount := 0
		for _, r := range resp {
			// NOTE: deleted objects are not propagated but locally deleted when conflict is detected

			deletionStrategy := s.index.DeletionStrategy()

			if !r.Deleted ||
				deletionStrategy == models.ReplicationConfigDeletionStrategyNoAutomatedResolution ||
				targetNodeOverridesSnapshot.NoDeletionResolution(shardDiffReader.TargetNodeName) {
				objectsNotResolved++
				continue
			}

			if deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict ||
				(deletionStrategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution &&
					r.UpdateTime > localUpdateTimeByUUID[strfmt.UUID(r.ID)]) {

				err := s.DeleteObject(propagationCtx, strfmt.UUID(r.ID), time.UnixMilli(r.UpdateTime))
				if err != nil {
					return nil, fmt.Errorf("deleting local objects: %w", err)
				}
				conflictDeletionCount++
			}
		}
		s.metrics.AddAsyncReplicationLocalDeletionsCount(conflictDeletionCount)

		if propagateErr != nil {
			return nil, fmt.Errorf("propagating local objects: %w", propagateErr)
		}
	}

	return []*hashBeatHostStats{
		{
			targetNodeName:               shardDiffReader.TargetNodeName,
			hashtreeDiffStartTime:        hashtreeDiffStart,
			hashtreeDiffTook:             hashtreeDiffTook,
			objectDigestsDiffTook:        objectDigestsDiffTook,
			localObjectDigestsCount:      localObjectDigestsCount,
			remoteObjectDigestsCount:     remoteObjectDigestsCount,
			objectsDiffCount:             objectsDiffCount,
			localObjectsPropagationCount: len(localObjectsToPropagate),
			localObjectsPropagationTook:  time.Since(objectsPropagationStart),
			objectsNotResolved:           objectsNotResolved,
			localHashtreeRoot:            shardDiffReader.LocalHashtreeRoot,
			remoteHashtreeRoot:           shardDiffReader.RemoteHashtreeRoot,
		},
	}, nil
}

func uuidFromBytes(uuidBytes []byte) (id strfmt.UUID, err error) {
	uuidParsed, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return id, err
	}
	return strfmt.UUID(uuidParsed.String()), nil
}

func bytesFromUUID(id strfmt.UUID) (uuidBytes []byte, err error) {
	uuidParsed, err := uuid.Parse(id.String())
	if err != nil {
		return nil, err
	}
	return uuidParsed.MarshalBinary()
}

func incToNextLexValue(b []byte) bool {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return false
		}
		b[i] = 0x00
	}
	return true
}

type objectToPropagate struct {
	uuid                  strfmt.UUID
	lastUpdateTime        int64
	remoteStaleUpdateTime int64
	// remoteDeleted is true when CompareDigests on the target reported that the
	// target has tombstoned this object and the configured deletion strategy
	// instructs the source to delete. When set, the object must NOT be
	// propagated; instead the source deletes its own local copy.
	remoteDeleted bool
}

// objectsToPropagateWithinRange determines which local objects in the given
// hashtree leaf range the source must propagate to targetNodeAddress.
//
// For each local batch it:
//  1. Fetches local digests via DigestObjectsInRange.
//  2. Filters out objects that are too recent (per maxUpdateTime).
//  3. Sends the remaining digests to the target via a single CompareDigests
//     round-trip. The target returns only those UUIDs that need action:
//     missing/stale entries (source must propagate) and tombstoned entries
//     where the deletion strategy instructs the source to delete locally.
//     This collapses the former O(N_local_batches × N_remote_batches) nested
//     HTTP scan to O(N_local_batches) round-trips.
func (s *Shard) objectsToPropagateWithinRange(ctx context.Context, config AsyncReplicationConfig,
	targetNodeAddress, targetNodeName string, initialLeaf, finalLeaf uint64, limit int,
	targetNodeOverrides additional.AsyncReplicationTargetNodeOverrides,
) (localObjectsCount int, remoteObjectsCount int, objectsToPropagate []objectToPropagate, err error) {
	objectsToPropagate = make([]objectToPropagate, 0, limit)

	hashtreeHeight := config.hashtreeHeight

	finalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(finalUUIDBytes, finalLeaf<<(64-hashtreeHeight)|((1<<(64-hashtreeHeight))-1))
	copy(finalUUIDBytes[8:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	finalUUID, err := uuidFromBytes(finalUUIDBytes)
	if err != nil {
		return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
	}

	// Compute the threshold once per call so all batches use a consistent
	// cut-off. Recomputing inside the loop would allow the threshold to drift
	// between batches, producing inconsistent eligibility decisions for objects
	// whose UpdateTime sits close to the boundary.
	maxUpdateTime := s.getHashBeatMaxUpdateTime(targetNodeName, targetNodeOverrides)

	// localNodeName is used as a tiebreaker for equal-timestamp conflicts: the
	// node with the lexicographically lower name wins and propagates its version.
	localNodeName := s.index.replicator.LocalNodeName()

	currLocalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(currLocalUUIDBytes, initialLeaf<<(64-hashtreeHeight))

	// filteredDigests and localDigestsByUUID are declared outside the loop and
	// reset on each iteration to avoid per-batch heap allocations on this hot path.
	filteredDigests := make([]types.RepairResponse, 0, config.diffBatchSize)
	localDigestsByUUID := make(map[uuid.UUID]types.RepairResponse, config.diffBatchSize)

	for limit > 0 && bytes.Compare(currLocalUUIDBytes, finalUUIDBytes) < 1 {
		if ctx.Err() != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, ctx.Err()
		}

		currLocalUUID, err := uuidFromBytes(currLocalUUIDBytes)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
		}

		currBatchSize := min(limit, config.diffBatchSize)

		allLocalDigests, err := s.index.DigestObjectsInRange(ctx, s.name, currLocalUUID, finalUUID, currBatchSize)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, fmt.Errorf("fetching local object digests: %w", err)
		}

		if len(allLocalDigests) == 0 {
			break
		}

		lastLocalUUIDBytes, err := bytesFromUUID(strfmt.UUID(allLocalDigests[len(allLocalDigests)-1].ID))
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, err
		}

		// Filter out objects that are too recent to propagate.
		// localDigestsByUUID uses uuid.UUID ([16]byte) keys to avoid per-entry
		// string allocations on this hot path.
		filteredDigests = filteredDigests[:0]
		clear(localDigestsByUUID)
		for _, d := range allLocalDigests {
			if d.UpdateTime > maxUpdateTime {
				continue
			}
			key, err := uuid.Parse(d.ID)
			if err != nil {
				// A local digest with an unparseable UUID indicates data corruption;
				// log and skip rather than silently diverging the two collections.
				s.index.logger.WithField("uuid", d.ID).
					Error("async replication: skipping local digest with invalid UUID")
				continue
			}
			filteredDigests = append(filteredDigests, d)
			localDigestsByUUID[key] = d
		}
		// Count only the objects eligible for comparison (after recency filtering),
		// so the stat reflects actual CompareDigests traffic, not objects scanned.
		localObjectsCount += len(filteredDigests)

		if len(filteredDigests) == 0 {
			// All objects in this batch are too recent to propagate, but later
			// UUID ranges may still contain eligible objects (DigestObjectsInRange
			// returns results ordered by UUID, not by UpdateTime). Advance past
			// this batch and keep scanning instead of stopping here.
			if len(allLocalDigests) < currBatchSize {
				break
			}
			overflow := incToNextLexValue(lastLocalUUIDBytes)
			if overflow {
				break
			}
			currLocalUUIDBytes = lastLocalUUIDBytes
			continue
		}

		// Single round-trip: target responds with only the UUIDs that need
		// propagation (missing or stale on target), eliminating the inner loop.
		staleDigests, err := s.index.replicator.CompareDigests(ctx, s.name, targetNodeAddress, filteredDigests)
		if err != nil {
			return localObjectsCount, remoteObjectsCount, objectsToPropagate, fmt.Errorf("comparing digests with remote: %w", err)
		}

		// remoteObjectsCount tracks digests sent to the remote for comparison,
		// i.e. the volume of the CompareDigests request. Objects queued for
		// propagation (stale/missing on remote after tiebreak) are a subset of
		// this and are captured separately via objectsDiffCount
		// at the call site.
		remoteObjectsCount += len(filteredDigests)

		propagated := 0
		for _, stale := range staleDigests {
			key, err := uuid.Parse(stale.ID)
			if err != nil {
				// An invalid UUID in a remote stale digest indicates a protocol or
				// remote bug; log it so the issue is visible rather than silently
				// skipping objects that may need propagation.
				s.index.logger.WithField("uuid", stale.ID).
					Error("async replication: skipping stale digest from remote with invalid UUID")
				continue
			}
			localDigest, ok := localDigestsByUUID[key]
			if !ok {
				continue
			}

			// Target has already applied the deletion strategy and instructed this
			// source node to delete the object locally (Deleted=true) or to propagate
			// the live object (Deleted=false, normal stale path below).
			if stale.Deleted {
				objectsToPropagate = append(objectsToPropagate, objectToPropagate{
					uuid:                  strfmt.UUID(stale.ID),
					lastUpdateTime:        localDigest.UpdateTime,
					remoteStaleUpdateTime: stale.UpdateTime,
					remoteDeleted:         true,
				})
				propagated++
				continue
			}

			// Equal-timestamp guard: CompareDigests does not return objects where
			// source and target hold the same UpdateTime (they are not stale).
			// This check is a defensive guard in case a caller path produces a
			// stale entry with a matching timestamp; it prevents the source from
			// re-propagating an object it just successfully delivered.
			if stale.UpdateTime != 0 && stale.UpdateTime == localDigest.UpdateTime &&
				localNodeName >= targetNodeName {
				continue
			}
			objectsToPropagate = append(objectsToPropagate, objectToPropagate{
				uuid:                  strfmt.UUID(stale.ID),
				lastUpdateTime:        localDigest.UpdateTime,
				remoteStaleUpdateTime: stale.UpdateTime, // 0 means missing from target
			})
			propagated++
		}

		if len(allLocalDigests) < currBatchSize {
			break
		}

		overflow := incToNextLexValue(lastLocalUUIDBytes)
		if overflow {
			break
		}

		currLocalUUIDBytes = lastLocalUUIDBytes
		// Decrement by objects actually queued for propagation so that limit
		// reflects remaining propagation capacity, not objects scanned.
		// Scanning many already-up-to-date objects should not exhaust the limit.
		limit -= propagated
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjectsCount, remoteObjectsCount, objectsToPropagate, nil
}

// getHashBeatMaxUpdateTime returns the maximum update time for the hash beat.
// If our local node and the target node have an upper time bound configured, use the
// configured upper time bound instead of the default one
func (s *Shard) getHashBeatMaxUpdateTime(targetNodeName string, targetNodeOverrides additional.AsyncReplicationTargetNodeOverrides) int64 {
	localNodeName := s.index.replicator.LocalNodeName()
	for _, override := range targetNodeOverrides {
		if override.Equal(&additional.AsyncReplicationTargetNodeOverride{
			SourceNode:   localNodeName,
			TargetNode:   targetNodeName,
			CollectionID: s.class.Class,
			ShardID:      s.name,
		}) {
			return override.UpperTimeBound
		}
	}
	return math.MaxInt64
}

func (s *Shard) propagateObjects(ctx context.Context, config AsyncReplicationConfig, host string,
	objectsToPropagate []strfmt.UUID, remoteStaleUpdateTime map[strfmt.UUID]int64,
) (res []types.RepairResponse, err error) {
	s.metrics.IncAsyncReplicationPropagationCount()

	defer func(start time.Time) {
		if err != nil {
			s.metrics.IncAsyncReplicationPropagationFailureCount()
			return
		}

		s.metrics.AddAsyncReplicationPropagationObjectCount(len(objectsToPropagate))
		s.metrics.ObserveAsyncReplicationPropagationDuration(time.Since(start))
	}(time.Now())

	type workerResponse struct {
		resp []types.RepairResponse
		err  error
	}

	// workerWg tracks active worker goroutines (not individual batches).
	// This is the key invariant: workerWg reaches zero only when every worker
	// goroutine has fully exited, which guarantees that all resultCh sends have
	// already completed before the closer goroutine closes the channel.
	var workerWg sync.WaitGroup

	// cancelCause cancels workerCtx and records the first cause (e.g. a panic).
	// All workers and in-flight requests are bound to workerCtx so they stop
	// promptly when one worker fails.
	workerCtx, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)

	numBatches := len(objectsToPropagate)/config.propagationBatchSize + 1
	batchCh := make(chan []strfmt.UUID, numBatches)
	// resultCh capacity covers one response per batch plus one panic error per
	// worker, ensuring the recovery defer never blocks before workerWg.Done fires.
	resultCh := make(chan workerResponse, numBatches+config.propagationConcurrency)

	for range config.propagationConcurrency {
		workerWg.Add(1)
		enterrors.GoWrapper(func() {
			// workerWg.Done is deferred first so it runs last (LIFO), after the
			// recovery defer below has already sent to resultCh.  This guarantees
			// that every resultCh send from this goroutine completes before the
			// closer goroutine's workerWg.Wait() unblocks and closes resultCh.
			defer workerWg.Done()
			// Recover panics so they are surfaced as errors rather than silently
			// swallowed by GoWrapper's logger.  cancelCause stops all other workers
			// via workerCtx; they drain batchCh without processing (see below).
			defer func() {
				if r := recover(); r != nil {
					panicErr := fmt.Errorf("worker panic: %v", r)
					s.index.logger.WithField("action", "async_replication_propagate").
						WithField("class_name", s.class.Class).
						WithField("shard_name", s.name).
						Errorf("recovered panic in propagateObjects worker: %v", r)
					cancelCause(panicErr)
					resultCh <- workerResponse{err: panicErr}
				}
			}()
			for uuidBatch := range batchCh {
				// When workerCtx is cancelled (panic in another worker or parent
				// cancellation), drain remaining batches without processing.
				// batchCh is closed by the producer after all batches are sent, so
				// this loop always terminates without needing a separate WaitGroup.
				if workerCtx.Err() != nil {
					continue
				}

				localObjs, err := s.MultiObjectByID(workerCtx, wrapIDsInMulti(uuidBatch))
				if err != nil {
					resultCh <- workerResponse{
						err: fmt.Errorf("fetching local objects: %w", err),
					}
					continue
				}

				batch := make([]*objects.VObject, 0, len(localObjs))

				for _, obj := range localObjs {
					if obj == nil {
						// local object was deleted meanwhile
						continue
					}

					var vectors map[string][]float32
					var multiVectors map[string][][]float32

					if obj.Vectors != nil {
						vectors = make(map[string][]float32, len(obj.Vectors))
						for targetVector, v := range obj.Vectors {
							vectors[targetVector] = v
						}
					}
					if obj.MultiVectors != nil {
						multiVectors = make(map[string][][]float32, len(obj.MultiVectors))
						for targetVector, v := range obj.MultiVectors {
							multiVectors[targetVector] = v
						}
					}

					obj := &objects.VObject{
						ID:                      obj.ID(),
						LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix(),
						LatestObject:            &obj.Object,
						Vector:                  obj.Vector,
						Vectors:                 vectors,
						MultiVectors:            multiVectors,
						StaleUpdateTime:         remoteStaleUpdateTime[obj.ID()],
					}

					batch = append(batch, obj)
				}

				if len(batch) > 0 {
					resp, err := s.index.replicator.Overwrite(workerCtx, host, s.class.Class, s.name, batch)

					resultCh <- workerResponse{
						resp: resp,
						err:  err,
					}
				}
			}
		}, s.index.logger)
	}

	// Send all batches upfront (batchCh is pre-sized to fit them all without
	// blocking), then close batchCh so workers exit their range loop naturally
	// once all items are consumed — no separate WaitGroup per batch needed.
	for i := 0; i < len(objectsToPropagate); {
		actualBatchSize := config.propagationBatchSize
		if i+actualBatchSize > len(objectsToPropagate) {
			actualBatchSize = len(objectsToPropagate) - i
		}

		batchCh <- objectsToPropagate[i : i+actualBatchSize]

		i += actualBatchSize
	}
	close(batchCh)

	enterrors.GoWrapper(func() {
		workerWg.Wait()
		close(resultCh)
	}, s.index.logger)

	ec := errorcompounder.New()

	for r := range resultCh {
		if r.err != nil {
			ec.Add(r.err)
			continue
		}

		res = append(res, r.resp...)
	}

	// If workerCtx was cancelled (parent timeout, panic, etc.) but no worker
	// managed to emit an error (e.g. all workers hit the drain-continue path
	// before executing any work), surface the cancellation cause so callers
	// are not misled by a nil error.
	if err := ec.ToError(); err != nil {
		return res, err
	}
	if cause := context.Cause(workerCtx); cause != nil {
		return res, cause
	}
	return res, nil
}
