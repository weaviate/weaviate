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
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
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
	defaultPropagationTimeout          = 60 * time.Second
	defaultPropagationLimit            = 1_000
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
}

// initAsyncReplication initialises async-replication state for the shard.
// It must be called while holding asyncReplicationRWMux for writing.
//
// If the function loaded a cached hashtree from disk it returns a non-nil
// afterRelease callback.  The caller MUST invoke afterRelease() after
// releasing asyncReplicationRWMux; calling it while the lock is still held
// would deadlock (the hashbeater trigger goroutine sends on the channel,
// and object-write goroutines wait on the same lock).
func (s *Shard) initAsyncReplication(config AsyncReplicationConfig) (afterRelease func(), err error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.asyncReplicationCancelFunc = cancelFunc

	s.asyncReplicationConfig = config

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

	if err := os.MkdirAll(s.pathHashTree(), os.ModePerm); err != nil {
		return nil, err
	}

	// load the most recent hashtree file
	dirEntries, err := os.ReadDir(s.pathHashTree())
	if err != nil {
		return nil, err
	}

	for i := len(dirEntries) - 1; i >= 0; i-- {
		dirEntry := dirEntries[i]

		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".ht" {
			continue
		}

		hashtreeFilename := filepath.Join(s.pathHashTree(), dirEntry.Name())

		if s.hashtree != nil {
			err := os.Remove(hashtreeFilename)
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("deleting older hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		f, err := os.OpenFile(hashtreeFilename, os.O_RDONLY, os.ModePerm)
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		// attempt to load hashtree from file
		s.hashtree, err = hashtree.DeserializeHashTree(bufio.NewReader(f))
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
		}

		err = f.Close()
		if err != nil {
			return nil, err
		}

		err = os.Remove(hashtreeFilename)
		if err != nil {
			return nil, err
		}

		if err := diskio.Fsync(s.pathHashTree()); err != nil {
			return nil, fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
		}

		if s.hashtree != nil && s.hashtree.Height() != config.hashtreeHeight {
			// existing hashtree is erased if a different height was specified
			s.hashtree = nil
		}
	}

	if s.hashtree != nil {
		s.hashtreeFullyInitialized = true
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("took", fmt.Sprintf("%v", time.Since(start))).
			Info("hashtree successfully initialized")

		// Set hashbeatNotifyCh now while we already hold the write lock.
		// We must NOT call initHashBeater here: initHashBeater tries to
		// re-acquire the write lock and would deadlock (initAsyncReplication
		// is always invoked while the caller holds asyncReplicationRWMux).
		// Instead, return a callback that the caller must invoke after
		// releasing the lock.
		propagationRequired := make(chan struct{}, 1)
		s.hashbeatNotifyCh = propagationRequired
		capturedCtx := ctx
		capturedConfig := config
		return func() {
			s.startHashbeaterGoroutines(capturedCtx, capturedConfig, propagationRequired)
		}, nil
	}

	s.hashtree, err = hashtree.NewHashTree(config.hashtreeHeight)
	if err != nil {
		return nil, err
	}

	s.hashtreeFullyInitialized = false

	enterrors.GoWrapper(func() {
		for i := 0; ; i++ {
			err := s.initHashtree(ctx, config, bucket)
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

	return nil, nil
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

		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()

		obj := &storobj.Object{}
		obj.Object.LastUpdateTimeUnix = updateTime
		err := s.mayUpsertObjectHashTree(obj, uuidBytes, objectInsertStatus{})
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
	s.asyncReplicationRWMux.Unlock()

	// initHashBeater is called outside the write lock: the trigger goroutine it
	// spawns immediately sends on an unbuffered channel, which blocks until the
	// hashbeater goroutine picks it up. Holding the write lock across goroutine
	// scheduling would stall every concurrent object write and HashTreeLevel RPC.
	s.index.logger.
		WithField("action", "async_replication").
		WithField("class_name", s.class.Class).
		WithField("shard_name", s.name).
		WithField("object_count", objCount).
		WithField("took", fmt.Sprintf("%v", time.Since(start))).
		Info("hashtree successfully initialized")

	s.initHashBeater(ctx, config)

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
	defer s.asyncReplicationRWMux.Unlock()

	if s.hashtree == nil {
		return
	}

	s.asyncReplicationCancelFunc()

	if s.hashtreeFullyInitialized && !s.hashtreeFlushFailed {
		// the hashtree needs to be fully in sync with stored data before it can be persisted
		err := s.dumpHashTree()
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("store hashtree failed: %v", err)
		}
	}

	s.hashtree = nil
	s.hashtreeFullyInitialized = false
	s.hashtreeFlushFailed = false
	s.hashbeatNotifyCh = nil

	if bucket := s.store.Bucket(helpers.ObjectsBucketLSM); bucket != nil {
		bucket.SetObjectFlushCallback(nil)
		bucket.SetFlushCallback(nil)
	}
}

func (s *Shard) SetAsyncReplicationState(_ context.Context, config AsyncReplicationConfig, enabled bool) error {
	var afterRelease func()

	err := func() error {
		s.asyncReplicationRWMux.Lock()
		defer s.asyncReplicationRWMux.Unlock()

		if enabled {
			if s.hashtree != nil {
				return nil
			}
			var err error
			afterRelease, err = s.initAsyncReplication(config)
			return err
		}

		if s.hashtree == nil {
			return nil
		}

		s.asyncReplicationCancelFunc()

		s.hashtree = nil
		s.hashtreeFullyInitialized = false
		s.hashbeatNotifyCh = nil
		if bucket := s.store.Bucket(helpers.ObjectsBucketLSM); bucket != nil {
			bucket.SetObjectFlushCallback(nil)
			bucket.SetFlushCallback(nil)
		}
		s.asyncReplicationStatsMux.Lock()
		s.asyncReplicationStatsByTargetNode = nil
		s.asyncReplicationStatsMux.Unlock()

		return nil
	}()
	if err != nil {
		return err
	}
	if afterRelease != nil {
		afterRelease()
	}
	return nil
}

func (s *Shard) addTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
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
	return s.SetAsyncReplicationState(ctx, s.index.AsyncReplicationConfig(), true)
}

func (s *Shard) removeTargetNodeOverride(ctx context.Context, targetNodeOverrideToRemove additional.AsyncReplicationTargetNodeOverride) error {
	targetNodeOverrideLen := 0
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
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
		return s.SetAsyncReplicationState(ctx, s.index.AsyncReplicationConfig(), s.index.AsyncReplicationEnabled())
	}
	return nil
}

func (s *Shard) removeAllTargetNodeOverrides(ctx context.Context) error {
	func() {
		s.asyncReplicationRWMux.Lock()
		// unlock before calling SetAsyncReplicationEnabled because it will lock again
		defer s.asyncReplicationRWMux.Unlock()
		s.targetNodeOverrides = make(additional.AsyncReplicationTargetNodeOverrides, 0)
	}()
	return s.SetAsyncReplicationState(ctx, s.index.AsyncReplicationConfig(), s.index.AsyncReplicationEnabled())
}

func (s *Shard) getAsyncReplicationStats(ctx context.Context) []*models.AsyncReplicationStatus {
	s.asyncReplicationStatsMux.RLock()
	defer s.asyncReplicationStatsMux.RUnlock()

	asyncReplicationStatsToReturn := make([]*models.AsyncReplicationStatus, 0, len(s.asyncReplicationStatsByTargetNode))
	for targetNodeName, asyncReplicationStats := range s.asyncReplicationStatsByTargetNode {
		asyncReplicationStatsToReturn = append(asyncReplicationStatsToReturn, &models.AsyncReplicationStatus{
			ObjectsPropagated:       uint64(asyncReplicationStats.localObjectsPropagationCount) - uint64(asyncReplicationStats.objectsNotResolved),
			StartDiffTimeUnixMillis: asyncReplicationStats.hashtreeDiffStartTime.UnixMilli(),
			TargetNode:              targetNodeName,
		})
	}

	return asyncReplicationStatsToReturn
}

func (s *Shard) dumpHashTree() (err error) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))

	hashtreeFilename := filepath.Join(s.pathHashTree(), fmt.Sprintf("hashtree-%x.ht", string(b[:])))

	f, err := os.OpenFile(hashtreeFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}
	// Ensure the fd is always closed even when Serialize/Flush/Sync fail.
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing hashtree %q: %w", hashtreeFilename, closeErr)
		}
	}()

	w := bufio.NewWriter(f)

	_, err = s.hashtree.Serialize(w)
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("storing hashtree %q: %w", hashtreeFilename, err)
	}

	if err := diskio.Fsync(s.pathHashTree()); err != nil {
		return fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
	}

	return nil
}

func (s *Shard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if !s.hashtreeFullyInitialized {
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}

	// TODO (jeroiraz): reusable pool of digests slices
	digests = make([]hashtree.Digest, hashtree.LeavesCount(level+1))

	n, err := s.hashtree.Level(level, discriminant, digests)
	if err != nil {
		return nil, err
	}

	return digests[:n], nil
}

// notifyHashbeat wakes the hashbeater goroutine without blocking.
// It is safe to call from any goroutine, including flush callbacks.
// If the hashbeater is not running (e.g. async replication is disabled),
// the call is a no-op.
//
// The underlying channel is buffered (capacity 1), so a notification sent
// while the hashbeater is mid-run is queued and picked up immediately when
// the current run finishes.  A second concurrent notification is dropped
// (the pending one already covers it).
func (s *Shard) notifyHashbeat() {
	s.asyncReplicationRWMux.RLock()
	ch := s.hashbeatNotifyCh
	s.asyncReplicationRWMux.RUnlock()

	if ch == nil {
		return
	}

	select {
	case ch <- struct{}{}:
	default:
		// hashbeat already notified or currently running; the next periodic
		// tick will cover any objects flushed between now and then.
	}
}

func (s *Shard) initHashBeater(ctx context.Context, config AsyncReplicationConfig) {
	// propagationRequired is used to wake the hashbeater when a change requires
	// propagation (e.g. a new target node override is added or a memtable flush
	// completes making new objects available for digest reads).
	// The channel is created here and published to s.hashbeatNotifyCh so that
	// notifyHashbeat() can also send to it. The goroutines below capture the
	// local variable directly — they must not read s.hashbeatNotifyCh because it
	// can be set to nil concurrently by mayStopAsyncReplication /
	// SetAsyncReplicationState.
	propagationRequired := make(chan struct{}, 1)
	s.asyncReplicationRWMux.Lock()
	if s.hashtree == nil {
		// async replication was disabled while initHashtree was running;
		// do not start the hashbeater.
		s.asyncReplicationRWMux.Unlock()
		return
	}
	s.hashbeatNotifyCh = propagationRequired
	s.asyncReplicationRWMux.Unlock()

	s.startHashbeaterGoroutines(ctx, config, propagationRequired)
}

// startHashbeaterGoroutines starts the hashbeater and its trigger goroutine.
// propagationRequired is the channel used to wake the hashbeater; it must
// already be stored in s.hashbeatNotifyCh before calling this function.
//
// Must NOT be called while holding asyncReplicationRWMux: the trigger goroutine
// immediately sends on the propagationRequired channel, and object-write
// goroutines acquire the read lock — holding the write lock across that
// scheduling point would stall every concurrent object-write and HashTreeLevel
// RPC that acquires the read lock.
func (s *Shard) startHashbeaterGoroutines(ctx context.Context, config AsyncReplicationConfig, propagationRequired chan struct{}) {
	var lastHashbeat time.Time
	var lastHashbeatPropagatedObjects bool
	var lastHashbeatMux sync.Mutex

	enterrors.GoWrapper(func() {
		s.metrics.IncAsyncReplicationHashbeaterRunning()
		defer s.metrics.DecAsyncReplicationHashbeaterRunning()

		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashbeater started...")

		defer func() {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Info("hashbeater stopped")
		}()

		var lastLog time.Time

		// workerAcquireBackoff is separate from hashbeatBackoff so that
		// semaphore-contention failures and hashbeat failures don't inflate
		// each other's retry intervals.
		workerAcquireBackoff := interval.NewBackoffTimer(1*time.Second, 3*time.Second, 5*time.Second)
		hashbeatBackoff := interval.NewBackoffTimer(1*time.Second, 3*time.Second, 5*time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-propagationRequired:
				shouldStop := func() bool {
					err := s.index.asyncReplicationWorkerAcquire(ctx)
					if err != nil {
						if time.Since(lastLog) >= config.loggingFrequency {
							lastLog = time.Now()
							s.index.logger.
								WithField("action", "async_replication").
								WithField("class_name", s.class.Class).
								WithField("shard_name", s.name).
								Warn(err)
						}

						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return true
						}

						select {
						case <-time.After(workerAcquireBackoff.CurrentInterval()):
						case <-ctx.Done():
							return true
						}
						workerAcquireBackoff.IncreaseInterval()
						return false
					}
					workerAcquireBackoff.Reset()
					defer s.index.asyncReplicationWorkerRelease()

					return s.handleHashbeatWakeup(
						ctx,
						config,
						hashbeatBackoff,
						&lastLog,
						&lastHashbeat,
						&lastHashbeatPropagatedObjects,
						&lastHashbeatMux,
					)
				}()
				if shouldStop {
					return
				}
			}
		}
	}, s.index.logger)

	// goroutine to monitor changes in alive nodes and time since last hashbeat
	// and "wake up" the hashbeater when necessary
	// e.g. when a node goes down or comes back up, or when frequency time has elapsed
	// since last hashbeat
	// this ensures that changes in cluster topology are quickly detected and propagated
	// without having to wait for the next frequency tick
	// note that the hashbeater itself also has a frequency ticker to ensure that
	// propagation occurs at least every frequency interval even if no changes in
	// alive nodes occur
	enterrors.GoWrapper(func() {
		s.metrics.IncAsyncReplicationHashbeatTriggerRunning()
		defer s.metrics.DecAsyncReplicationHashbeatTriggerRunning()

		// fire an initial hashbeat immediately on startup; guard with ctx.Done
		// so the goroutine does not leak if the context is cancelled before the
		// hashbeater goroutine has a chance to receive from the unbuffered channel.
		select {
		case propagationRequired <- struct{}{}:
		case <-ctx.Done():
			return
		}

		nt := time.NewTicker(config.aliveNodesCheckingFrequency)
		defer nt.Stop()

		ft := time.NewTicker(min(config.frequencyWhilePropagating, config.frequency))
		defer ft.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-nt.C:
				comparedHosts := s.getLastComparedHosts()
				aliveHosts := s.allAliveHostnames()

				slices.Sort(comparedHosts)
				slices.Sort(aliveHosts)

				if !slices.Equal(comparedHosts, aliveHosts) {
					select {
					case <-ctx.Done():
						return
					case propagationRequired <- struct{}{}:
					}

					s.setLastComparedNodes(aliveHosts)
				}
			case <-ft.C:
				var shouldHashbeat bool
				lastHashbeatMux.Lock()
				shouldHashbeat = (lastHashbeatPropagatedObjects && time.Since(lastHashbeat) >= config.frequencyWhilePropagating) ||
					time.Since(lastHashbeat) >= config.frequency
				lastHashbeatMux.Unlock()

				if shouldHashbeat {
					select {
					case <-ctx.Done():
						return
					case propagationRequired <- struct{}{}:
					}
				}
			}
		}
	}, s.index.logger)
}

func (s *Shard) handleHashbeatWakeup(
	ctx context.Context,
	config AsyncReplicationConfig,
	backoffTimer *interval.BackoffTimer,
	lastLog *time.Time,
	lastHashbeat *time.Time,
	lastHashbeatPropagatedObjects *bool,
	lastHashbeatMux *sync.Mutex,
) (shouldStop bool) {
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

		backoffTimer.Reset()
		lastHashbeatMux.Lock()
		*lastHashbeat = time.Now()
		*lastHashbeatPropagatedObjects = false
		lastHashbeatMux.Unlock()
		return false
	}

	stats, err := s.hashBeat(ctx, config)

	// update the shard stats for the target node
	func() {
		s.asyncReplicationStatsMux.Lock()
		defer s.asyncReplicationStatsMux.Unlock()

		if s.asyncReplicationStatsByTargetNode == nil {
			s.asyncReplicationStatsByTargetNode = make(map[string]*hashBeatHostStats)
		}
		if (err == nil || errors.Is(err, replica.ErrNoDiffFound)) && stats != nil {
			for _, stat := range stats {
				if stat != nil {
					s.index.logger.WithFields(logrus.Fields{
						"shard_name":                           s.name,
						"target_node_name":                     stat.targetNodeName,
						"hashtree_diff_took":                   stat.hashtreeDiffTook,
						"object_digests_diff_took":             stat.objectDigestsDiffTook,
						"local_object_digests_count":           stat.localObjectDigestsCount,
						"remote_object_digests_count":          stat.remoteObjectDigestsCount,
						"objects_queued_for_propagation_count": stat.objectsQueuedForPropagationCount,
						"local_objects_propagation_count":      stat.localObjectsPropagationCount,
						"local_objects_propagation_took":       stat.localObjectsPropagationTook,
					}).Debug("updating async replication stats")
					s.asyncReplicationStatsByTargetNode[stat.targetNodeName] = stat
				}
			}
		}
	}()

	if err != nil {
		if ctx.Err() != nil {
			return true
		}

		if errors.Is(err, replica.ErrNoDiffFound) {
			if time.Since(*lastLog) >= config.loggingFrequency {
				*lastLog = time.Now()
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("hosts", s.getLastComparedHosts()).
					Debug("hashbeat iteration successfully completed: no differences were found")
			}

			backoffTimer.Reset()
			lastHashbeatMux.Lock()
			*lastHashbeat = time.Now()
			*lastHashbeatPropagatedObjects = false
			lastHashbeatMux.Unlock()
			return false
		}

		if time.Since(*lastLog) >= config.loggingFrequency {
			*lastLog = time.Now()
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("hashbeat iteration failed: %v", err)
		}

		select {
		case <-time.After(backoffTimer.CurrentInterval()):
		case <-ctx.Done():
			return true
		}
		backoffTimer.IncreaseInterval()
		lastHashbeatMux.Lock()
		*lastHashbeat = time.Now()
		*lastHashbeatPropagatedObjects = false
		lastHashbeatMux.Unlock()
		return false
	}

	statsHaveObjectsPropagated := false
	for _, stat := range stats {
		if stat.objectsQueuedForPropagationCount > 0 {
			statsHaveObjectsPropagated = true
		}
	}

	if time.Since(*lastLog) >= config.loggingFrequency {
		*lastLog = time.Now()

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
				WithField("objects_queued_for_propagation_count", stat.objectsQueuedForPropagationCount).
				WithField("local_objects_propagation_count", stat.localObjectsPropagationCount).
				WithField("local_objects_propagation_took", stat.localObjectsPropagationTook).
				Debug("hashbeat iteration successfully completed")
		}
	}

	backoffTimer.Reset()
	lastHashbeatMux.Lock()
	*lastHashbeat = time.Now()
	*lastHashbeatPropagatedObjects = statsHaveObjectsPropagated
	lastHashbeatMux.Unlock()

	return false
}

func (s *Shard) setLastComparedNodes(hosts []string) {
	s.lastComparedHostsMux.Lock()
	defer s.lastComparedHostsMux.Unlock()

	s.lastComparedHosts = hosts
}

func (s *Shard) getLastComparedHosts() []string {
	s.lastComparedHostsMux.RLock()
	defer s.lastComparedHostsMux.RUnlock()

	// Return a copy: the caller sorts the result in-place (slices.Sort), and
	// mutating the live backing array without the lock is a data race.
	return slices.Clone(s.lastComparedHosts)
}

func (s *Shard) allAliveHostnames() []string {
	return s.index.router.AllHostnames()
}

type hashBeatHostStats struct {
	targetNodeName                   string
	hashtreeDiffStartTime            time.Time
	hashtreeDiffTook                 time.Duration
	objectDigestsDiffTook            time.Duration
	localObjectDigestsCount          int
	remoteObjectDigestsCount         int // digests sent to remote for comparison
	objectsQueuedForPropagationCount int // objects queued for propagation (stale/missing on remote after tiebreak)
	localObjectsPropagationCount     int
	localObjectsPropagationTook      time.Duration
	objectsNotResolved               int
}

func (s *Shard) hashBeat(ctx context.Context, config AsyncReplicationConfig) (stats []*hashBeatHostStats, err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationIterationCount()
	s.metrics.IncAsyncReplicationIterationRunning()

	defer func() {
		s.metrics.DecAsyncReplicationIterationRunning()

		if err != nil && !errors.Is(err, replica.ErrNoDiffFound) {
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

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, ht, config.diffPerNodeTimeout, targetNodeOverridesSnapshot)
	if err != nil {
		if errors.Is(err, replica.ErrNoDiffFound) && len(targetNodeOverridesSnapshot) > 0 {
			stats := make([]*hashBeatHostStats, 0, len(targetNodeOverridesSnapshot))
			for _, o := range targetNodeOverridesSnapshot {
				stats = append(stats, &hashBeatHostStats{
					targetNodeName:        o.TargetNode,
					hashtreeDiffStartTime: hashtreeDiffStart,
				})
			}
			return stats, err
		}
		return nil, fmt.Errorf("collecting hashtree differences: %w", err)
	}

	hashtreeDiffTook := time.Since(hashtreeDiffStart)
	s.metrics.ObserveAsyncReplicationHashtreeDiffDuration(hashtreeDiffTook)

	rangeReader := shardDiffReader.RangeReader

	objectDigestsDiffStart := time.Now()

	localObjectDigestsCount := 0
	remoteObjectDigestsCount := 0
	objectsQueuedForPropagationCount := 0
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
		objectsQueuedForPropagationCount += len(objsToPropagateWithinRange)

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

	objectsPropagationStart := time.Now()

	objectsNotResolved := 0
	if len(localObjectsToPropagate) > 0 {
		propagationCtx, cancel := context.WithTimeout(ctx, config.propagationTimeout)
		defer cancel()

		resp, propagateErr := s.propagateObjects(propagationCtx, config, shardDiffReader.TargetNodeAddress, localObjectsToPropagate, remoteStaleUpdateTimeByUUID)

		// Process conflict responses from successful batches even when some
		// batches failed, so deletion conflicts are resolved before we surface
		// the error. Failed objects will be retried on the next hashtree diff.
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
			}
		}

		if propagateErr != nil {
			return nil, fmt.Errorf("propagating local objects: %w", propagateErr)
		}
	}

	return []*hashBeatHostStats{
		{
			targetNodeName:                   shardDiffReader.TargetNodeName,
			hashtreeDiffStartTime:            hashtreeDiffStart,
			hashtreeDiffTook:                 hashtreeDiffTook,
			objectDigestsDiffTook:            objectDigestsDiffTook,
			localObjectDigestsCount:          localObjectDigestsCount,
			remoteObjectDigestsCount:         remoteObjectDigestsCount,
			objectsQueuedForPropagationCount: objectsQueuedForPropagationCount,
			localObjectsPropagationCount:     len(localObjectsToPropagate),
			localObjectsPropagationTook:      time.Since(objectsPropagationStart),
			objectsNotResolved:               objectsNotResolved,
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
		filteredDigests := make([]types.RepairResponse, 0, len(allLocalDigests))
		localDigestsByUUID := make(map[uuid.UUID]types.RepairResponse, len(allLocalDigests))
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
		// this and are captured separately via objectsQueuedForPropagationCount
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

			// Equal-timestamp tiebreaker: both nodes have this object at the same
			// UpdateTime but potentially with different content. Only the node with
			// the lexicographically lower name propagates, ensuring exactly one side
			// wins deterministically without any extra round-trip.
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
