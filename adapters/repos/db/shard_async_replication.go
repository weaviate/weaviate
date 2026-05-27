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
	"os"
	"path/filepath"
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
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	defaultHashtreeHeightSingleTenant = 16
	defaultHashtreeHeightMultiTenant  = 10
	defaultFrequency                  = 30 * time.Second
	defaultFrequencyWhilePropagating  = 5 * time.Second
	defaultLoggingFrequency           = 60 * time.Second
	defaultDiffBatchSize              = 1_000
	defaultDiffPerNodeTimeout         = 10 * time.Second
	defaultPrePropagationTimeout      = 300 * time.Second
	defaultPropagationTimeout         = 60 * time.Second
	defaultPropagationLimit           = 1_000
	defaultPropagationConcurrency     = 1
	defaultPropagationBatchSize       = 100
	defaultPropagationDelay           = 30 * time.Second
	// asyncReplicationWorkerDrainTimeout is the maximum time mayStopAsyncReplication
	// will wait for in-flight hashbeat workers to exit after the per-shard context
	// has been cancelled. Workers should unblock almost immediately once their
	// context is done; this deadline exists solely to prevent a non-cancellable
	// downstream RPC (kernel-level TCP stall, buggy transport) from blocking
	// shard shutdown indefinitely.
	asyncReplicationWorkerDrainTimeout = 10 * time.Second

	// hashtreeDumpTimeout is the maximum time dumpHashTreeWithTimeout will wait
	// for the hashtree fsync+rename to complete. dumpHashTreeOf calls f.Sync()
	// which is a blocking OS syscall that can stall for minutes on degraded
	// storage. Treating the dump as best-effort — if it times out the tree is
	// simply rebuilt from scratch on the next startup — prevents shard shutdown
	// from hanging indefinitely and blocking rolling restarts.
	hashtreeDumpTimeout = 30 * time.Second

	minHashtreeHeight = 0
	maxHashtreeHeight = 20

	// minFrequency is the smallest accepted value for `frequency` in both
	// per-class API overrides and global runtime config. Below this, hashbeats
	// would run too often to be useful in a steady-state cluster.
	minFrequency = 5 * time.Second

	// minFrequencyWhilePropagating is the smallest accepted value for
	// `frequencyWhilePropagating`. This rate kicks in only while propagation is
	// actively in flight, so it can be tighter than minFrequency but must stay
	// at or above 1s to avoid dispatcher thrash.
	minFrequencyWhilePropagating = 1 * time.Second

	// minLoggingFrequency is the floor for loggingFrequency. Sub-second logging
	// at hashbeat rate would flood logs in busy clusters.
	minLoggingFrequency = 1 * time.Second

	minDiffBatchSize = 1
	maxDiffBatchSize = 10_000

	minPropagationLimit = 1
	maxPropagationLimit = 100_000

	minPropagationConcurrency = 1
	maxPropagationConcurrency = 20

	minPropagationBatchSize = 1
	maxPropagationBatchSize = 1_000
)

// asyncReplicationClassOverrides holds per-class API values that take
// precedence over both code defaults and global runtime config. A nil pointer
// means "not set via API; fall back to global or code default".
type asyncReplicationClassOverrides struct {
	hashtreeHeight            *int
	frequency                 *time.Duration
	frequencyWhilePropagating *time.Duration
	loggingFrequency          *time.Duration
	diffBatchSize             *int
	diffPerNodeTimeout        *time.Duration
	prePropagationTimeout     *time.Duration
	propagationTimeout        *time.Duration
	propagationLimit          *int
	propagationConcurrency    *int
	propagationBatchSize      *int
	propagationDelay          *time.Duration
}

// AsyncReplicationConfig holds the effective per-shard configuration for async
// replication. Values are populated from code defaults, per-class schema/API
// overrides, and global runtime DynamicValues by calling Effective(). The zero
// value is not safe for direct use; callers must always obtain a fully resolved
// snapshot via Effective() before passing it to the scheduler or hashbeat cycle.
type AsyncReplicationConfig struct {
	hashtreeHeight            int
	frequency                 time.Duration
	frequencyWhilePropagating time.Duration
	loggingFrequency          time.Duration
	diffBatchSize             int
	diffPerNodeTimeout        time.Duration
	prePropagationTimeout     time.Duration
	propagationTimeout        time.Duration
	propagationLimit          int
	propagationConcurrency    int
	propagationBatchSize      int
	propagationDelay          time.Duration
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
	if co.hashtreeHeight != nil {
		result.hashtreeHeight = *co.hashtreeHeight
	}
	if co.frequency != nil {
		result.frequency = *co.frequency
	}
	if co.frequencyWhilePropagating != nil {
		result.frequencyWhilePropagating = *co.frequencyWhilePropagating
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
	if co.propagationDelay != nil {
		result.propagationDelay = *co.propagationDelay
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
	// applyDurMinFrequency applies a DynamicValue[time.Duration], clamping it
	// up to the supplied minimum when the configured value is positive but
	// below the minimum. Zero (the DynamicValue "not configured" sentinel) is
	// silently ignored — mirrors applyInt's clamp-on-configured semantics so
	// that an operator who explicitly sets a sub-minimum runtime value still
	// gets a working value (the minimum) rather than a silently-dropped override.
	applyDurMinFrequency := func(dv *configRuntime.DynamicValue[time.Duration], field *time.Duration, minVal time.Duration) {
		if dv == nil {
			return
		}
		v := dv.Get()
		if v <= 0 {
			return
		}
		if v < minVal {
			v = minVal
		}
		*field = v
	}
	// applyDurPositive applies a DynamicValue[time.Duration] only when it is
	// strictly positive. Used for timeout and delay fields where zero is the
	// DynamicValue "not configured" sentinel but valid sub-100ms values (e.g.
	// propagationDelay=50ms) must not be silently rejected.
	applyDurPositive := func(dv *configRuntime.DynamicValue[time.Duration], field *time.Duration) {
		if dv == nil {
			return
		}
		if v := dv.Get(); v > 0 {
			*field = v
		}
	}
	applyInt(globals.AsyncReplicationHashtreeHeight, &result.hashtreeHeight, minHashtreeHeight, maxHashtreeHeight)
	applyDurMinFrequency(globals.AsyncReplicationFrequency, &result.frequency, minFrequency)
	applyDurMinFrequency(globals.AsyncReplicationFrequencyWhilePropagating, &result.frequencyWhilePropagating, minFrequencyWhilePropagating)
	applyDurPositive(globals.AsyncReplicationLoggingFrequency, &result.loggingFrequency)
	applyInt(globals.AsyncReplicationDiffBatchSize, &result.diffBatchSize, minDiffBatchSize, maxDiffBatchSize)
	applyDurPositive(globals.AsyncReplicationDiffPerNodeTimeout, &result.diffPerNodeTimeout)
	applyDurPositive(globals.AsyncReplicationPrePropagationTimeout, &result.prePropagationTimeout)
	applyDurPositive(globals.AsyncReplicationPropagationTimeout, &result.propagationTimeout)
	applyInt(globals.AsyncReplicationPropagationLimit, &result.propagationLimit, minPropagationLimit, maxPropagationLimit)
	applyInt(globals.AsyncReplicationPropagationConcurrency, &result.propagationConcurrency, minPropagationConcurrency, maxPropagationConcurrency)
	applyInt(globals.AsyncReplicationPropagationBatchSize, &result.propagationBatchSize, minPropagationBatchSize, maxPropagationBatchSize)
	applyDurPositive(globals.AsyncReplicationPropagationDelay, &result.propagationDelay)

	// Defensive clamp: guard against zero-value fields that would cause
	// downstream divide-by-zero, spawn-zero-workers, or immediate-deadline bugs.
	// These fields must always be >= their minimum, regardless of how the config
	// was constructed. Timeout fields guard against context.WithTimeout(ctx, 0)
	// which creates a context that is already expired.
	if result.diffBatchSize < minDiffBatchSize {
		result.diffBatchSize = minDiffBatchSize
	}
	if result.propagationLimit < minPropagationLimit {
		result.propagationLimit = minPropagationLimit
	}
	if result.propagationConcurrency < minPropagationConcurrency {
		result.propagationConcurrency = minPropagationConcurrency
	}
	if result.propagationBatchSize < minPropagationBatchSize {
		result.propagationBatchSize = minPropagationBatchSize
	}
	if result.frequency < minFrequency {
		result.frequency = minFrequency
	}
	if result.frequencyWhilePropagating < minFrequencyWhilePropagating {
		result.frequencyWhilePropagating = minFrequencyWhilePropagating
	}
	if result.loggingFrequency < minLoggingFrequency {
		result.loggingFrequency = minLoggingFrequency
	}
	if result.diffPerNodeTimeout <= 0 {
		result.diffPerNodeTimeout = defaultDiffPerNodeTimeout
	}
	if result.prePropagationTimeout <= 0 {
		result.prePropagationTimeout = defaultPrePropagationTimeout
	}
	if result.propagationTimeout <= 0 {
		result.propagationTimeout = defaultPropagationTimeout
	}

	// Clear classOverrides in the snapshot — they have been applied.
	result.classOverrides = asyncReplicationClassOverrides{}
	return result
}

// asyncReplicationClampWarner surfaces positive-but-below-minimum global
// runtime overrides on the two Frequency fields. Without it, an operator who
// sets a frequency value below its declared minimum silently gets the
// minimum, with no signal that their requested value was adjusted — the
// per-class path emits this Warn, but the runtime path did not.
//
// Dedup is consecutive-duplicate suppression per field: only the last warned
// value per field is remembered, so a stable misconfiguration logs exactly
// once across all hashbeat cycles, and flipping to a new sub-minimum value
// re-arms the warning. An A→B→A alternation would log A twice — a tradeoff
// we accept to keep state O(fields).
type asyncReplicationClampWarner struct {
	mu     sync.Mutex
	last   map[string]time.Duration
	logger logrus.FieldLogger
}

// checkSubFloor emits one Warn per consecutive-distinct positive-but-below-minimum
// requested value for the given field. Steady-state happy path returns under
// the range check without touching the mutex.
func (w *asyncReplicationClampWarner) checkSubFloor(field string, dv *configRuntime.DynamicValue[time.Duration], floor time.Duration) {
	if w == nil || w.logger == nil || dv == nil {
		return
	}
	v := dv.Get()
	if v <= 0 || v >= floor {
		return
	}
	w.mu.Lock()
	if prev, ok := w.last[field]; ok && prev == v {
		w.mu.Unlock()
		return
	}
	if w.last == nil {
		w.last = make(map[string]time.Duration)
	}
	w.last[field] = v
	w.mu.Unlock()
	w.logger.WithFields(logrus.Fields{
		"field":     field,
		"requested": v,
		"min":       floor,
		"applied":   floor,
		"source":    "runtime",
	}).Warn("async-replication global runtime value below minimum; clamping to min")
}

func (w *asyncReplicationClampWarner) checkGlobals(globals entreplication.GlobalConfig) {
	if w == nil {
		return
	}
	w.checkSubFloor("frequency", globals.AsyncReplicationFrequency, minFrequency)
	w.checkSubFloor("frequencyWhilePropagating", globals.AsyncReplicationFrequencyWhilePropagating, minFrequencyWhilePropagating)
}

// initRetryBackoff returns the exponential backoff for hashtree initialisation
// attempt i, capped at 5 minutes. The shift is capped at 12 (2^12 × 100 ms ≈
// 409 s) before the Duration multiplication to prevent int64 overflow: the
// naive 1<<i overflows at i=37, wrapping to a negative duration and causing
// time.After to fire immediately, turning the retry loop into a busy-loop.
func initRetryBackoff(i int) time.Duration {
	const maxShift = 12
	shift := i
	if shift > maxShift {
		shift = maxShift
	}
	return min(time.Duration(1<<shift)*100*time.Millisecond, 5*time.Minute)
}

// initAsyncReplication initialises async-replication state for the shard.
// It must be called while holding asyncReplicationRWMux for writing.
//
// The per-shard async-replication ctx is derived from the scheduler's lifecycle
// ctx so that scheduler/DB shutdown propagates cancellation to in-flight
// hashbeat work without relying solely on Deregister/disableAsyncReplication
// being called on every path.
func (s *Shard) initAsyncReplication(config AsyncReplicationConfig, cached hashtree.AggregatedHashTree) error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	parentCtx := context.Background()
	if s.index != nil && s.index.asyncReplicationScheduler != nil {
		parentCtx = s.index.asyncReplicationScheduler.ctx
	}
	ctx, cancelFunc := context.WithCancel(parentCtx)
	s.asyncReplicationCancelFunc = cancelFunc
	s.asyncRepCtx = ctx

	s.asyncRepLastLog.Store(0)
	s.asyncReplicationConfig = config

	effectiveConfig := config
	if s.index != nil && s.index.globalreplicationConfig != nil {
		effectiveConfig = config.Effective(*s.index.globalreplicationConfig)
	}

	if cached != nil {
		s.hashtree = cached
		s.hashtreeFullyInitialized = true
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree successfully initialized from cache")

		// scheduler.Register must NOT be called under the write lock: it sends
		// on addCh which the dispatcher reads while it may hold
		// asyncReplicationRWMux.RLock (in onResultLocked), causing a deadlock.
		// Spawning a goroutine mirrors the pattern used by the new-hashtree path.
		//
		// asyncRepWg tracks this goroutine so that rebuildHashtree's
		// asyncRepWg.Wait() serialises rebuilds against it. Without tracking, a
		// rapid disable→enable cycle (e.g. during a rebuild or a repair override
		// remove→add) would spawn a new goroutine before the old one exits.
		// Note: disableAsyncReplication does NOT call asyncRepWg.Wait(); the
		// goroutine exits promptly once the context is cancelled or hashtree is nil.
		s.asyncRepWg.Add(1)
		enterrors.GoWrapper(func() {
			defer s.asyncRepWg.Done()
			s.asyncReplicationRWMux.RLock()
			if s.hashtree == nil {
				// Async replication was disabled before this goroutine ran.
				s.asyncReplicationRWMux.RUnlock()
				return
			}
			s.asyncReplicationRWMux.RUnlock()

			if s.index.asyncReplicationScheduler == nil {
				return
			}
			if err := s.index.asyncReplicationScheduler.Register(s); err != nil {
				s.index.logger.WithField("action", "async_replication").Error(err)
				return
			}

			// Guard against disableAsyncReplication running between the lock
			// release above and Register() returning. If disable ran in that
			// window, it called Deregister as a no-op (shard wasn't registered
			// yet) and set hashtree to nil. Self-deregister now so the disabled
			// shard is not left in the scheduler. RLock is held across Deregister
			// to close the TOCTOU window: enableAsyncReplication needs WLock to
			// set hashtree, so it cannot race between the nil-check and Deregister.
			s.asyncReplicationRWMux.RLock()
			stillRunning := s.hashtree != nil
			if !stillRunning {
				if err := s.index.asyncReplicationScheduler.Deregister(s); err != nil {
					s.index.logger.WithField("action", "async_replication").Error(err)
				}
			}
			s.asyncReplicationRWMux.RUnlock()
		}, s.index.logger)
		return nil
	}

	var err error
	s.hashtree, err = hashtree.NewHashTree(effectiveConfig.hashtreeHeight)
	if err != nil {
		return err
	}

	s.hashtreeFullyInitialized = false
	s.minimalHashtreeInitializationCh = make(chan struct{})

	// asyncRepWg tracks this goroutine so that rebuildHashtree's
	// asyncRepWg.Wait() serialises rebuilds against it, preventing the old
	// goroutine from overlapping with a new one spawned by the next
	// enableAsyncReplication call (e.g. during a rebuild or repair cycle).
	// Note: disableAsyncReplication does NOT call asyncRepWg.Wait(); the
	// goroutine exits promptly once ctx is cancelled or hashtree is nil.
	s.asyncRepWg.Add(1)
	enterrors.GoWrapper(func() {
		defer s.asyncRepWg.Done()

		if s.index.asyncReplicationScheduler == nil {
			return
		}

		// Gate concurrent disk scans: on restart with many tenants, each shard
		// without a persisted hashtree spawns one goroutine here simultaneously.
		// Without this limit, thousands of concurrent scans thrash disk and spike RSS.
		//
		// The slot is acquired and released around each individual attempt rather
		// than held for the entire retry lifetime. This prevents a shard with
		// persistent initHashtree failures from permanently occupying a slot and
		// blocking all other shards from initialising.
		for i := 0; ; i++ {
			if err := s.index.asyncReplicationScheduler.acquireHashtreeInitSlot(ctx); err != nil {
				return // context cancelled before a slot became available
			}
			// Wrapped in a closure so that releaseHashtreeInitSlot is deferred
			// and runs even if initHashtree panics (GoWrapper recovers at
			// the goroutine boundary, after the deferred release fires).
			err := func() error {
				defer s.index.asyncReplicationScheduler.releaseHashtreeInitSlot()
				return s.initHashtree(ctx, effectiveConfig, bucket)
			}()

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

			// Exponential backoff capped at 5 min. Use select instead of
			// time.Sleep so that context cancellation (shard shutdown) is
			// respected immediately rather than after up to 5 minutes.
			backoff := initRetryBackoff(i)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}

			s.asyncReplicationRWMux.Lock()
			if s.hashtree == nil || ctx.Err() != nil {
				// Async replication was disabled (hashtree nil-ed) or the
				// context was cancelled while we slept. Exit the retry loop
				// instead of resetting so we neither panic on a nil hashtree
				// nor corrupt a newly-allocated hashtree from a concurrent
				// enableAsyncReplication call.
				s.asyncReplicationRWMux.Unlock()
				return
			}
			s.hashtree.Reset()
			// releaseInitialization (the afterInMemCallback passed to
			// ApplyToObjectDigests) always closes minimalHashtreeInitializationCh
			// before initHashtree returns — even on error — so the channel is
			// already closed here. Closing it again would panic. Replace it with
			// a fresh channel for the next attempt so write paths that arrive
			// during the retry wait for that attempt's releaseInitialization.
			s.minimalHashtreeInitializationCh = make(chan struct{})
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
			loaded = nil // discard any partial result; fall through to full scan
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("deserializing hashtree file %q: %v", filename, err)
		}

		if err := f.Close(); err != nil {
			return nil, err
		}
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err := diskio.Fsync(s.pathHashTree()); err != nil {
			return nil, fmt.Errorf("fsync hashtree directory %q: %w", s.pathHashTree(), err)
		}
	}

	if loaded != nil && loaded.Height() != expectedHeight {
		// Height mismatch — discard; a fresh scan will be performed instead.
		// Log a warning so operators can diagnose unexpectedly long restart times:
		// every shard on every node will re-scan its full object store when this
		// happens, which can take minutes on large datasets.
		s.index.logger.
			WithField("action", "async_replication_hashtree_load").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			WithField("cached_height", loaded.Height()).
			WithField("expected_height", expectedHeight).
			Warn("cached hashtree height mismatch — discarding and re-scanning full object store")
		return nil, nil
	}

	return loaded, nil
}

// initHashtree performs a full on-disk object scan to populate the shard's
// hashtree. It is called from a background goroutine after the write lock is
// released so that concurrent reads and writes are not blocked. Progress is
// logged at loggingFrequency intervals. The scan is gated by hashtreeInitSem
// to bound concurrent I/O across all shards at startup.
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

	// releaseInitialization closes minimalHashtreeInitializationCh under RLock
	// so it serializes with the write-lock path in the retry loop that replaces
	// the channel. RLock is the correct lock here (not Lock): we are only
	// reading the channel field and closing it; the retry path replaces the
	// field under Lock. Invariant: this closure is called exactly once per
	// initHashtree attempt (ApplyToObjectDigests guarantees it fires the
	// afterInMemCallback exactly once), so there is no risk of double-close.
	releaseInitialization := func() {
		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()

		close(s.minimalHashtreeInitializationCh)
	}

	objCount := 0
	prevProgressLogging := time.Now()

	err = bucket.ApplyToObjectDigests(ctx, releaseInitialization, func(uuidBytes []byte, updateTime int64) error {
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

		if err := s.upsertHashTreeLeaf(uuidBytes, updateTime); err != nil {
			return err
		}

		objCount++

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

	if s.index.asyncReplicationScheduler != nil {
		if err := s.index.asyncReplicationScheduler.Register(s); err != nil {
			s.index.logger.WithField("action", "async_replication").Error(err)
		} else {
			// Guard against disableAsyncReplication running between the lock release
			// above and Register() returning. If disable ran in that window, it called
			// Deregister as a no-op (shard wasn't registered yet) and set hashtree to
			// nil. Self-deregister now so the disabled shard is not left in the scheduler.
			// RLock is held across Deregister to close the TOCTOU window: enableAsyncReplication
			// needs WLock to set hashtree, so it cannot race between the nil-check and Deregister.
			s.asyncReplicationRWMux.RLock()
			stillRunning := s.hashtree != nil
			if !stillRunning {
				if err := s.index.asyncReplicationScheduler.Deregister(s); err != nil {
					s.index.logger.WithField("action", "async_replication").Error(err)
				}
			}
			s.asyncReplicationRWMux.RUnlock()
		}
	}

	return nil
}

// waitForMinimalHashTreeInitialization blocks until the hashtree has been
// minimally initialized (enough to serve object writes) or ctx is cancelled.
// "Minimal" means ApplyToObjectDigests has finished scanning the in-memory
// memtable: at that point writes can safely update the hashtree concurrently
// with the ongoing on-disk scan.
//
// It must NOT be called while holding asyncReplicationRWMux: the initHashtree
// retry path needs write-lock to replace minimalHashtreeInitializationCh, so
// holding RLock here while blocking on the channel would deadlock.
//
// The function does NOT loop after the channel fires. A closed channel means
// either the in-mem scan completed (proceed) or the retry path replaced the
// channel (also proceed — the retry resets the hashtree, concurrent writes on
// the fresh tree are safe). Looping would cause a busy-spin because a closed
// channel returns immediately on every iteration. This is safe even if the
// retry path already issued a new minimalHashtreeInitializationCh by the time
// the select fires: write paths that arrive between two retry attempts operate
// on the reset hashtree, which is valid (writes update leaf digests regardless
// of whether the full on-disk scan has completed).
func (s *Shard) waitForMinimalHashTreeInitialization(ctx context.Context) error {
	s.asyncReplicationRWMux.RLock()
	done := s.hashtree == nil || s.hashtreeFullyInitialized
	ch := s.minimalHashtreeInitializationCh
	s.asyncReplicationRWMux.RUnlock()

	if done {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// mayStopAsyncReplication cancels async replication for the shard if it is
// running, deregisters the shard from the scheduler, and waits up to
// asyncReplicationWorkerDrainTimeout for any in-flight hashbeat cycle to
// drain. It then persists the hashtree to disk if it was fully initialised.
//
// Unlike disableAsyncReplication, this function waits for in-flight cycles
// before returning. In the common case (workers respect context cancellation)
// this is a near-zero wait; the timeout exists so a non-cancellable downstream
// RPC cannot block shard shutdown indefinitely.
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
	if s.hashtreeFullyInitialized {
		capturedHT = s.hashtree
	}

	s.hashtree = nil
	s.hashtreeFullyInitialized = false
	s.clearAsyncCheckpointLocked()

	s.asyncReplicationRWMux.Unlock()

	// Remove from the scheduler so no new cycles are dispatched, then wait for
	// any in-flight cycle to drain. asyncRepCtx is already cancelled so the
	// cycle unwinds quickly (context errors on RPCs); the Wait adds ~0 latency
	// in the common case and gives a strict happens-before guarantee that no
	// hashbeat worker accesses shard resources after this call returns.
	//
	// asyncRepWg drain is bounded by asyncReplicationWorkerDrainTimeout.
	// Workers should exit almost immediately once their context is cancelled;
	// the timeout guards against a non-cancellable downstream RPC (e.g. a
	// kernel-level TCP stall) blocking shard shutdown indefinitely. If the
	// deadline fires, we proceed anyway: the worker goroutine is stuck in a
	// blocking syscall and cannot touch Go heap safely, but holding up the
	// entire shard shutdown is worse operationally.
	if s.index.asyncReplicationScheduler != nil {
		if err := s.index.asyncReplicationScheduler.Deregister(s); err != nil {
			s.index.logger.WithField("action", "async_replication").Error(err)
		}
	}
	workersDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(workersDone)
		s.asyncRepWg.Wait()
	}, s.index.logger)
	select {
	case <-workersDone:
	case <-time.After(asyncReplicationWorkerDrainTimeout):
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Warn("async replication worker did not stop within deadline; proceeding with forced shutdown")
	}

	if capturedHT != nil {
		// the hashtree needs to be fully in sync with stored data before it can be persisted
		s.dumpHashTreeWithTimeout(capturedHT)
	}
}

// enableAsyncReplication starts async replication for the shard, registering
// it with the scheduler and loading or initialising a hashtree. If the shard
// is already running, the stored config is updated in-place (a no-op init) so
// that the next hashbeat cycle picks up any changed settings.
func (s *Shard) enableAsyncReplication(ctx context.Context, config AsyncReplicationConfig) error {
	if s.index.asyncReplicationScheduler == nil {
		return fmt.Errorf("async replication scheduler is nil for index %q", s.index.ID())
	}

	// Fast path: if already running, skip disk I/O entirely.
	s.asyncReplicationRWMux.RLock()
	alreadyRunning := s.hashtree != nil
	s.asyncReplicationRWMux.RUnlock()

	if alreadyRunning {
		s.asyncReplicationRWMux.Lock()
		defer s.asyncReplicationRWMux.Unlock()

		if s.hashtree != nil {
			// Already running. Update the stored base config so the next hashbeat
			// cycle picks up any changed settings (e.g. classOverrides cleared via
			// API). The scheduler's height-detection in runEntry will signal a
			// rebuild via asyncRepNeedsRebuild if the effective hashtreeHeight
			// changed; non-height settings take effect on the next Effective() call.
			s.asyncReplicationConfig = config
			return nil
		}
		// Hashtree was nil-ed between the RLock check and write lock acquisition.
		// Fall through to a fresh init without a cached tree.
		return s.initAsyncReplication(config, nil)
	}

	// Compute the effective config (needed for hashtreeHeight) before taking
	// the write lock so we can load the cached hashtree from disk outside it.
	// tryLoadHashtreeFromDisk does synchronous I/O (ReadDir, OpenFile, Remove,
	// Fsync); holding the write lock for its duration would block all concurrent
	// RLock callers (hashbeat readers, object writes, commit handlers).
	effectiveConfig := config
	if s.index != nil && s.index.globalreplicationConfig != nil {
		effectiveConfig = config.Effective(*s.index.globalreplicationConfig)
	}
	cached, err := s.tryLoadHashtreeFromDisk(effectiveConfig.hashtreeHeight)
	if err != nil {
		return err
	}

	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()

	if s.hashtree != nil {
		// Another goroutine raced us and already initialised the hashtree.
		// Discard the loaded tree and just update the config.
		s.asyncReplicationConfig = config
		return nil
	}
	return s.initAsyncReplication(config, cached)
}

// disableAsyncReplication cancels the per-shard async-replication context,
// nils the hashtree, deregisters the shard from the scheduler, and persists
// the hashtree to disk if it was fully initialised.
//
// Unlike mayStopAsyncReplication, this function does NOT call asyncRepWg.Wait().
// Callers that need a strict happens-before guarantee against in-flight cycle
// completion should use mayStopAsyncReplication instead, or call Deregister
// followed by asyncRepWg.Wait() explicitly.
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
		if s.hashtreeFullyInitialized {
			capturedHT = s.hashtree
		}

		s.hashtree = nil
		s.hashtreeFullyInitialized = false
		s.clearAsyncCheckpointLocked()

		needDeregister = true

		if capturedHT != nil {
			afterRelease = func() {
				s.dumpHashTreeWithTimeout(capturedHT)
			}
		}

		return nil
	}()
	if err != nil {
		return err
	}
	if needDeregister {
		// Remove from the scheduler. The context has already been cancelled so
		// any in-flight cycle will exit promptly; we do not wait for it here.
		// Deregister may return context.Canceled if the scheduler is shutting
		// down concurrently; that is safe to ignore because the rebuild path
		// does not require a strict happens-before with the scheduler's shutdown.
		if s.index.asyncReplicationScheduler != nil {
			if err := s.index.asyncReplicationScheduler.Deregister(s); err != nil {
				s.index.logger.WithField("action", "async_replication").Error(err)
			}
		}
		// Clear stats outside asyncReplicationRWMux to avoid nesting
		// asyncReplicationStatsMux inside a write lock.
		s.asyncReplicationStatsMux.Lock()
		s.asyncReplicationStatsByTargetNode = nil
		s.asyncReplicationStatsMux.Unlock()
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
	var removedNodes []string
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
				removedNodes = append(removedNodes, existing.TargetNode)
				continue
			}
			newTargetNodeOverrides = append(newTargetNodeOverrides, existing)
		}
		s.targetNodeOverrides = newTargetNodeOverrides

		targetNodeOverrideLen = len(s.targetNodeOverrides)
	}()
	// Delete stats outside asyncReplicationRWMux to avoid nesting
	// asyncReplicationStatsMux inside a write lock.
	if len(removedNodes) > 0 {
		s.asyncReplicationStatsMux.Lock()
		for _, node := range removedNodes {
			delete(s.asyncReplicationStatsByTargetNode, node)
		}
		s.asyncReplicationStatsMux.Unlock()
	}
	// if there are no overrides left, return the async replication config to what it
	// was before overrides were added
	if targetNodeOverrideLen == 0 {
		// Snapshot enabled + config atomically under a single replicationConfigLock
		// RLock to close the TOCTOU window: a concurrent updateReplicationConfig
		// (WLock) could disable async replication between the AsyncReplicationEnabled
		// check and the AsyncReplicationConfig read, causing us to re-enable a shard
		// that should be off. asyncReplicationEnabled and Config are lock-free reads;
		// we release before calling enable/disable, which need asyncReplicationRWMux.
		//
		// Known limitation: a TOCTOU window remains between the RUnlock and the
		// enable/disable call — a concurrent updateReplicationConfig could flip the
		// state in that gap. The inconsistency is transient; the next
		// updateReplicationConfig call will correct it.
		s.index.replicationConfigLock.RLock()
		enabled := s.index.asyncReplicationEnabled()
		cfg := s.index.Config.AsyncReplicationConfig
		s.index.replicationConfigLock.RUnlock()
		if enabled {
			return s.enableAsyncReplication(ctx, cfg)
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
	// Same atomic snapshot as removeTargetNodeOverride: hold replicationConfigLock
	// RLock across both reads to prevent updateReplicationConfig from toggling the
	// state between the enabled check and the config read.
	s.index.replicationConfigLock.RLock()
	enabled := s.index.asyncReplicationEnabled()
	cfg := s.index.Config.AsyncReplicationConfig
	s.index.replicationConfigLock.RUnlock()
	if enabled {
		return s.enableAsyncReplication(ctx, cfg)
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

	f, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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

// dumpHashTreeWithTimeout persists ht to disk via dumpHashTreeOf but caps the
// operation at hashtreeDumpTimeout. dumpHashTreeOf calls f.Sync(), a blocking
// OS syscall that can stall for minutes on degraded or full storage; wrapping
// it in a goroutine with a timeout prevents shard shutdown from hanging
// indefinitely during rolling restarts or node decommissions.
//
// On timeout the background goroutine continues running until the OS Sync
// completes (it cannot be cancelled at the kernel level). The goroutine captures
// the shard pointer for error logging only; Go's GC ensures the Shard and its
// Index remain alive until the goroutine exits. No mutable shard state (store,
// hashtree) is accessed after this function returns. Any incomplete .tmp file
// left on disk is silently ignored on reload (tryLoadHashtreeFromDisk skips
// .tmp suffixes), and the tree will be rebuilt from scratch on next startup.
func (s *Shard) dumpHashTreeWithTimeout(ht hashtree.AggregatedHashTree) {
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		if err := s.dumpHashTreeOf(ht); err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("store hashtree failed: %v", err)
		}
	}, s.index.logger)
	select {
	case <-done:
	case <-time.After(hashtreeDumpTimeout):
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Warnf("hashtree dump timed out after %s; skipping — tree will be rebuilt on next startup", hashtreeDumpTimeout)
	}
}

// HashTreeLevel returns the digests at level of the shard's async-replication
// hashtree, filtered by discriminant (Size() == hashtree.LeavesCount(level)).
// Returns an error if the hashtree is not yet fully initialised.
func (s *Shard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	if level < 0 {
		return nil, fmt.Errorf("hashtree level must be non-negative: %d", level)
	}
	if discriminant == nil {
		return nil, fmt.Errorf("hashtree level %d: nil discriminant", level)
	}

	expected := hashtree.LeavesCount(level)
	if discriminant.Size() != expected {
		return nil, fmt.Errorf("hashtree level %d: discriminant size %d, expected %d (height mismatch?)",
			level, discriminant.Size(), expected)
	}
	digests = make([]hashtree.Digest, expected)

	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if !s.hashtreeFullyInitialized {
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}

	n, err := s.hashtree.Level(level, discriminant, digests)
	if err != nil {
		return nil, err
	}

	return digests[:n], nil
}

var (
	errAsyncReplicationNotActive = replica.ErrAsyncReplicationNotActive
	errAsyncCheckpointStale      = replica.ErrAsyncCheckpointStale
)

func (s *Shard) CreateAsyncCheckpoint(ctx context.Context, cutoffMs int64, createdAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.hashtree == nil || !s.hashtreeFullyInitialized {
		s.metrics.IncAsyncCheckpointCreateFailureCount()
		return errAsyncReplicationNotActive
	}

	if s.asyncCheckpointHashtree != nil && !createdAt.After(s.asyncCheckpointCreatedAt) {
		s.metrics.IncAsyncCheckpointCreateFailureCount()
		return errAsyncCheckpointStale
	}

	// Lifetime measured from local activation, never initiator createdAt, to avoid skew.
	wasActive := s.asyncCheckpointHashtree != nil
	if wasActive {
		s.metrics.ObserveAsyncCheckpointLifetime(time.Since(s.asyncCheckpointActivatedAt))
	}

	s.asyncCheckpointHashtree = s.hashtree.Clone()
	s.asyncCheckpointCutoff = cutoffMs
	s.asyncCheckpointCreatedAt = createdAt
	s.asyncCheckpointActivatedAt = time.Now()

	s.metrics.IncAsyncCheckpointCreateCount()
	if !wasActive {
		s.metrics.IncAsyncCheckpointActive()
	}
	return nil
}

// DeleteAsyncCheckpoint is idempotent. Counted under async_checkpoint_delete_total only
// when an active checkpoint was actually cleared (implicit clears via stop/disable go
// through clearAsyncCheckpointLocked).
func (s *Shard) DeleteAsyncCheckpoint(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.asyncReplicationRWMux.Lock()
	defer s.asyncReplicationRWMux.Unlock()
	if s.asyncCheckpointHashtree == nil {
		return nil
	}
	s.metrics.IncAsyncCheckpointDeleteCount()
	s.clearAsyncCheckpointLocked()
	return nil
}

// clearAsyncCheckpointLocked requires asyncReplicationRWMux held for writing.
// Shared by DeleteAsyncCheckpoint and the stop/disable cleanup paths.
func (s *Shard) clearAsyncCheckpointLocked() {
	if s.asyncCheckpointHashtree != nil {
		s.metrics.ObserveAsyncCheckpointLifetime(time.Since(s.asyncCheckpointActivatedAt))
		s.metrics.DecAsyncCheckpointActive()
	}
	s.asyncCheckpointHashtree = nil
	s.asyncCheckpointCutoff = 0
	s.asyncCheckpointCreatedAt = time.Time{}
	s.asyncCheckpointActivatedAt = time.Time{}
}

// AsyncCheckpointRoot does not consult ctx: folding cancellation into ok=false would
// let callers misread a cancelled context as an inactive checkpoint.
func (s *Shard) AsyncCheckpointRoot(ctx context.Context) (root hashtree.Digest, cutoffMs int64, createdAt time.Time, ok bool) {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	if s.asyncCheckpointHashtree == nil {
		return hashtree.Digest{}, 0, time.Time{}, false
	}
	return s.asyncCheckpointHashtree.Root(), s.asyncCheckpointCutoff, s.asyncCheckpointCreatedAt, true
}

// runHashbeatCycle runs one full hashbeat cycle and is called by a scheduler
// worker goroutine. It uses shard-level state maps (asyncRepLast*) which are
// only ever accessed from a single worker at a time (enforced by asyncRepWg).
// Returns (propagated, err):
//   - (false, replicaerrors.ErrNoDiffFound) – no diff; use long interval
//   - (false, ctx.Err())              – context cancelled
//   - (false, <other error>)          – failure; scheduler applies backoff
//   - (propagated, nil)               – success
func (s *Shard) runHashbeatCycle(ctx context.Context, config AsyncReplicationConfig) (propagated bool, err error) {
	// Bail out immediately if the per-shard context was cancelled between the
	// runEntry ctx.Err() check and this call. This narrows the window in which
	// an in-flight cycle can access shard state (hashtree, store) after
	// mayStopAsyncReplication has set hashtree=nil and allowed the store to
	// begin shutting down.
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	targetNodeOverridesLen := func() int {
		s.asyncReplicationRWMux.RLock()
		defer s.asyncReplicationRWMux.RUnlock()
		return len(s.targetNodeOverrides)
	}()

	if (!s.index.AsyncReplicationEnabled() && targetNodeOverridesLen == 0) || s.index.maintenanceModeEnabled() {
		// skip hashbeat iteration when async replication is disabled and no target node overrides are set
		// or maintenance mode is enabled for localhost
		if s.index.maintenanceModeEnabled() {
			if time.Since(time.Unix(s.asyncRepLastLog.Load(), 0)) >= config.loggingFrequency {
				s.asyncRepLastLog.Store(time.Now().Unix())
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					Info("skipping async replication in maintenance mode")
			}
		}
		return false, nil
	}

	stats, err := s.hashBeat(ctx, config)

	if stats != nil {
		nodes := make([]string, 0, len(stats))
		for _, stat := range stats {
			if stat != nil && stat.targetNodeName != "" {
				nodes = append(nodes, stat.targetNodeName)
			}
		}
		s.setLastComparedNodes(nodes)
	}

	// update the shard stats for the target node
	var toLog []*hashBeatHostStats
	func() {
		s.asyncReplicationStatsMux.Lock()
		defer s.asyncReplicationStatsMux.Unlock()

		if s.asyncReplicationStatsByTargetNode == nil {
			s.asyncReplicationStatsByTargetNode = make(map[string]*hashBeatHostStats)
		}
		if (err == nil || errors.Is(err, replicaerrors.ErrNoDiffFound)) && stats != nil {
			for _, stat := range stats {
				if stat != nil {
					s.asyncReplicationStatsByTargetNode[stat.targetNodeName] = stat
					toLog = append(toLog, stat)
				}
			}
		}
	}()
	for _, stat := range toLog {
		s.index.logger.WithFields(logrus.Fields{
			"shard_name":                      s.name,
			"target_node_name":                stat.targetNodeName,
			"hashtree_diff_took":              stat.hashtreeDiffTook,
			"object_digests_diff_took":        stat.objectDigestsDiffTook,
			"local_object_digests_count":      stat.localObjectDigestsCount,
			"objects_diff_count":              stat.objectsDiffCount,
			"local_objects_propagation_count": stat.localObjectsPropagationCount,
			"local_objects_propagation_took":  stat.localObjectsPropagationTook,
		}).Debug("updating async replication stats")
	}

	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if errors.Is(err, replicaerrors.ErrNoDiffFound) {
			if time.Since(time.Unix(s.asyncRepLastLog.Load(), 0)) >= config.loggingFrequency {
				s.asyncRepLastLog.Store(time.Now().Unix())
				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("hosts", s.getLastComparedHosts()).
					Debug("hashbeat iteration successfully completed: no differences were found")
			}
			return false, replicaerrors.ErrNoDiffFound
		}

		if time.Since(time.Unix(s.asyncRepLastLog.Load(), 0)) >= config.loggingFrequency {
			s.asyncRepLastLog.Store(time.Now().Unix())
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
		// Overwrite) rather than objectsDiffCount. This keeps the fast-poll
		// cadence consistent with the Case A skip condition in HadWork.
		if stat.localObjectsPropagationCount > 0 {
			propagated = true
		}
	}

	if time.Since(time.Unix(s.asyncRepLastLog.Load(), 0)) >= config.loggingFrequency {
		s.asyncRepLastLog.Store(time.Now().Unix())

		for _, stat := range stats {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				WithField("target_node_name", stat.targetNodeName).
				WithField("hashtree_diff_took", stat.hashtreeDiffTook).
				WithField("object_digests_diff_took", stat.objectDigestsDiffTook).
				WithField("local_object_digests_count", stat.localObjectDigestsCount).
				WithField("objects_diff_count", stat.objectsDiffCount).
				WithField("local_objects_propagation_count", stat.localObjectsPropagationCount).
				WithField("local_objects_propagation_took", stat.localObjectsPropagationTook).
				Debug("hashbeat iteration successfully completed")
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

type hashBeatHostStats struct {
	targetNodeName               string
	hashtreeDiffStartTime        time.Time
	hashtreeDiffTook             time.Duration
	objectDigestsDiffTook        time.Duration
	localObjectDigestsCount      int
	objectsDiffCount             int // total objects found different in this cycle: outbound propagations
	localObjectsPropagationCount int
	localObjectsPropagationTook  time.Duration
	objectsNotResolved           int
}

// hashBeat runs one diff-and-repair cycle against all replication targets for
// this shard. It calls CollectShardDifferences to find hashtree divergences,
// then objectsToPropagateWithinRange to identify the specific objects, and
// finally propagateObjects to push them to the target.
//
// Returns (stats, err):
//   - stats: per-target timing and count metrics (nil on early failure)
//   - err:   first error encountered; ErrNoDiffFound when all targets agree
func (s *Shard) hashBeat(
	ctx context.Context,
	config AsyncReplicationConfig,
) (stats []*hashBeatHostStats, err error) {
	start := time.Now()

	s.metrics.IncAsyncReplicationIterationCount()
	s.metrics.IncAsyncReplicationIterationRunning()

	defer func() {
		s.metrics.DecAsyncReplicationIterationRunning()

		if err != nil && !errors.Is(err, replicaerrors.ErrNoDiffFound) {
			s.metrics.IncAsyncReplicationIterationFailureCount()
			return
		}

		s.metrics.ObserveAsyncReplicationIterationDuration(time.Since(start))
	}()

	var ht hashtree.AggregatedHashTree
	var cpht hashtree.AggregatedHashTree
	var activeCutoff int64
	var targetNodeOverridesSnapshot additional.AsyncReplicationTargetNodeOverrides

	s.asyncReplicationRWMux.RLock()
	if s.hashtree == nil {
		s.asyncReplicationRWMux.RUnlock()
		// handling the case of a hashtree being explicitly set to nil
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}
	ht = s.hashtree
	cpht = s.asyncCheckpointHashtree
	if cpht != nil {
		activeCutoff = s.asyncCheckpointCutoff
	}
	// create a snapshot of targetNodeOverrides to use throughout hashbeat
	if s.targetNodeOverrides != nil {
		targetNodeOverridesSnapshot = slices.Clone(s.targetNodeOverrides)
	}
	s.asyncReplicationRWMux.RUnlock()

	// Prefer the bounded checkpoint tree when active; fall through to the
	// unbounded tree below if the checkpoint reports no diffs, otherwise
	// post-cutoff propagation stalls.
	activeHT := ht
	if cpht != nil {
		activeHT = cpht
	}

	hashtreeDiffStart := time.Now()

	shardDiffReader, err := s.index.replicator.CollectShardDifferences(ctx, s.name, activeHT, config.diffPerNodeTimeout, targetNodeOverridesSnapshot)
	if cpht != nil && errors.Is(err, replicaerrors.ErrNoDiffFound) {
		activeCutoff = 0
		shardDiffReader, err = s.index.replicator.CollectShardDifferences(ctx, s.name, ht, config.diffPerNodeTimeout, targetNodeOverridesSnapshot)
	}
	if err != nil {
		if errors.Is(err, replicaerrors.ErrNoDiffFound) && len(targetNodeOverridesSnapshot) > 0 {
			stats := make([]*hashBeatHostStats, 0, len(targetNodeOverridesSnapshot))
			for _, o := range targetNodeOverridesSnapshot {
				stats = append(stats, &hashBeatHostStats{
					targetNodeName:        o.TargetNode,
					hashtreeDiffStartTime: hashtreeDiffStart,
				})
			}
			return stats, err
		}
		if errors.Is(err, replicaerrors.ErrNoDiffFound) {
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
	objectsDiffCount := 0

	localObjectsToPropagate := make([]strfmt.UUID, 0, config.propagationLimit)
	localUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)
	remoteStaleUpdateTimeByUUID := make(map[strfmt.UUID]int64, config.propagationLimit)

	objectDigestsDiffCtx, cancel := context.WithTimeout(ctx, config.prePropagationTimeout)
	defer cancel()

	for len(localObjectsToPropagate) < config.propagationLimit {
		initialLeaf, finalLeaf, err := rangeReader.Next()
		if err != nil {
			if errors.Is(err, hashtree.ErrNoMoreRanges) {
				break
			}
			return nil, fmt.Errorf("reading collected differences: %w", err)
		}

		localObjsCountWithinRange, objsToPropagateWithinRange, err := s.objectsToPropagateWithinRange(
			objectDigestsDiffCtx,
			config,
			shardDiffReader.TargetNodeAddress,
			shardDiffReader.TargetNodeName,
			initialLeaf,
			finalLeaf,
			config.propagationLimit-len(localObjectsToPropagate),
			targetNodeOverridesSnapshot,
			activeCutoff,
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
		objectsDiffCount += len(objsToPropagateWithinRange)

		for _, obj := range objsToPropagateWithinRange {
			localObjectsToPropagate = append(localObjectsToPropagate, obj.uuid)
			localUpdateTimeByUUID[obj.uuid] = obj.lastUpdateTime
			remoteStaleUpdateTimeByUUID[obj.uuid] = obj.remoteStaleUpdateTime
		}
	}

	objectDigestsDiffTook := time.Since(objectDigestsDiffStart)
	s.metrics.ObserveAsyncReplicationObjectDigestsDiffDuration(objectDigestsDiffTook)
	s.metrics.AddAsyncReplicationObjectsDiffCount(objectsDiffCount)

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
			deleted, notResolved, err := s.resolveObjectConflict(
				propagationCtx, r, s.index.DeletionStrategy(),
				shardDiffReader.TargetNodeName, targetNodeOverridesSnapshot,
				localUpdateTimeByUUID,
			)
			if err != nil {
				return nil, err
			}
			if deleted {
				conflictDeletionCount++
			} else if notResolved {
				objectsNotResolved++
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
			objectsDiffCount:             objectsDiffCount,
			localObjectsPropagationCount: len(localObjectsToPropagate),
			localObjectsPropagationTook:  time.Since(objectsPropagationStart),
			objectsNotResolved:           objectsNotResolved,
		},
	}, nil
}

// resolveObjectConflict applies the configured deletion strategy to a single
// repair response. It returns (deleted=true) when the local object was removed,
// (notResolved=true) when a deletion conflict was detected but could not be
// automatically resolved, and (false, false) when the object was successfully
// propagated without a deletion conflict (r.Deleted=false) or when the local
// version is newer and wins the conflict.
func (s *Shard) resolveObjectConflict(
	ctx context.Context,
	r types.RepairResponse,
	deletionStrategy string,
	targetNodeName string,
	targetOverrides additional.AsyncReplicationTargetNodeOverrides,
	localUpdateTimes map[strfmt.UUID]int64,
) (deleted bool, notResolved bool, err error) {
	if !r.Deleted {
		// Remote object is live (not deleted) — overwrite succeeded, no conflict.
		return false, false, nil
	}
	if deletionStrategy == models.ReplicationConfigDeletionStrategyNoAutomatedResolution ||
		targetOverrides.NoDeletionResolution(targetNodeName) {
		// Deletion conflict detected but auto-resolution is disabled.
		return false, true, nil
	}
	if deletionStrategy == models.ReplicationConfigDeletionStrategyDeleteOnConflict ||
		(deletionStrategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution &&
			r.UpdateTime > localUpdateTimes[strfmt.UUID(r.ID)]) {
		if err := s.DeleteObject(ctx, strfmt.UUID(r.ID), time.UnixMilli(r.UpdateTime)); err != nil {
			return false, false, fmt.Errorf("deleting local objects: %w", err)
		}
		return true, false, nil
	}
	return false, false, nil
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
}

// objectsToPropagateWithinRange returns the local objects in the given hashtree
// leaf range that must be propagated to the target. Per batch it fetches local
// digests (DigestObjectsInRange), drops the ones too recent to propagate (per
// maxUpdateTime), and asks the target via CompareDigests for the subset needing
// action: objects missing on the target (UpdateTime==0, which also covers
// target-side tombstones) and objects the source holds a newer version of. Every
// returned entry is queued; tombstone resolution is left to the post-Overwrite
// resolveObjectConflict path.
func (s *Shard) objectsToPropagateWithinRange(ctx context.Context, config AsyncReplicationConfig,
	targetNodeAddress, targetNodeName string, initialLeaf, finalLeaf uint64, limit int,
	targetNodeOverrides additional.AsyncReplicationTargetNodeOverrides,
	asyncCheckpointCutoff int64,
) (localObjectsCount int, objectsToPropagate []objectToPropagate, err error) {
	objectsToPropagate = make([]objectToPropagate, 0, min(limit, config.diffBatchSize))

	hashtreeHeight := config.hashtreeHeight

	finalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(finalUUIDBytes, finalLeaf<<(64-hashtreeHeight)|((1<<(64-hashtreeHeight))-1))
	copy(finalUUIDBytes[8:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	finalUUID, err := uuidFromBytes(finalUUIDBytes)
	if err != nil {
		return localObjectsCount, objectsToPropagate, err
	}

	// Computed once so every batch uses the same cut-off; a per-batch recompute
	// would let objects near the boundary flip eligibility mid-scan.
	maxUpdateTime := s.getHashBeatMaxUpdateTime(config, targetNodeName, targetNodeOverrides)

	// Checkpoint-driven cycles cap propagation at the cutoff; fallback (cutoff == 0) is a no-op.
	if asyncCheckpointCutoff > 0 && asyncCheckpointCutoff < maxUpdateTime {
		maxUpdateTime = asyncCheckpointCutoff
	}

	currLocalUUIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(currLocalUUIDBytes, initialLeaf<<(64-hashtreeHeight))

	// Reused (reset) each iteration to avoid per-batch allocations.
	filteredDigests := make([]types.RepairResponse, 0, config.diffBatchSize)
	localDigestsByUUID := make(map[uuid.UUID]int64, config.diffBatchSize)

	for limit > 0 && bytes.Compare(currLocalUUIDBytes, finalUUIDBytes) < 1 {
		if ctx.Err() != nil {
			return localObjectsCount, objectsToPropagate, ctx.Err()
		}

		currLocalUUID, err := uuidFromBytes(currLocalUUIDBytes)
		if err != nil {
			return localObjectsCount, objectsToPropagate, err
		}

		currBatchSize := min(limit, config.diffBatchSize)

		allLocalDigests, err := s.index.DigestObjectsInRange(ctx, s.name, currLocalUUID, finalUUID, currBatchSize)
		if err != nil {
			return localObjectsCount, objectsToPropagate, fmt.Errorf("fetching local object digests: %w", err)
		}

		if len(allLocalDigests) == 0 {
			break
		}

		lastLocalUUIDBytes, err := bytesFromUUID(strfmt.UUID(allLocalDigests[len(allLocalDigests)-1].ID))
		if err != nil {
			return localObjectsCount, objectsToPropagate, err
		}

		// Drop too-recent objects; index the rest by parsed UUID so the
		// CompareDigests response can be matched back without re-parsing.
		filteredDigests = filteredDigests[:0]
		clear(localDigestsByUUID)
		for _, d := range allLocalDigests {
			if d.UpdateTime > maxUpdateTime {
				continue
			}
			parsed, err := uuid.Parse(d.ID)
			if err != nil {
				// Unparseable UUID => corrupt local data; log and skip.
				s.index.logger.WithField("uuid", d.ID).
					Error("async replication: skipping local digest with invalid UUID")
				continue
			}
			filteredDigests = append(filteredDigests, d)
			localDigestsByUUID[parsed] = d.UpdateTime
		}
		localObjectsCount += len(filteredDigests)

		if len(filteredDigests) == 0 {
			// Whole batch too recent — but results are UUID-ordered, not
			// UpdateTime-ordered, so later UUIDs may still be eligible: advance
			// past this batch and keep scanning rather than stopping.
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

		// One round-trip: the target returns only the digests needing action
		// (missing or stale; missing also covers target-side tombstones).
		staleDigests, err := s.index.replicator.CompareDigests(ctx, s.name, targetNodeAddress, filteredDigests)
		if err != nil {
			return localObjectsCount, objectsToPropagate, fmt.Errorf("comparing digests with remote: %w", err)
		}

		batchActionCount := 0
		for _, stale := range staleDigests {
			key, err := uuid.Parse(stale.ID)
			if err != nil {
				s.index.logger.WithField("uuid", stale.ID).
					Error("async replication: skipping stale digest with invalid UUID")
				continue
			}
			localUT, ok := localDigestsByUUID[key]
			if !ok {
				// Target returned an ID we did not send — protocol/remote bug; skip.
				s.index.logger.WithField("uuid", stale.ID).
					Error("async replication: target returned a digest not present in source batch")
				continue
			}

			// Queue every returned digest (stale or missing/tombstoned); the
			// target's Overwrite handler + resolveObjectConflict settle deletions.
			objectsToPropagate = append(objectsToPropagate, objectToPropagate{
				uuid:                  strfmt.UUID(stale.ID),
				lastUpdateTime:        localUT,
				remoteStaleUpdateTime: stale.UpdateTime, // 0 when missing-or-tombstoned on target
			})
			batchActionCount++
		}

		if len(allLocalDigests) < currBatchSize {
			break
		}

		overflow := incToNextLexValue(lastLocalUUIDBytes)
		if overflow {
			break
		}

		currLocalUUIDBytes = lastLocalUUIDBytes
		// Charge the budget only for queued propagations, not for scanned-but-current objects.
		limit -= batchActionCount
	}

	// Note: propagations == 0 means local shard is laying behind remote shard,
	// the local shard may receive recent objects when remote shard propagates them

	return localObjectsCount, objectsToPropagate, nil
}

// getHashBeatMaxUpdateTime returns the cutoff update time for propagation.
// Objects with UpdateTime > returned value are too recent to propagate and are
// skipped. An explicit UpperTimeBound override (set per source/target pair)
// takes precedence over the propagationDelay-based default.
func (s *Shard) getHashBeatMaxUpdateTime(config AsyncReplicationConfig, targetNodeName string, targetNodeOverrides additional.AsyncReplicationTargetNodeOverrides) int64 {
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
	return time.Now().Add(-config.propagationDelay).UnixMilli()
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

	numBatches := (len(objectsToPropagate) + config.propagationBatchSize - 1) / config.propagationBatchSize
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
					// Skip ctx-cancellation errors: the parent surfaces the cancel
					// cause via context.Cause(workerCtx) once workers drain.
					// Forwarding ctx errors here would clutter the compounder with
					// N copies of the same cancellation reason.
					if workerCtx.Err() != nil {
						continue
					}
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
					if err != nil && workerCtx.Err() != nil {
						// Same rationale as MultiObjectByID above: ctx cancellation
						// is surfaced by the parent via context.Cause; don't fan it
						// out across resultCh.
						continue
					}

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
		defer close(resultCh)
		workerWg.Wait()
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
