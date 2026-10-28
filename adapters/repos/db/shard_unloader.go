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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// ShardUnloader is the lazy shard unloading policy. The db only calls it when
// [Config.ShardUnloader] is set; a nil unloader leaves every path as it was.
//
// Concurrency contract: BeforeLoad runs on the loading goroutine while that
// wrapper's load mutex is held, so it must not block and must not call back
// into that wrapper. It may hand other wrappers to its own goroutines, which
// unload them with [LazyLoadShard.TryUnload]; that only try-locks, so it can
// never wait on the loading goroutine.
type ShardUnloader interface {
	// Registry returns the registry the db records loaded wrappers in. It must
	// return the same registry for the unloader's whole life.
	Registry() *LoadedShardRegistry
	// BeforeLoad runs before a cold wrapper loads, ahead of the memory and
	// concurrency admission; never for a startup warmup load, which must not
	// make room for itself. It cannot fail the load.
	BeforeLoad(shard *LazyLoadShard)
	// ReserveWarmupLoad reserves room for one startup warmup load and reports
	// false at the limit. Reservations are atomic, so the warmups of all
	// indexes together never pass the limit. Each true is paired with one
	// ReleaseWarmupLoad once that load finished or was abandoned.
	ReserveWarmupLoad() bool
	ReleaseWarmupLoad()
	// SetReindexActivitySource hands the unloader the snapshot it passes in
	// [UnloadRequest.Reindex]; db.New calls it once, before any shard loads.
	SetReindexActivitySource(src func(ctx context.Context) (ReindexActivity, error))
	// Close stops the unloader's goroutines. DB.Shutdown calls it before any
	// index shuts down, so no unload races an index teardown.
	Close(ctx context.Context) error
}

// UnloadMode selects which clauses [LazyLoadShard.TryUnload] applies.
type UnloadMode uint8

const (
	// UnloadModeRequest makes room for a load: only the correctness clauses apply.
	UnloadModeRequest UnloadMode = iota
	// UnloadModeBackground also applies the soft clauses (residency, idleness, queues, hashtree).
	UnloadModeBackground
)

// UnloadRequest parameterises one [LazyLoadShard.TryUnload] attempt.
type UnloadRequest struct {
	Mode UnloadMode
	// MinResidency skips a shard loaded more recently than this (background only).
	MinResidency time.Duration
	// IdleTTL, when set, skips a shard accessed within it (background only).
	IdleTTL time.Duration
	// PickedAt is the victim's LastAccess when the policy picked it; an access
	// since refuses the unload as reaccessed. 0 skips the check.
	PickedAt uint32
	// Reindex is the caller's snapshot from [DB.ReindexActivitySnapshot]; the
	// zero value refuses every shard a runtime reindex could be working on.
	Reindex ReindexActivity
}

// ReindexActivity is one snapshot of the shards a runtime reindex works on,
// taken outside every shard lock because building it may reach the leader.
// A reload does not re-arm a running reindex's double-write callbacks, so
// such a shard must stay loaded.
type ReindexActivity struct {
	taken   bool
	failed  bool
	live    ShardReindexActivityLookup
	cleanup CleanupInProgressLookup
}

// NewReindexActivity is a usable snapshot over live, nil meaning no reindex runs.
func NewReindexActivity(live ShardReindexActivityLookup) ReindexActivity {
	return ReindexActivity{taken: true, live: live}
}

// Usable reports whether the snapshot was taken and its lookup succeeded.
func (a ReindexActivity) Usable() bool { return a.taken && !a.failed }

// blocks reports whether the shard may be under a reindex; the cleanup lookup is node-local and read live.
func (a ReindexActivity) blocks(collection, shard string) UnloadOutcome {
	if !a.Usable() {
		return UnloadOutcomeReindexUnknown
	}
	if (a.live != nil && a.live(collection, shard)) || (a.cleanup != nil && a.cleanup(collection, shard)) {
		return UnloadOutcomeReindex
	}
	return ""
}

// ReindexActivitySnapshot answers [DB.AnyLiveReindexForShard] for every shard
// at once, with ctx bounding the DTM list. An unwired lookup reads as no
// reindex, as AnyLiveReindexForShard does; a failed one is returned unusable.
func (db *DB) ReindexActivitySnapshot(ctx context.Context) (ReindexActivity, error) {
	if db.config.RuntimeReindexDisabled {
		return ReindexActivity{taken: true}, nil
	}
	db.reindexAuditMu.RLock()
	snapshotBuilder := db.shardReindexActivitySnapshotBuilder
	activityBuilder := db.shardReindexActivityLookupBuilder
	cleanupBuilder := db.reindexCleanupInProgressLookupBldr
	db.reindexAuditMu.RUnlock()

	a := ReindexActivity{taken: true}
	switch {
	case snapshotBuilder != nil:
		live, err := snapshotBuilder(ctx)
		if err != nil {
			return ReindexActivity{taken: true, failed: true}, err
		}
		a.live = live
	case activityBuilder != nil:
		a.live = activityBuilder()
	default:
	}
	if cleanupBuilder != nil {
		a.cleanup = cleanupBuilder()
	}
	return a, nil
}

// UnloadOutcome values are meant as Prometheus labels, so renaming one
// empties dashboards with no error.
type UnloadOutcome string

const (
	UnloadOutcomeOK              UnloadOutcome = "ok"
	UnloadOutcomeLockBusy        UnloadOutcome = "lock_busy"
	UnloadOutcomeNotInMap        UnloadOutcome = "not_in_map"
	UnloadOutcomeRecovering      UnloadOutcome = "recovering"
	UnloadOutcomeInUse           UnloadOutcome = "in_use"
	UnloadOutcomeHalted          UnloadOutcome = "halted"
	UnloadOutcomePending2PC      UnloadOutcome = "pending_2pc"
	UnloadOutcomeChangeLog       UnloadOutcome = "changelog"
	UnloadOutcomeAsyncOverride   UnloadOutcome = "async_override"
	UnloadOutcomeAsyncCheckpoint UnloadOutcome = "async_checkpoint"
	UnloadOutcomeVectorOp        UnloadOutcome = "vector_op"
	UnloadOutcomeReindex         UnloadOutcome = "reindex"
	UnloadOutcomeReindexUnknown  UnloadOutcome = "reindex_unknown"
	UnloadOutcomeManualReadOnly  UnloadOutcome = "manual_readonly"
	// UnloadOutcomeResourceReadOnly: disk usage holds the node read-only, where an unload's flush could tear the shard.
	UnloadOutcomeResourceReadOnly UnloadOutcome = "resource_readonly"
	UnloadOutcomeMinResidency     UnloadOutcome = "min_residency"
	UnloadOutcomeNotIdle          UnloadOutcome = "not_idle"
	UnloadOutcomeQueueNotEmpty    UnloadOutcome = "queue_not_empty"
	UnloadOutcomeHashtreeInit     UnloadOutcome = "hashtree_init"
	UnloadOutcomeTorn             UnloadOutcome = "torn"
	UnloadOutcomeIndexClosing     UnloadOutcome = "index_closing"
	UnloadOutcomeAlreadyCold      UnloadOutcome = "already_cold"
	UnloadOutcomeReaccessed       UnloadOutcome = "reaccessed"
)

// Checklist only: the exhaustive lint fails until a new outcome is listed here.
//
//exhaustive:enforce
var _ = map[UnloadOutcome]struct{}{
	UnloadOutcomeOK:               {},
	UnloadOutcomeLockBusy:         {},
	UnloadOutcomeNotInMap:         {},
	UnloadOutcomeRecovering:       {},
	UnloadOutcomeInUse:            {},
	UnloadOutcomeHalted:           {},
	UnloadOutcomePending2PC:       {},
	UnloadOutcomeChangeLog:        {},
	UnloadOutcomeAsyncOverride:    {},
	UnloadOutcomeAsyncCheckpoint:  {},
	UnloadOutcomeVectorOp:         {},
	UnloadOutcomeReindex:          {},
	UnloadOutcomeReindexUnknown:   {},
	UnloadOutcomeManualReadOnly:   {},
	UnloadOutcomeResourceReadOnly: {},
	UnloadOutcomeMinResidency:     {},
	UnloadOutcomeNotIdle:          {},
	UnloadOutcomeQueueNotEmpty:    {},
	UnloadOutcomeHashtreeInit:     {},
	UnloadOutcomeTorn:             {},
	UnloadOutcomeIndexClosing:     {},
	UnloadOutcomeAlreadyCold:      {},
	UnloadOutcomeReaccessed:       {},
}

// LoadedShardRegistry holds every loaded wrapper of the DB in a dense slice, so
// a policy can sample victims without walking every index. It is touched only
// on load and unload. mu is a leaf lock: nothing else is taken while it is held.
type LoadedShardRegistry struct {
	mu          sync.Mutex
	shards      []*LazyLoadShard
	byNamespace map[string]int

	loaded    atomic.Int64
	unloading atomic.Int64
}

func NewLoadedShardRegistry() *LoadedShardRegistry {
	return &LoadedShardRegistry{byNamespace: map[string]int{}}
}

// add registers l; callers hold l.mutex, so add and remove of one wrapper never interleave.
func (r *LoadedShardRegistry) add(l *LazyLoadShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.regSlot != 0 {
		return
	}
	r.shards = append(r.shards, l)
	l.regSlot = len(r.shards)
	r.byNamespace[l.namespace()]++
	r.loaded.Add(1)
}

// remove deregisters l by swap-remove; idempotent. Callers hold l.mutex.
func (r *LoadedShardRegistry) remove(l *LazyLoadShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := l.regSlot
	if slot == 0 {
		return
	}
	last := len(r.shards) - 1
	moved := r.shards[last]
	r.shards[slot-1] = moved
	moved.regSlot = slot
	r.shards[last] = nil
	r.shards = r.shards[:last]
	l.regSlot = 0
	ns := l.namespace()
	if r.byNamespace[ns]--; r.byNamespace[ns] <= 0 {
		delete(r.byNamespace, ns)
	}
	r.loaded.Add(-1)
}

// Loaded is the node-wide number of loaded wrappers.
func (r *LoadedShardRegistry) Loaded() int64 { return r.loaded.Load() }

// Unloading is the number of TryUnload teardowns in flight.
func (r *LoadedShardRegistry) Unloading() int64 { return r.unloading.Load() }

// NamespaceLoaded is the number of loaded wrappers of one namespace ("" for none).
func (r *LoadedShardRegistry) NamespaceLoaded(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byNamespace[namespace]
}

// NamespaceCounts replaces dst's entries with the loaded count per namespace and returns it.
func (r *LoadedShardRegistry) NamespaceCounts(dst map[string]int) map[string]int {
	if dst == nil {
		dst = map[string]int{}
	}
	clear(dst)
	r.mu.Lock()
	defer r.mu.Unlock()
	for ns, n := range r.byNamespace {
		dst[ns] = n
	}
	return dst
}

// Sample appends up to k distinct random loaded wrappers to dst and returns it.
// It allocates only when dst lacks capacity.
func (r *LoadedShardRegistry) Sample(dst []*LazyLoadShard, k int) []*LazyLoadShard {
	if k <= 0 {
		return dst
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.shards)
	if n <= k {
		return append(dst, r.shards...)
	}
	base := len(dst)
	for attempts := 0; len(dst)-base < k && attempts < 4*k; attempts++ {
		candidate := r.shards[rand.IntN(n)]
		duplicate := false
		for _, picked := range dst[base:] {
			if picked == candidate {
				duplicate = true
				break
			}
		}
		if !duplicate {
			dst = append(dst, candidate)
		}
	}
	return dst
}

// accessClock is a coarse seconds clock for LastAccess. Only a running unloader
// ticks it, so a node without one never touches it.
var (
	accessClock      atomic.Uint32
	accessClockEpoch = time.Now()
)

const accessClockTick = time.Second

// AccessClockNow returns the coarse access clock, in seconds since process start.
func AccessClockNow() uint32 { return accessClock.Load() }

// advanceAccessClock never moves the clock back, whichever DB's ticker runs it.
func advanceAccessClock() {
	now := uint32(time.Since(accessClockEpoch)/time.Second) + 1
	for {
		cur := accessClock.Load()
		if cur >= now || accessClock.CompareAndSwap(cur, now) {
			return
		}
	}
}

// startAccessClock ticks the access clock until stop closes.
func startAccessClock(stop <-chan struct{}, logger logrus.FieldLogger) {
	advanceAccessClock()
	enterrors.GoWrapper(func() {
		ticker := time.NewTicker(accessClockTick)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				advanceAccessClock()
			}
		}
	}, logger)
}

// lazyShardUnloading reports whether this index keeps every local shard a
// *LazyLoadShard for its whole life, so the unloader can release it. It needs a
// configured unloader, which only a licensed node with lazy loading on has.
func (c *IndexConfig) lazyShardUnloading() bool {
	return c.ShardUnloader != nil && c.EnableLazyLoadShards
}

// initLoadedLazyShard is initShard's eager branch for an unloading index: it
// loads the shard into a wrapper instead of building a bare *Shard, through the
// same memory and limiter admission. Callers hold shardCreateLocks.
func (i *Index) initLoadedLazyShard(ctx context.Context, shardName string, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics,
) (ShardLike, error) {
	if err := i.ensureShardDir(shardName); err != nil {
		return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
	}
	lazy := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue,
		i.allocChecker, i.shardLoadLimiter, i.shardReindexer, false, i.bitmapBufPool)
	if _, _, err := lazy.loadIfCold(ctx); err != nil {
		// Never published, so it must not stay counted as an unloaded shard.
		promMetrics.DeleteUnloadedShard(monitoring.ShardRegistrationLazy)
		return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
	}
	return lazy, nil
}

// pinWhileUnloading pins a shard a long background walk works on unpinned, so
// the unloader cannot close it mid-walk; with unloading off it does nothing,
// leaving the walk as it was. release is never nil.
func (i *Index) pinWhileUnloading(name string) (release func(), err error) {
	if !i.Config.lazyShardUnloading() {
		return func() {}, nil
	}
	if err := i.enterRead(); err != nil {
		return func() {}, err
	}
	defer i.exitRead()
	shard, release, err := i.pinResidentShard(name, false)
	if err != nil {
		return func() {}, err
	}
	if shard == nil {
		return func() {}, fmt.Errorf("shard %q: %w", name, enterrors.ErrShardNotRegistered)
	}
	return release, nil
}
