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
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backgroundaccess"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// ShardUnloader is the idle shard collector, the policy that unloads idle shards.
// The db only calls it when [Config.ShardUnloader] is set; a nil unloader
// leaves every path as it was.
//
// Concurrency contract: BeforeLoad runs on the loading goroutine while that
// wrapper's load mutex is held, so it must not block and must not call back
// into that wrapper. The unloader's own goroutines unload with
// [LazyLoadShard.TryUnload], which only try-locks, so it can never wait on a load.
type ShardUnloader interface {
	// Registry returns the registry the db records loaded wrappers in. It must
	// return the same registry for the unloader's whole life.
	Registry() *LoadedShardRegistry
	// BeforeLoad runs before a cold wrapper loads, never for a startup warmup load; it cannot fail the load.
	BeforeLoad(shard *LazyLoadShard)
	// Collecting reports whether idle shards are being unloaded now; it reads atomics only.
	Collecting() bool
	// Close stops the unloader's goroutines. DB.Shutdown calls it before any
	// index shuts down, so no unload races an index teardown.
	Close(ctx context.Context) error
}

// UnloadRequest parameterises one [LazyLoadShard.TryUnload] attempt.
type UnloadRequest struct {
	// IdleTTL, when set, skips a shard accessed within it.
	IdleTTL time.Duration
}

// LoadedShardRegistry holds every loaded wrapper of the DB in a dense slice, so
// a policy can sweep them without walking every index. It is touched only
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

// add registers l with the shard it just loaded. Callers hold l.mutex, so add
// and remove of one wrapper never interleave.
func (r *LoadedShardRegistry) add(l *LazyLoadShard, s *Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.regSlot != 0 {
		return
	}
	r.shards = append(r.shards, l)
	l.regSlot = len(r.shards)
	l.regShard = s
	r.byNamespace[l.namespace()]++
	r.loaded.Add(1)
}

// remove deregisters l by swap-remove; idempotent. Callers hold l.mutex.
func (r *LoadedShardRegistry) remove(l *LazyLoadShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(l)
}

// removeShard deregisters l only while it is registered for s, so a late
// deferred shutdown of an old shard never deregisters a reload.
func (r *LoadedShardRegistry) removeShard(l *LazyLoadShard, s *Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.regSlot == 0 || l.regShard != s {
		return
	}
	r.removeLocked(l)
}

func (r *LoadedShardRegistry) removeLocked(l *LazyLoadShard) {
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
	l.regShard = nil
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

// AppendChunk appends up to limit wrappers from slot from on to dst and returns
// dst and the next slot, or -1 past the end. Walking it releases mu between
// chunks, so a concurrent add or remove may make the walk miss or repeat one.
func (r *LoadedShardRegistry) AppendChunk(dst []*LazyLoadShard, from, limit int) ([]*LazyLoadShard, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if from < 0 || from >= len(r.shards) || limit <= 0 {
		return dst, -1
	}
	end := min(from+limit, len(r.shards))
	dst = append(dst, r.shards[from:end]...)
	if end == len(r.shards) {
		return dst, -1
	}
	return dst, end
}

// withBackgroundShardAccess marks ctx as background work: its shard pins never count as access.
func withBackgroundShardAccess(ctx context.Context) context.Context {
	return backgroundaccess.With(ctx)
}

func isBackgroundShardAccess(ctx context.Context) bool {
	return backgroundaccess.Is(ctx)
}

type loadedShardsOnlyKey struct{}

// errLoadedOnlyShardUnloaded is returned by a loaded-only pin of an unloaded lazy shard.
var errLoadedOnlyShardUnloaded = errors.New("shard not loaded")

// withLoadedShardsOnly makes local shard pins return errLoadedOnlyShardUnloaded instead of loading an unloaded lazy shard.
func withLoadedShardsOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, loadedShardsOnlyKey{}, true)
}

func loadedShardsOnly(ctx context.Context) bool {
	only, _ := ctx.Value(loadedShardsOnlyKey{}).(bool)
	return only
}

// accessClock is a coarse seconds clock for LastAccess. Only a node with an
// unloader ticks or stamps it, so a node without one never touches it.
var (
	accessClock      atomic.Uint32
	accessClockEpoch = time.Now()
)

const accessClockTick = time.Second

// AccessClockNow returns the coarse access clock, in seconds since process start.
func AccessClockNow() uint32 { return accessClock.Load() }

// AccessClockIdle reports whether last is at least idleTTL behind now; one spare tick covers an access just before a tick.
func AccessClockIdle(now, last uint32, idleTTL time.Duration) bool {
	return now > last && time.Duration(now-last-1)*accessClockTick >= idleTTL
}

// accessClockReading is the access clock tick for now, from monotonic elapsed time.
func accessClockReading() uint32 {
	return uint32(time.Since(accessClockEpoch)/time.Second) + 1
}

// raiseUint32 stores x unless v already holds x or later, so it never moves v back.
func raiseUint32(v *atomic.Uint32, x uint32) {
	for {
		cur := v.Load()
		if cur >= x || v.CompareAndSwap(cur, x) {
			return
		}
	}
}

// advanceAccessClock never moves the clock back, whichever DB's ticker runs it.
func advanceAccessClock() { raiseUint32(&accessClock, accessClockReading()) }

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

// wrapsShardsForUnloading reports whether new indexes keep every shard in a
// wrapper the unloader can release; lazy loading forced off keeps bare shards.
func (c *Config) wrapsShardsForUnloading() bool {
	return c.ShardUnloader != nil && (c.EnableLazyLoadShards == nil || *c.EnableLazyLoadShards)
}

// initLoadedLazyShard is initShard's eager branch for an unloading index: it
// loads the shard into a wrapper instead of building a bare *Shard, through the
// same memory and limiter admission. Callers hold shardCreateLocks, or own an
// index not yet published, as its startup does with opts skipping the memory
// check and the BeforeLoad the eager startup load never had.
func (i *Index) initLoadedLazyShard(ctx context.Context, shardName string, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics, opts loadOpts,
) (*LazyLoadShard, error) {
	if err := i.ensureShardDir(shardName); err != nil {
		return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
	}
	lazy := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue,
		i.allocChecker, i.shardLoadLimiter, i.shardReindexer, false, i.bitmapBufPool)
	if _, _, err := lazy.load(ctx, opts); err != nil {
		// Never published, so it must not stay counted as an unloaded shard.
		promMetrics.DeleteUnloadedShard(monitoring.ShardRegistrationLazy)
		return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
	}
	return lazy, nil
}
