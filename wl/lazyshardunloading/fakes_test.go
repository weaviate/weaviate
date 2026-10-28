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

package lazyshardunloading

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type fakeShard struct {
	name      string
	namespace string
	reg       *fakeRegistry

	lastAccess atomic.Uint32
	loadedAt   atomic.Int64
	unloadedAt atomic.Int64
	pending    atomic.Bool
	inUse      atomic.Bool
	queueBusy  atomic.Bool
	block      atomic.Pointer[chan struct{}]
	panics     atomic.Bool

	mu   sync.Mutex
	reqs []db.UnloadRequest
}

func (f *fakeShard) LastAccess() uint32 { return f.lastAccess.Load() }

func (f *fakeShard) LoadedAt() time.Time {
	if ns := f.loadedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

func (f *fakeShard) UnloadedAt() time.Time {
	if ns := f.unloadedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

func (f *fakeShard) Namespace() string { return f.namespace }

func (f *fakeShard) UnloadCandidate() bool {
	return f.loadedAt.Load() != 0 && !f.pending.Load() && !f.inUse.Load()
}

func (f *fakeShard) TryMarkUnloadPending() bool { return f.pending.CompareAndSwap(false, true) }

func (f *fakeShard) ClearUnloadPending() { f.pending.Store(false) }

func (f *fakeShard) TryUnload(_ context.Context, req db.UnloadRequest) db.UnloadOutcome {
	defer f.ClearUnloadPending()
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if ch := f.block.Load(); ch != nil {
		<-*ch
	}
	if f.panics.Load() {
		panic("injected unload panic")
	}
	if f.loadedAt.Load() == 0 {
		return db.UnloadOutcomeAlreadyCold
	}
	if f.inUse.Load() {
		return db.UnloadOutcomeInUse
	}
	if req.Mode == db.UnloadModeBackground {
		if req.MinResidency > 0 && time.Since(f.LoadedAt()) < req.MinResidency {
			return db.UnloadOutcomeMinResidency
		}
		if req.IdleTTL > 0 && time.Duration(f.reg.clock()-f.lastAccess.Load())*time.Second < req.IdleTTL {
			return db.UnloadOutcomeNotIdle
		}
		if f.queueBusy.Load() {
			return db.UnloadOutcomeQueueNotEmpty
		}
	}
	f.reg.remove(f)
	f.loadedAt.Store(0)
	f.unloadedAt.Store(time.Now().UnixNano())
	return db.UnloadOutcomeOK
}

func (f *fakeShard) requests() []db.UnloadRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]db.UnloadRequest(nil), f.reqs...)
}

func (f *fakeShard) loaded() bool { return f.loadedAt.Load() != 0 }

func (f *fakeShard) touch() { f.lastAccess.Store(f.reg.clock()) }

type fakeRegistry struct {
	epoch time.Time

	mu     sync.Mutex
	shards []*fakeShard
}

func newFakeRegistry() *fakeRegistry { return &fakeRegistry{epoch: time.Now()} }

func (r *fakeRegistry) clock() uint32 { return uint32(time.Since(r.epoch)/time.Second) + 1 }

func (r *fakeRegistry) load(name, namespace string) *fakeShard {
	f := &fakeShard{name: name, namespace: namespace, reg: r}
	f.markLoaded()
	return f
}

func (f *fakeShard) markLoaded() {
	f.loadedAt.Store(time.Now().UnixNano())
	f.unloadedAt.Store(0)
	f.touch()
	f.reg.mu.Lock()
	f.reg.shards = append(f.reg.shards, f)
	f.reg.mu.Unlock()
}

func (r *fakeRegistry) remove(f *fakeShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range r.shards {
		if s == f {
			r.shards = append(r.shards[:i], r.shards[i+1:]...)
			return
		}
	}
}

func (r *fakeRegistry) Loaded() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.shards))
}

func (r *fakeRegistry) Unloading() int64 { return 0 }

func (r *fakeRegistry) NamespaceLoaded(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.shards {
		if s.namespace == namespace {
			n++
		}
	}
	return n
}

func (r *fakeRegistry) NamespaceCounts(dst map[string]int) map[string]int {
	if dst == nil {
		dst = map[string]int{}
	}
	clear(dst)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.shards {
		dst[s.namespace]++
	}
	return dst
}

func (r *fakeRegistry) Sample(dst []*fakeShard, k int) []*fakeShard {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(dst, r.shards[:min(k, len(r.shards))]...)
}

type testKnobs struct {
	idleTTL, idleCheckInterval, minResidency, interval *runtime.DynamicValue[time.Duration]
	maxLoaded, rate, candidates, workers               *runtime.DynamicValue[int]
	high, low, share                                   *runtime.DynamicValue[float64]
	paused                                             *runtime.DynamicValue[bool]
}

func newTestKnobs() *testKnobs {
	return &testKnobs{
		idleTTL:           runtime.NewDynamicValue(time.Duration(0)),
		idleCheckInterval: runtime.NewDynamicValue(time.Second),
		minResidency:      runtime.NewDynamicValue(30 * time.Second),
		interval:          runtime.NewDynamicValue(time.Second),
		maxLoaded:         runtime.NewDynamicValue(0),
		rate:              runtime.NewDynamicValue(DefaultUnloadRate),
		candidates:        runtime.NewDynamicValue(DefaultMakeRoomCandidates),
		workers:           runtime.NewDynamicValue(1),
		high:              runtime.NewDynamicValue(0.0),
		low:               runtime.NewDynamicValue(0.0),
		share:             runtime.NewDynamicValue(0.0),
		paused:            runtime.NewDynamicValue(false),
	}
}

func (k *testKnobs) knobs() Knobs {
	return Knobs{
		IdleTTL: k.idleTTL, IdleCheckInterval: k.idleCheckInterval, MinResidency: k.minResidency, Interval: k.interval,
		MaxLoadedShards: k.maxLoaded, UnloadRate: k.rate, MakeRoomCandidates: k.candidates, UnloadWorkers: k.workers,
		MemoryHighWatermark: k.high, MemoryLowWatermark: k.low, NamespaceMaxShare: k.share, Paused: k.paused,
	}
}

func set[T runtime.DynamicType](t testing.TB, dv *runtime.DynamicValue[T], v T) {
	if err := dv.SetValue(v); err != nil {
		if t == nil {
			panic(err)
		}
		t.Fatal(err)
	}
}

type harness struct {
	t      testing.TB
	reg    *fakeRegistry
	knobs  *testKnobs
	p      *policy[*fakeShard]
	ratio  atomic.Uint64
	active sync.Map
	prom   *prometheus.Registry
	logs   *logrustest.Hook
}

type harnessOpt func(*harness, *policyDeps)

func withNamespaces() harnessOpt {
	return func(_ *harness, d *policyDeps) { d.namespaces = true }
}

func withQueueCap(n int) harnessOpt {
	return func(_ *harness, d *policyDeps) { d.queueCap = n }
}

func newHarness(t testing.TB, configure func(*testKnobs), opts ...harnessOpt) *harness {
	t.Helper()
	k := newTestKnobs()
	if configure != nil {
		configure(k)
	}
	h := newHarnessWithKnobs(t, k.knobs(), opts...)
	h.knobs = k
	return h
}

func newHarnessWithKnobs(t testing.TB, knobs Knobs, opts ...harnessOpt) *harness {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	h := &harness{t: t, reg: newFakeRegistry(), prom: prometheus.NewPedanticRegistry(), logs: hook}
	deps := policyDeps{
		logger:   logger,
		clock:    h.reg.clock,
		memRatio: func() float64 { return math.Float64frombits(h.ratio.Load()) },
		nsActive: func(ns string) bool {
			v, ok := h.active.Load(ns)
			return !ok || v.(bool)
		},
	}
	for _, opt := range opts {
		opt(h, &deps)
	}
	deps.metrics = newMetrics(h.prom, deps.namespaces, logger)
	h.p = newPolicy[*fakeShard](h.reg, knobs, deps)
	t.Cleanup(func() { require.NoError(t, h.p.close(context.Background())) })
	return h
}

func (h *harness) setRatio(r float64) { h.ratio.Store(math.Float64bits(r)) }

func (h *harness) loadN(n int, namespace string) []*fakeShard {
	out := make([]*fakeShard, n)
	for i := range out {
		out[i] = h.reg.load(namespace+"-"+time.Now().String(), namespace)
		time.Sleep(time.Second)
	}
	return out
}

func (h *harness) loadedCount(shards []*fakeShard) int {
	n := 0
	for _, s := range shards {
		if s.loaded() {
			n++
		}
	}
	return n
}
