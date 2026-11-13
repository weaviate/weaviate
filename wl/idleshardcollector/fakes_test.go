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

package idleshardcollector

import (
	"context"
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
	block      atomic.Pointer[chan struct{}]
	panics     atomic.Bool
	// refuse makes every TryUnload refuse, as a shard shutdown can.
	refuse atomic.Bool
	// seq is the registry-wide order in which TryUnload was entered, 0 if never.
	seq atomic.Int64

	mu   sync.Mutex
	reqs []db.UnloadRequest
}

func (f *fakeShard) LastAccess() uint32 { return f.lastAccess.Load() }

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

func (f *fakeShard) TryUnload(_ context.Context, req db.UnloadRequest) bool {
	defer f.ClearUnloadPending()
	f.seq.Store(f.reg.unloadSeq.Add(1))
	if d := f.reg.unloadDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if ch := f.block.Load(); ch != nil {
		<-*ch
	}
	if f.panics.Load() {
		panic("injected unload panic")
	}
	if f.loadedAt.Load() == 0 || f.inUse.Load() || f.refuse.Load() {
		return false
	}
	if req.IdleTTL > 0 && !db.AccessClockIdle(f.reg.clock(), f.lastAccess.Load(), req.IdleTTL) {
		return false
	}
	f.reg.remove(f)
	f.loadedAt.Store(0)
	f.unloadedAt.Store(time.Now().UnixNano())
	return true
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

	unloadSeq   atomic.Int64
	unloadDelay atomic.Int64

	mu     sync.Mutex
	shards []*fakeShard
	chunks int
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

func (r *fakeRegistry) AppendChunk(dst []*fakeShard, from, limit int) ([]*fakeShard, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks++
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

type testKnobs struct {
	idleTTL, checkInterval *runtime.DynamicValue[time.Duration]
	rate, workers          *runtime.DynamicValue[int]
	paused                 *runtime.DynamicValue[bool]
}

func newTestKnobs() *testKnobs {
	return &testKnobs{
		idleTTL:       runtime.NewDynamicValue(time.Duration(0)),
		checkInterval: runtime.NewDynamicValue(time.Second),
		rate:          runtime.NewDynamicValue(DefaultUnloadRate),
		workers:       runtime.NewDynamicValue(1),
		paused:        runtime.NewDynamicValue(false),
	}
}

func (k *testKnobs) knobs() Knobs {
	return Knobs{
		IdleTTL: k.idleTTL, CheckInterval: k.checkInterval,
		UnloadRate: k.rate, UnloadWorkers: k.workers, Paused: k.paused,
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
	active sync.Map
	prom   *prometheus.Registry
	logs   *logrustest.Hook
}

type harnessOpt func(*harness, *policyDeps)

func withNamespaces() harnessOpt {
	return func(_ *harness, d *policyDeps) { d.namespaces = true }
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
		logger: logger,
		clock:  h.reg.clock,
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
