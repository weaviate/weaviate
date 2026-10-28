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

//go:build integrationTest

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica"
)

var (
	unloadRaceTolerated = []error{errShardStillInUse, errShutdownInProgress, errAlreadyShutdown}
	errTwoPhaseTaskLost = errors.New("prepared task lost before commit")
)

type unloadRace struct {
	t        *testing.T
	f        *unloadFixture
	mu       sync.Mutex
	ids      []strfmt.UUID
	errs     []error
	ok       map[string]*atomic.Int64
	progress atomic.Int64
}

func newUnloadRace(t *testing.T, f *unloadFixture) *unloadRace {
	return &unloadRace{t: t, f: f, ok: map[string]*atomic.Int64{}}
}

func (r *unloadRace) counter(op string) *atomic.Int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.ok[op]
	if !ok {
		c = &atomic.Int64{}
		r.ok[op] = c
	}
	return c
}

func (r *unloadRace) record(op string, err error, tolerated ...error) {
	r.progress.Add(1)
	if err == nil {
		r.counter(op).Add(1)
		return
	}
	for _, e := range append(tolerated, unloadRaceTolerated...) {
		if errors.Is(err, e) {
			return
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Errorf("%s: %w", op, err))
}

func (r *unloadRace) addID(id strfmt.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *unloadRace) someID() (strfmt.UUID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ids) == 0 {
		return "", false
	}
	return r.ids[len(r.ids)/2], true
}

type unloadContender struct {
	name string
	run  func(r *unloadRace) error
	// tolerated lists contender-specific errors that are not invariant breaks.
	tolerated []error
	pause     time.Duration
}

func unloaderLoop(r *unloadRace) error {
	req := UnloadRequest{Reindex: noReindex, Mode: UnloadModeRequest}
	if r.progress.Load()%2 == 0 {
		req.Mode = UnloadModeBackground
	}
	l, ok := r.f.index.shards.Load(r.f.name).(*LazyLoadShard)
	if !ok {
		return nil
	}
	o := l.TryUnload(context.Background(), req)
	r.counter("outcome=" + string(o)).Add(1)
	switch o {
	case UnloadOutcomeTorn:
		return fmt.Errorf("torn shard")
	case UnloadOutcomeOK:
		r.counter("unloaded").Add(1)
	default:
	}
	return nil
}

func writerLoop(r *unloadRace) error {
	obj := testObject(r.f.index.Config.ClassName.String())
	shard, release, err := r.f.index.getOrInitShard(context.Background(), r.f.name)
	defer release()
	if err != nil {
		return err
	}
	if err := shard.PutObject(context.Background(), obj); err != nil {
		return err
	}
	r.addID(obj.ID())
	return nil
}

func readerLoop(r *unloadRace) error {
	id, ok := r.someID()
	if !ok {
		return nil
	}
	shard, release, err := r.f.index.GetShard(context.Background(), r.f.name)
	defer release()
	if err != nil || shard == nil {
		return err
	}
	exists, err := shard.Exists(context.Background(), id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("object %s lost", id)
	}
	return nil
}

func twoPhaseLoop(r *unloadRace) error {
	ctx := context.Background()
	obj := testObject(r.f.index.Config.ClassName.String())
	reqID := string(obj.ID())
	if resp := r.f.index.ReplicateObject(ctx, r.f.name, reqID, obj, 0); len(resp.Errors) > 0 {
		return fmt.Errorf("prepare: %v", resp.Errors)
	}
	res, ok := r.f.index.CommitReplication(ctx, r.f.name, reqID).(replica.SimpleResponse)
	if !ok {
		return fmt.Errorf("%s: %w", reqID, errTwoPhaseTaskLost)
	}
	if len(res.Errors) > 0 {
		return fmt.Errorf("commit: %v", res.Errors)
	}
	r.addID(obj.ID())
	return nil
}

func haltLoop(r *unloadRace) error {
	ctx := context.Background()
	shard, release, err := r.f.index.getOrInitShard(ctx, r.f.name)
	defer release()
	if err != nil {
		return err
	}
	if err := shard.HaltForTransfer(ctx, false, 0); err != nil {
		return err
	}
	time.Sleep(time.Millisecond)
	return shard.resumeMaintenanceCycles(ctx)
}

func changeLogLoop(r *unloadRace) error {
	ctx := context.Background()
	if err := r.f.index.IncomingStartChangeCapture(ctx, r.f.name, "op-race"); err != nil {
		return err
	}
	return r.f.index.IncomingStopChangeCapture(ctx, r.f.name, "op-race")
}

func blockLoadingLoop(r *unloadRace) error {
	release := r.f.lazy.blockLoading()
	time.Sleep(100 * time.Microsecond)
	release()
	return nil
}

func coldHotLoop(r *unloadRace) error {
	if _, err := r.f.index.UnloadLocalShard(context.Background(), r.f.name); err != nil {
		return err
	}
	if err := r.f.index.initLocalShard(context.Background(), r.f.name); err != nil {
		return err
	}
	_, release, err := r.f.index.getOrInitShard(context.Background(), r.f.name)
	release()
	return err
}

func namespaceSuspendLoop(r *unloadRace) error {
	if _, err := r.f.index.db.UnloadShard(context.Background(), r.f.index.Config.ClassName.String(), r.f.name); err != nil {
		return err
	}
	return r.f.index.db.ReopenShard(context.Background(), r.f.index.Config.ClassName.String(), r.f.name)
}

func unloadContenders() []unloadContender {
	return []unloadContender{
		{name: "reads", run: readerLoop},
		{name: "writes", run: writerLoop},
		{name: "2pc prepare then commit", run: twoPhaseLoop},
		{name: "halt and resume", run: haltLoop},
		{name: "change log activation", run: changeLogLoop},
		{name: "blockLoading", run: blockLoadingLoop},
		{name: "cold then hot", run: coldHotLoop},
		{name: "namespace unload and reopen", run: namespaceSuspendLoop},
	}
}

func (r *unloadRace) loop(stop <-chan struct{}, wg *sync.WaitGroup, c unloadContender) {
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r.record(c.name, c.run(r), c.tolerated...)
			if c.pause > 0 {
				time.Sleep(c.pause)
			}
		}
	}, r.f.index.logger)
}

func goroutineDump() string {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		return fmt.Sprintf("goroutine dump failed: %v", err)
	}
	return buf.String()
}

// runWithWatchdog fails the test with a goroutine dump if no operation completes for stall.
func (r *unloadRace) runWithWatchdog(contenders []unloadContender, d, stall time.Duration) {
	t := r.t
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, c := range contenders {
		r.loop(stop, &wg, c)
	}
	done := make(chan struct{})
	deadline := time.After(d)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last, lastAt := r.progress.Load(), time.Now()
	stopped := false
	for {
		select {
		case <-deadline:
			if !stopped {
				stopped = true
				close(stop)
				enterrors.GoWrapper(func() { wg.Wait(); close(done) }, r.f.index.logger)
			}
		case <-done:
			return
		case <-tick.C:
			if p := r.progress.Load(); p != last {
				last, lastAt = p, time.Now()
			} else if time.Since(lastAt) > stall {
				t.Fatalf("no progress for %v (deadlock?)\n%s", stall, goroutineDump())
			}
		}
	}
}

// requireInvariants checks no error, no lost object and no orphan instance; mustUnload also requires a real unload.
func (r *unloadRace) requireInvariants(mustUnload bool) {
	t := r.t
	f := r.f
	r.mu.Lock()
	errs := append([]error(nil), r.errs...)
	ids := append([]strfmt.UUID(nil), r.ids...)
	r.mu.Unlock()
	require.Empty(t, errs)
	outcomes := map[string]int64{}
	r.mu.Lock()
	for k, v := range r.ok {
		if strings.HasPrefix(k, "outcome=") {
			outcomes[k] = v.Load()
		}
	}
	r.mu.Unlock()
	if mustUnload {
		require.Positive(t, r.counter("unloaded").Load(), "the race never unloaded the shard: %v", outcomes)
	}
	t.Logf("unload outcomes: %v", outcomes)

	outcome, err := f.index.UnloadLocalShard(testCtx(), f.name)
	require.NoError(t, err)
	require.Equal(t, ShardUnloadOutcomeUnloaded, outcome)
	meta, err := shardmeta.Open(shardPath(f.index.path(), f.name), time.Second)
	require.NoError(t, err, "an orphan instance still holds the metadata db")
	require.NoError(t, meta.Close())
	require.Zero(t, f.unloader.registry.Loaded())
	require.Zero(t, f.unloader.registry.Unloading())

	require.NoError(t, f.index.initLocalShard(testCtx(), f.name))
	shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
	require.NoError(t, err)
	defer release()
	for _, id := range ids {
		exists, err := shard.Exists(testCtx(), id)
		require.NoError(t, err)
		require.True(t, exists, "object %s lost", id)
	}
	require.EqualValues(t, 1, f.unloader.registry.Loaded())
}

func TestLazyLoadShard_TryUnloadRacesEachContender(t *testing.T) {
	for _, c := range unloadContenders() {
		t.Run(c.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			r := newUnloadRace(t, f)
			r.addID(f.put(t))
			unloads := unloadContender{name: "unload", run: unloaderLoop}
			r.runWithWatchdog([]unloadContender{unloads, c}, 1500*time.Millisecond, 20*time.Second)
			r.requireInvariants(true)
			require.Positive(t, r.counter(c.name).Load(), "contender never succeeded")
			t.Logf("unloads=%d %s=%d", r.counter("unload").Load(), c.name, r.counter(c.name).Load())
		})
	}
}

func TestLazyLoadShard_TryUnloadStressWithWatchdog(t *testing.T) {
	coldUnloads := map[string]bool{"cold then hot": true, "namespace unload and reopen": true}
	tests := []struct {
		name     string
		withCold bool
	}{
		{name: "unloader with every non-cold contender"},
		{name: "unloader with cold unloads too", withCold: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			r := newUnloadRace(t, f)
			r.addID(f.put(t))
			var contenders []unloadContender
			for _, c := range unloadContenders() {
				if coldUnloads[c.name] && !tt.withCold {
					continue
				}
				if c.name == "2pc prepare then commit" && tt.withCold {
					c.tolerated = []error{errTwoPhaseTaskLost}
				}
				c.pause = time.Millisecond
				contenders = append(contenders, c)
			}
			contenders = append(contenders,
				unloadContender{name: "unload", run: unloaderLoop},
				unloadContender{name: "unload-2", run: unloaderLoop},
				unloadContender{name: "reads-2", run: readerLoop, pause: time.Millisecond},
				unloadContender{name: "writes-2", run: writerLoop, pause: time.Millisecond},
			)
			r.runWithWatchdog(contenders, 5*time.Second, 20*time.Second)
			r.requireInvariants(!tt.withCold)
			var sb strings.Builder
			for _, c := range contenders {
				require.Positive(t, r.counter(c.name).Load(), c.name)
				fmt.Fprintf(&sb, "%s=%d ", c.name, r.counter(c.name).Load())
			}
			t.Log(sb.String())
		})
	}
}

func TestLazyLoadShard_TryUnloadRacesTerminalOps(t *testing.T) {
	tests := []struct {
		name  string
		op    func(f *unloadFixture) error
		after func(t *testing.T, f *unloadFixture)
	}{
		{
			name: "drop",
			op:   func(f *unloadFixture) error { return f.index.dropShards([]string{f.name}) },
			after: func(t *testing.T, f *unloadFixture) {
				require.Nil(t, f.index.shards.Load(f.name))
				require.NoDirExists(t, shardPath(f.index.path(), f.name))
			},
		},
		{
			name: "index shutdown",
			op:   func(f *unloadFixture) error { return f.index.Shutdown(testCtx()) },
			after: func(t *testing.T, f *unloadFixture) {
				meta, err := shardmeta.Open(shardPath(f.index.path(), f.name), time.Second)
				require.NoError(t, err, "an orphan instance still holds the metadata db")
				require.NoError(t, meta.Close())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 5 {
				f := newUnloadFixture(t)
				r := newUnloadRace(t, f)
				r.addID(f.put(t))
				stop := make(chan struct{})
				var wg sync.WaitGroup
				r.loop(stop, &wg, unloadContender{name: "unload", run: unloaderLoop})
				r.loop(stop, &wg, unloadContender{name: "reads", run: readerLoop, tolerated: []error{errAlreadyShutdown, errIndexClosed}})
				time.Sleep(50 * time.Millisecond)
				require.NoError(t, tt.op(f))
				close(stop)
				wg.Wait()
				require.Eventually(t, func() bool { return f.unloader.registry.Loaded() == 0 }, 5*time.Second, 10*time.Millisecond)
				require.Zero(t, f.unloader.registry.Unloading())
				tt.after(t, f)
			}
		})
	}
}

func TestLazyLoadShard_TryUnloadAfterRecoveringPromotion(t *testing.T) {
	f := newUnloadFixture(t)
	id := f.put(t)
	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

	f.lazy.blockLoad(enterrors.ErrShardRecovering)
	rec := &RecoveringShard{LazyLoadShard: f.lazy}
	f.index.shards.Store(f.name, rec)
	require.Equal(t, UnloadOutcomeRecovering, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

	require.NoError(t, f.index.PromoteRecoveringLocalShard(testCtx(), f.name))
	require.Same(t, f.lazy, f.index.shards.Load(f.name))
	require.True(t, f.exists(t, id))
	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	f.requireUnloaded(t)
	require.True(t, f.exists(t, id))
}

func TestLazyLoadShard_TryUnloadLeavesTenantStatus(t *testing.T) {
	state := NewMultiTenantShardingStateBuilder().
		WithIndexName("UnloadTenants").WithNodePrefix("node").
		WithReplicationFactor(1).
		WithTenant("t1", models.TenantActivityStatusHOT).
		Build()
	u := newFakeShardUnloader()
	f := newAddPropertyLazyFixture(t, "UnloadTenants", state, withShardUnloader(u))
	lazy := f.coldShards(t)["t1"]
	require.NotNil(t, lazy)
	_, _, err := lazy.loadIfCold(testCtx())
	require.NoError(t, err)
	require.Equal(t, UnloadOutcomeOK, lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	require.Equal(t, models.TenantActivityStatusHOT, state.Physical["t1"].Status)
	require.Same(t, lazy, f.index.shards.Load("t1"))
}
