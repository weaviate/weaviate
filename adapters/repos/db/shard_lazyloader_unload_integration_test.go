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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
	ok       sync.Map
	progress atomic.Int64
}

func (r *unloadRace) counter(op string) *atomic.Int64 {
	c, _ := r.ok.LoadOrStore(op, &atomic.Int64{})
	return c.(*atomic.Int64)
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
	name      string
	run       func(r *unloadRace) error
	tolerated []error
}

func unloaderLoop(r *unloadRace) error {
	l, ok := r.f.index.shards.Load(r.f.name).(*LazyLoadShard)
	if !ok {
		return nil
	}
	if l.TryUnload(context.Background(), UnloadRequest{}) {
		r.counter("unloaded").Add(1)
		return nil
	}
	r.counter("refused").Add(1)
	if s := l.currentShard(); s != nil && s.teardownError() != nil {
		return fmt.Errorf("torn shard: %w", s.teardownError())
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
		return fmt.Errorf("prepare: %w", resp.FirstError())
	}
	res, ok := r.f.index.CommitReplication(ctx, r.f.name, reqID).(replica.SimpleResponse)
	if !ok {
		return fmt.Errorf("%s: %w", reqID, errTwoPhaseTaskLost)
	}
	if len(res.Errors) > 0 {
		return fmt.Errorf("commit: %w", res.FirstError())
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
	if err := shard.HaltForTransfer(ctx, false); err != nil {
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

func isLoadedLoop(r *unloadRace) error {
	if l, ok := r.f.index.shards.Load(r.f.name).(*LazyLoadShard); ok {
		l.isLoaded()
	}
	_, release, err := r.f.index.GetShard(context.Background(), r.f.name)
	release()
	return err
}

func namespaceSuspendLoop(r *unloadRace) error {
	if _, err := r.f.index.db.UnloadShard(context.Background(), r.f.index.Config.ClassName.String(), r.f.name); err != nil {
		return err
	}
	return r.f.index.db.ReopenShard(context.Background(), r.f.index.Config.ClassName.String(), r.f.name)
}

var errPinnedShardShut = errors.New("pin handed out a shut shard")

func wrapperPinLoop(createLocked bool) func(r *unloadRace) error {
	return func(r *unloadRace) error {
		if createLocked {
			r.f.index.shardCreateLocks.RLock(r.f.name)
			defer r.f.index.shardCreateLocks.RUnlock(r.f.name)
		}
		release, err := r.f.lazy.preventShutdown()
		if err != nil {
			if createLocked {
				return fmt.Errorf("pin under the create lock: %s", err.Error())
			}
			return err
		}
		defer release()
		if s := r.f.lazy.currentShard(); s == nil || s.shut.Load() {
			return errPinnedShardShut
		}
		return nil
	}
}

func unloadContenders() []unloadContender {
	return []unloadContender{
		{name: "reads", run: readerLoop},
		{name: "writes", run: writerLoop},
		// A shard shutdown drops a prepared task, idle unload and COLD alike; the shutdown fix owns it.
		{name: "2pc prepare then commit", run: twoPhaseLoop, tolerated: []error{errTwoPhaseTaskLost}},
		{name: "halt and resume", run: haltLoop},
		{name: "change log activation", run: changeLogLoop},
		{name: "isLoaded", run: isLoadedLoop},
		{name: "cold then hot", run: coldHotLoop},
		{name: "namespace unload and reopen", run: namespaceSuspendLoop},
		{name: "wrapper pin under the create lock", run: wrapperPinLoop(true)},
		{name: "wrapper pin", run: wrapperPinLoop(false)},
		{name: "concurrent unload", run: unloaderLoop},
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
		}
	}, r.f.index.logger)
}

// runWithWatchdog races contenders for 1.5s and fails with a goroutine dump if no operation completes for 20s.
func (r *unloadRace) runWithWatchdog(contenders []unloadContender) {
	stop, done := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	for _, c := range contenders {
		r.loop(stop, &wg, c)
	}
	deadline := time.After(1500 * time.Millisecond)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last, lastAt := r.progress.Load(), time.Now()
	for {
		select {
		case <-deadline:
			deadline = nil
			close(stop)
			enterrors.GoWrapper(func() { wg.Wait(); close(done) }, r.f.index.logger)
		case <-done:
			return
		case <-tick.C:
			if p := r.progress.Load(); p != last {
				last, lastAt = p, time.Now()
			} else if time.Since(lastAt) > 20*time.Second {
				var buf bytes.Buffer
				r.t.Fatalf("no progress for 20s (deadlock?): %v\n%s", pprof.Lookup("goroutine").WriteTo(&buf, 2), buf.String())
			}
		}
	}
}

// requireInvariants checks a real unload, no error, no lost object and no orphan instance.
func (r *unloadRace) requireInvariants() {
	t := r.t
	f := r.f
	r.mu.Lock()
	errs := append([]error(nil), r.errs...)
	ids := append([]strfmt.UUID(nil), r.ids...)
	r.mu.Unlock()
	require.Empty(t, errs)
	require.Positive(t, r.counter("unloaded").Load(), "the race never unloaded the shard, refused %d times", r.counter("refused").Load())

	outcome, err := f.index.UnloadLocalShard(testCtx(), f.name)
	require.NoError(t, err)
	require.Equal(t, ShardUnloadOutcomeUnloaded, outcome)
	requireNoOrphanInstance(t, f)
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

func requireNoOrphanInstance(t *testing.T, f *unloadFixture) {
	meta, err := shardmeta.Open(shardPath(f.index.path(), f.name), time.Second)
	require.NoError(t, err, "an orphan instance still holds the metadata db")
	require.NoError(t, meta.Close())
}

func TestLazyLoadShard_TryUnloadRacesEachContender(t *testing.T) {
	for _, c := range unloadContenders() {
		t.Run(c.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			r := &unloadRace{t: t, f: f}
			r.addID(f.put(t))
			unloads := unloadContender{name: "unload", run: unloaderLoop}
			for range 10 {
				r.runWithWatchdog([]unloadContender{unloads, c})
				if r.counter(c.name).Load() > 0 {
					break
				}
			}
			r.requireInvariants()
			require.Positive(t, r.counter(c.name).Load(), "contender never succeeded")
		})
	}
}

func TestLazyLoadShard_TryUnloadRacesTerminalOps(t *testing.T) {
	tests := []struct {
		name  string
		op    func(f *unloadFixture) error
		after func(t *testing.T, f *unloadFixture)
	}{
		{name: "drop", op: func(f *unloadFixture) error { return f.index.dropShards([]string{f.name}) }, after: func(t *testing.T, f *unloadFixture) {
			require.Nil(t, f.index.shards.Load(f.name))
			require.NoDirExists(t, shardPath(f.index.path(), f.name))
		}},
		{name: "index shutdown", op: func(f *unloadFixture) error { return f.index.Shutdown(testCtx()) }, after: requireNoOrphanInstance},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 5 {
				f := newUnloadFixture(t)
				r := &unloadRace{t: t, f: f}
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
	require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))

	f.lazy.blockLoad(enterrors.ErrShardRecovering)
	f.index.shards.Store(f.name, &RecoveringShard{LazyLoadShard: f.lazy})
	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))

	require.NoError(t, f.index.PromoteRecoveringLocalShard(testCtx(), f.name))
	require.Same(t, f.lazy, f.index.shards.Load(f.name))
	require.True(t, f.exists(t, id))
	require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	f.requireUnloaded(t)
	require.True(t, f.exists(t, id))
}

type switchableNamespace struct{ suspended atomic.Bool }

func (e *switchableNamespace) GetNamespace(name string) (api.Namespace, bool) {
	state := api.NamespaceStateActive
	if e.suspended.Load() {
		state = api.NamespaceStateSuspended
	}
	return api.Namespace{Name: name, State: state}, true
}

func TestLazyLoadShard_TryUnloadAndSuspendUnloadInEitherOrder(t *testing.T) {
	tests := []struct {
		name      string
		idleFirst bool
	}{
		{name: "idle unload then suspend unload", idleFirst: true},
		{name: "suspend unload then idle unload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			e := &switchableNamespace{}
			f.index.namespace, f.index.namespacesExister = "alpha", e
			e.suspended.Store(true)

			if tt.idleFirst {
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
				f.requireUnloaded(t)
			}
			outcome, err := f.index.db.UnloadShard(testCtx(), f.index.Config.ClassName.String(), f.name)
			require.NoError(t, err)
			require.Equal(t, ShardUnloadOutcomeUnloaded, outcome)
			require.Nil(t, f.index.shards.Load(f.name))
			require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
			require.Zero(t, f.unloader.registry.Loaded())

			e.suspended.Store(false)
			require.NoError(t, f.index.db.ReopenShard(testCtx(), f.index.Config.ClassName.String(), f.name))
			require.NotNil(t, f.index.shards.Load(f.name))
			require.True(t, f.exists(t, id))
		})
	}
}
