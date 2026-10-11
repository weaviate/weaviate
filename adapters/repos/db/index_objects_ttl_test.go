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
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// fakeTTLTenantsManager implements the ttlTenantsManager interface for testing.
type fakeTTLTenantsManager struct {
	statusMap map[string]string // tenant name → activity status
	statusErr error             // if non-nil, returned by TenantsStatus

	deactivateCalled []deactivateCall // records each DeactivateTenants call
	deactivateErr    error            // if non-nil, returned by DeactivateTenants
}

type deactivateCall struct {
	ctxWasLive  bool // ctx.Err() == nil at the time of the call
	hasDeadline bool // context had a deadline (from WithTimeout)
	class       string
	tenant      string
}

func (f *fakeTTLTenantsManager) TenantsStatus(class string, tenants ...string) (map[string]string, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	result := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if s, ok := f.statusMap[t]; ok {
			result[t] = s
		}
	}
	return result, nil
}

func (f *fakeTTLTenantsManager) DeactivateTenants(ctx context.Context, class string, tenants ...string) error {
	_, hasDeadline := ctx.Deadline()
	for _, t := range tenants {
		f.deactivateCalled = append(f.deactivateCalled, deactivateCall{
			ctxWasLive:  ctx.Err() == nil,
			hasDeadline: hasDeadline,
			class:       class,
			tenant:      t,
		})
	}
	return f.deactivateErr
}

func newTestLoop(t *testing.T, mgr *fakeTTLTenantsManager, autoActivation bool,
	findFn func(ctx context.Context) ([]strfmt.UUID, error),
	batchFn func(ctx context.Context, uuids []strfmt.UUID) (bool, error),
) tenantTTLLoop {
	t.Helper()
	if batchFn == nil {
		// true is "the batch deleted something", the result that does not stop the loop
		batchFn = func(context.Context, []strfmt.UUID) (bool, error) { return true, nil }
	}
	return tenantTTLLoop{
		class:                 "MyClass",
		tenant:                "tenant_0",
		autoActivationEnabled: autoActivation,
		mgr:                   mgr,
		findUUIDs:             findFn,
		processBatch:          batchFn,
	}
}

// TestShardIsLazyUnloaded checks that only a lazy shard not yet materialized is skipped;
// absent (COLD), loaded lazy, and non-lazy shards are not.
func TestShardIsLazyUnloaded(t *testing.T) {
	tests := []struct {
		name  string
		shard ShardLike // nil means the tenant is absent from the shard map
		want  bool
	}{
		{name: "absent tenant", shard: nil, want: false},
		{name: "lazy shard not loaded", shard: &LazyLoadShard{}, want: true},
		{name: "lazy shard loaded", shard: newLoadedLazyShard(&Shard{}), want: false},
		{name: "non-lazy shard", shard: &Shard{}, want: false},
		{name: "recovering shard not loaded", shard: &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}, want: true},
		{name: "recovering shard loaded", shard: &RecoveringShard{LazyLoadShard: newLoadedLazyShard(&Shard{})}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &Index{}
			if tt.shard != nil {
				idx.shards.Store("tenant_0", tt.shard)
			}
			assert.Equal(t, tt.want, idx.shardIsLazyUnloaded("tenant_0"))
		})
	}
}

// TestTenantTTLLoop_ContextCancelAfterActivation is the primary regression test for
// the server bug reported in:
//
//	test_tenant_auto_activation_during_ttl_deletion (weaviate-e2e-tests CI failure, 2026-04-13)
//
// Bug: when a previously-COLD tenant was auto-activated by the TTL loop and the TTL context
// was canceled mid-deletion (e.g. index drop, node shutdown, TTL round abort), the goroutine
// exited without calling DeactivateTenants. The tenant remained permanently HOT/ACTIVE.
//
// Fix: deferred DeactivateTenants with bounded timeout in tenantTTLLoop.ensureDeactivation.
func TestTenantTTLLoop_ContextCancelAfterActivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	// cancelCtx simulates the TTL context being canceled mid-deletion
	// (e.g. index drop, node shutdown, or TTL round abort).
	cancelCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	batchesProcessed := 0
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			if batchesProcessed == 0 {
				return []strfmt.UUID{"uuid-1", "uuid-2", "uuid-3"}, nil
			}
			// If we reach this point, the loop failed to detect context cancellation.
			return nil, nil
		},
		func(ctx context.Context, _ []strfmt.UUID) (bool, error) {
			batchesProcessed++
			// Simulate the TTL context being canceled after the first batch
			// (e.g. index drop, node shutdown, or TTL round abort).
			cancel(fmt.Errorf("concurrent raft deactivation canceled ttl context"))
			return true, nil
		},
	)

	ec := errorcompounder.New()
	loop.run(cancelCtx, ec)

	require.Equal(t, 1, batchesProcessed, "expected exactly one batch before context cancel")

	// KEY ASSERTION: DeactivateTenants must be called even though the context was canceled.
	require.Len(t, mgr.deactivateCalled, 1, "DeactivateTenants must be called exactly once")
	call := mgr.deactivateCalled[0]
	assert.Equal(t, "MyClass", call.class)
	assert.Equal(t, "tenant_0", call.tenant)

	// The deactivation call must use a live context (not the canceled TTL context)
	// so the RAFT call can complete.
	assert.True(t, call.ctxWasLive,
		"DeactivateTenants must be called with a non-canceled context")
	assert.True(t, call.hasDeadline,
		"DeactivateTenants must be called with a timeout-bounded context")
}

func TestTenantTTLLoop_NormalCompletion_Deactivates(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	finds := atomic.Int32{}
	batches := atomic.Int32{}
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			if finds.Add(1) <= 2 {
				return []strfmt.UUID{"uuid-1"}, nil
			}
			return nil, nil
		},
		func(context.Context, []strfmt.UUID) (bool, error) {
			batches.Add(1)
			return true, nil
		},
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.NoError(t, ec.ToError())
	// a batch that deleted something earns another round
	assert.Equal(t, int32(2), batches.Load())
	assert.Equal(t, int32(3), finds.Load())
	require.Len(t, mgr.deactivateCalled, 1)
	assert.Equal(t, "tenant_0", mgr.deactivateCalled[0].tenant)
}

// TestTenantTTLLoop_StopsWhenABatchMakesNoProgress covers a tenant whose delete cannot
// make progress. The search cannot exclude it, so each round hands it the same uuids.
func TestTenantTTLLoop_StopsWhenABatchMakesNoProgress(t *testing.T) {
	// findUUIDs never runs out below, so only the loop's own exit ends the run. The
	// bound reports a missing exit as a failure instead of hanging the package.
	const maxRounds = 3

	deleteErr := errors.New("shard unavailable")
	closingErr := errors.New("index closing")

	tests := []struct {
		name    string
		batch   func(cancel context.CancelCauseFunc) (bool, error)
		wantErr error // nil means nothing may be filed
	}{
		{
			// the loop ends on the next round's cause check, so the tenant is not swept again
			name: "sweep stopped after the delete succeeded",
			batch: func(cancel context.CancelCauseFunc) (bool, error) {
				cancel(closingErr)
				return true, nil
			},
			wantErr: closingErr,
		},
		{
			name:    "delete fails",
			batch:   func(context.CancelCauseFunc) (bool, error) { return false, deleteErr },
			wantErr: deleteErr,
		},
		{
			name:    "delete reports neither an error nor a count",
			batch:   func(context.CancelCauseFunc) (bool, error) { return false, nil },
			wantErr: errTTLNoProgress,
		},
		{
			name: "sweep stopped while the delete ran",
			batch: func(cancel context.CancelCauseFunc) (bool, error) {
				cancel(errors.New("index closing"))
				return false, nil
			},
			wantErr: nil,
		},
		{
			// only the no-progress arm spares a stopped sweep. A delete that failed
			// is filed whether the sweep was stopped or not
			name: "sweep stopped and the delete failed",
			batch: func(cancel context.CancelCauseFunc) (bool, error) {
				cancel(errors.New("index closing"))
				return false, deleteErr
			},
			wantErr: deleteErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeTTLTenantsManager{
				statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(nil) })

			rounds := 0
			loop := newTestLoop(t, mgr, true,
				func(context.Context) ([]strfmt.UUID, error) {
					rounds++
					require.LessOrEqual(t, rounds, maxRounds,
						"loop kept sweeping: findUUIDs called %d times for a delete that made no progress", rounds)
					return []strfmt.UUID{"uuid-1"}, nil
				},
				func(context.Context, []strfmt.UUID) (bool, error) { return tt.batch(cancel) },
			)

			ec := errorcompounder.New()
			loop.run(ctx, ec)

			assert.Equal(t, 1, rounds, "the tenant must be swept once")
			if tt.wantErr == nil {
				assert.True(t, ec.Empty(), "a stopped sweep is not this tenant failing")
			} else {
				require.Equal(t, 1, ec.Len(), "one filing per swept tenant")
				assert.ErrorIs(t, ec.ToError(), tt.wantErr)
			}
			require.Len(t, mgr.deactivateCalled, 1, "a tenant activated for TTL is deactivated on every exit")
		})
	}
}

// TestTenantTTLLoop_KeepsSweepingATenantThatDeletedPartOfItsBatch pins that the tenant is swept
// again and its failure filed once for the sweep.
func TestTenantTTLLoop_KeepsSweepingATenantThatDeletedPartOfItsBatch(t *testing.T) {
	deleteErr := errors.New("one object failed")

	tests := []struct {
		name string
		// what each round's delete reports alongside deleteErr, before the find runs dry
		deleted    []bool
		wantRounds int
	}{
		{
			name:       "the tenant drains, then the find runs dry",
			deleted:    []bool{true, true, true},
			wantRounds: 4,
		},
		{
			name:       "the last round deletes nothing, which stops the tenant",
			deleted:    []bool{true, true, false},
			wantRounds: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeTTLTenantsManager{
				statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
			}

			rounds := 0
			loop := newTestLoop(t, mgr, true,
				func(context.Context) ([]strfmt.UUID, error) {
					rounds++
					// reports a loop that will not stop as a failure instead of hanging the package
					require.LessOrEqual(t, rounds, len(tt.deleted)+1,
						"loop kept sweeping: findUUIDs called %d times", rounds)
					if rounds > len(tt.deleted) {
						return nil, nil
					}
					return []strfmt.UUID{"uuid-1"}, nil
				},
				func(context.Context, []strfmt.UUID) (bool, error) {
					return tt.deleted[rounds-1], deleteErr
				},
			)

			ec := errorcompounder.New()
			loop.run(context.Background(), ec)

			assert.Equal(t, tt.wantRounds, rounds,
				"a tenant that deleted part of its batch drains in one sweep, not one batch per sweep")
			require.Equal(t, 1, ec.Len(),
				"a tenant retried across rounds reports its failure once for the sweep, not once a round")
			assert.ErrorIs(t, ec.ToError(), deleteErr)
			require.Len(t, mgr.deactivateCalled, 1, "a tenant activated for TTL is deactivated on every exit")
		})
	}
}

func TestTenantTTLLoop_ActiveTenant_NoDeactivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusHOT},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.Empty(t, mgr.deactivateCalled, "active tenant must not be deactivated")
}

func TestTenantTTLLoop_AutoActivationDisabled_NoDeactivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, false, // autoActivation disabled
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.Empty(t, mgr.deactivateCalled)
}

func TestTenantTTLLoop_TenantNotActive_NoDeactivation(t *testing.T) {
	// findUUIDs returning ErrTenantNotActive means the tenant was never successfully
	// activated. The loop should silently skip it AND must NOT trigger a RAFT
	// DeactivateTenants call (the tenant is already COLD).
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			return nil, enterrors.ErrTenantNotActive
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.NoError(t, ec.ToError(), "ErrTenantNotActive must be silently ignored")
	assert.Empty(t, mgr.deactivateCalled, "ErrTenantNotActive must not trigger deactivation")
}

func TestTenantTTLLoop_ContextAlreadyCanceled(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cancel(errors.New("pre-canceled"))

	findCalled := false
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			findCalled = true
			return nil, nil
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(ctx, ec)

	assert.False(t, findCalled, "findUUIDs must not be called when context is already canceled")
	// deactivate was never set (TenantsStatus was never called), so no deactivation.
	assert.Empty(t, mgr.deactivateCalled)
}

func TestTenantTTLLoop_DeactivateError_ReportedInEc(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap:     map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
		deactivateErr: errors.New("raft timeout"),
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	err := ec.ToError()
	require.Error(t, err)
	assert.ErrorContains(t, err, "deactivate tenant")
	assert.ErrorContains(t, err, "raft timeout")
}

// currentGoroutineID reads the id off a one-frame stack dump, so a test can tell the shard
// deleteFromShards ran itself from the ones it handed to the group.
func currentGoroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine 42 [running]:"
	return strings.Fields(string(buf[:n]))[1]
}

// shardDispatchTimeout bounds every wait in runShardDispatch, so a dispatch that never runs a
// shard inline fails in seconds instead of parking the package on a blocked group slot.
const shardDispatchTimeout = 2 * time.Second

// shardDispatchGrace is how long runShardDispatch gives a dispatch to return while the shards it
// gave the group are still held. One that waits for them cannot return in any grace at all, so
// this only prices how fast one that does not wait is caught.
const shardDispatchGrace = 50 * time.Millisecond

// shardDispatchRun records what one deleteFromShards call did. freeSlots counts the group slots
// left when the first shard to run on the calling goroutine ran.
type shardDispatchRun struct {
	deleted   bool
	stopped   bool
	ran       []string
	inline    []string
	got       map[string][]strfmt.UUID
	freeSlots int
	// set when deleteFromShards returned while the shards it gave the group were still blocked
	returnedEarly bool
}

// runShardDispatch calls deleteFromShards on a goroutine of its own and reports what it did. A
// shard the group runs holds its slot until the inline shard has counted the free ones. One free
// slot out of a limit of len(wantRan) therefore means every other shard was dispatched first.
func runShardDispatch(t *testing.T, shards2uuids map[string][]strfmt.UUID, limit int) shardDispatchRun {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(limit)

	var (
		mu           sync.Mutex
		run          shardDispatchRun
		dispatcherID string
		releaseOnce  sync.Once
	)
	release := make(chan struct{})
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	inlineRan := make(chan struct{}, 1)
	done := make(chan struct{})

	run.got = map[string][]strfmt.UUID{}
	deleteShard := func(shard string, uuids []strfmt.UUID) (bool, error) {
		inline := currentGoroutineID() == dispatcherID

		mu.Lock()
		run.ran = append(run.ran, shard)
		run.got[shard] = uuids
		if inline {
			run.inline = append(run.inline, shard)
		}
		first := len(run.inline) == 1
		mu.Unlock()

		if !inline {
			<-release
			return true, nil
		}
		if !first {
			return true, nil
		}

		free := 0
		for eg.TryGo(func() error { <-release; return nil }) {
			free++
		}
		mu.Lock()
		run.freeSlots = free
		mu.Unlock()
		inlineRan <- struct{}{}
		return true, nil
	}

	enterrors.GoWrapper(func() {
		defer close(done)
		dispatcherID = currentGoroutineID()
		deleted, stopped := deleteFromShards(context.Background(), eg, errorcompounder.NewSafe(),
			"MyClass", shards2uuids, map[string]struct{}{}, map[string]struct{}{}, deleteShard)
		mu.Lock()
		run.deleted, run.stopped = deleted, stopped
		mu.Unlock()
	}, logger)

	select {
	case <-inlineRan:
	case <-done:
	case <-time.After(shardDispatchTimeout):
		t.Error("no shard ran on the dispatching goroutine")
	}

	select {
	case <-done:
		mu.Lock()
		run.returnedEarly = true
		mu.Unlock()
	case <-time.After(shardDispatchGrace):
	}
	unblock()

	select {
	case <-done:
	case <-time.After(shardDispatchTimeout):
		t.Fatal("deleteFromShards did not return")
	}
	require.NoError(t, eg.Wait())

	mu.Lock()
	defer mu.Unlock()
	return run
}

// TestDeleteFromShardsRunsTheLastShardInline pins which shard a round keeps for itself. The
// caller waits for the group either way, so handing the last shard over leaves it holding a slot
// to do nothing. A shard with nothing expired is not one of the dispatched.
func TestDeleteFromShardsRunsTheLastShardInline(t *testing.T) {
	cases := []struct {
		name          string
		shards2uuids  map[string][]strfmt.UUID
		wantRan       []string
		limit         int // 0 means a slot for every shard but the inline one
		wantInline    int
		wantFreeSlots int
	}{
		{
			name:          "the only shard runs inline",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}},
			wantRan:       []string{"s1"},
			wantInline:    1,
			wantFreeSlots: 1,
		},
		{
			name:          "the last of two runs inline",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}},
			wantRan:       []string{"s1", "s2"},
			wantInline:    1,
			wantFreeSlots: 1,
		},
		{
			name:          "a shard with a nil or empty uuid list is neither dispatched nor counted",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}, "nil": nil, "empty": {}, "s2": {"b"}},
			wantRan:       []string{"s1", "s2"},
			wantInline:    1,
			wantFreeSlots: 1,
		},
		{
			// the group is saturated for every collection once the node runs GOMAXPROCS sweeps
			name:          "a shard the group has no slot for runs inline too",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
			wantRan:       []string{"s1", "s2", "s3"},
			limit:         1,
			wantInline:    2,
			wantFreeSlots: 0,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			limit := tt.limit
			if limit == 0 {
				limit = len(tt.wantRan)
			}
			run := runShardDispatch(t, tt.shards2uuids, limit)

			require.ElementsMatch(t, tt.wantRan, run.ran, "every shard with expired uuids is swept, and only those")
			require.Len(t, run.inline, tt.wantInline,
				"a shard runs on the calling goroutine when it is last, or when the group refuses it")
			require.Equal(t, tt.wantFreeSlots, run.freeSlots,
				"the first shard run inline is dispatched after every shard the group accepted")
			for _, shard := range tt.wantRan {
				assert.Equal(t, tt.shards2uuids[shard], run.got[shard], "each shard is handed its own uuids")
			}
			if len(tt.wantRan) > 1 {
				assert.False(t, run.returnedEarly,
					"deleteFromShards returns only once the shards it gave the group have finished")
			}
			assert.True(t, run.deleted)
			assert.False(t, run.stopped)
		})
	}
}

// TestDeleteFromShardsWithNothingExpired pins that a round with nothing to delete dispatches no
// shard and reports no deletion, which is how the sweep loop learns to stop.
func TestDeleteFromShardsWithNothingExpired(t *testing.T) {
	run := runShardDispatch(t, map[string][]strfmt.UUID{"s1": {}, "s2": nil}, 1)

	assert.Empty(t, run.ran)
	assert.False(t, run.deleted)
	assert.False(t, run.stopped)
}

// TestDeleteFromShardsStopsOnAStoppedSweep pins that a stopped sweep runs the shard it is on and
// then leaves the rest undispatched, rather than tearing a batch up mid-flight.
func TestDeleteFromShardsStopsOnAStoppedSweep(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	eg := enterrors.NewErrorGroupWrapper(logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var mu sync.Mutex
	ran := []string{}
	_, stopped := deleteFromShards(ctx, eg, errorcompounder.NewSafe(), "MyClass",
		map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
		map[string]struct{}{}, map[string]struct{}{},
		func(shard string, _ []strfmt.UUID) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			ran = append(ran, shard)
			return true, nil
		})
	require.NoError(t, eg.Wait())

	assert.True(t, stopped)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, ran, 1, "the stop is observed after one shard is dispatched, and the shards it never reached are not swept")
}

// TestDeleteFromShardsContainsAPanicToItsShard pins that a panicking shard neither takes the
// sweep loop down with it nor disappears. The other shards are still dispatched, and the panic
// is recorded on the group, which is the only route out for one the group did not run itself.
func TestDeleteFromShardsContainsAPanicToItsShard(t *testing.T) {
	cases := []struct {
		name         string
		shards2uuids map[string][]strfmt.UUID
		panicking    []string
	}{
		{
			name:         "the only shard panics, so the inline one does",
			shards2uuids: map[string][]strfmt.UUID{"s1": {"a"}},
			panicking:    []string{"s1"},
		},
		{
			name:         "every shard panics, so the inline one does whichever it is",
			shards2uuids: map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
			panicking:    []string{"s1", "s2", "s3"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// the integration job disables recovery, under which a panic here kills the binary
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			logger, _ := logrustest.NewNullLogger()
			eg := enterrors.NewErrorGroupWrapper(logger)

			panicking := map[string]bool{}
			for _, shard := range tt.panicking {
				panicking[shard] = true
			}
			var (
				mu      sync.Mutex
				ran     []string
				stopped bool
			)
			ec := errorcompounder.NewSafe()
			dropped := map[string]struct{}{}
			require.NotPanics(t, func() {
				_, stopped = deleteFromShards(context.Background(), eg, ec, "MyClass", tt.shards2uuids,
					dropped, map[string]struct{}{}, func(shard string, _ []strfmt.UUID) (bool, error) {
						mu.Lock()
						ran = append(ran, shard)
						mu.Unlock()
						if panicking[shard] {
							panic("shard delete panicked: " + shard)
						}
						return true, nil
					})
			}, "a panicking shard must not unwind the sweep loop")

			var filed []string
			// the sweep's waiter discards this the same way. run returns nil, so the only error Wait
			// can carry is a panic a group goroutine raised, and collect files that one too
			_ = eg.WaitAndCollect(func(err error, groups ...string) {
				filed = append(filed, fmt.Sprintf("%v %v", groups, err))
			})

			mu.Lock()
			defer mu.Unlock()
			assert.Len(t, ran, len(tt.shards2uuids), "every shard is still dispatched")
			assert.False(t, stopped)
			assert.ElementsMatch(t, tt.panicking, slices.Collect(maps.Keys(dropped)),
				"a delete that panicked deleted nothing, so its shard is dropped for the rest of the sweep")
			assert.Zero(t, ec.Len(), "a panicking shard is filed once, by the group, not also as having made no progress")
			require.Len(t, filed, len(tt.panicking), "one collected panic per panicking shard")
			for _, entry := range filed {
				assert.Contains(t, entry, "[MyClass s", "the panic is filed under its collection and shard")
				assert.Contains(t, entry, "panic occurred")
			}
		})
	}
}

// shardOutcome is what a test's fake delete reports for one shard.
type shardOutcome struct {
	deleted bool
	err     error
	// stopsSweep cancels the sweep from inside the delete, as an index closing under a
	// running batch does
	stopsSweep bool
}

// shardRound records what one deleteFromShards call did over a set of shard outcomes.
type shardRound struct {
	deleted bool
	stopped bool
	ran     []string
	filed   error
	filings int
}

// runShardRound calls deleteFromShards once, handing every shard one uuid and the outcome its
// name maps to. dropped carries in the shards earlier rounds gave up on and filed the ones whose
// failure they already reported, and the call adds to both.
func runShardRound(t *testing.T, outcomes map[string]shardOutcome,
	dropped, filed map[string]struct{},
) shardRound {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.NewSafe()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })

	shards2uuids := make(map[string][]strfmt.UUID, len(outcomes))
	for shard := range outcomes {
		shards2uuids[shard] = []strfmt.UUID{"uuid-1"}
	}

	var (
		mu  sync.Mutex
		ran []string
	)
	deleted, stopped := deleteFromShards(ctx, eg, ec, "MyClass", shards2uuids, dropped, filed,
		func(shard string, _ []strfmt.UUID) (bool, error) {
			mu.Lock()
			ran = append(ran, shard)
			mu.Unlock()

			outcome := outcomes[shard]
			if outcome.stopsSweep {
				cancel(errors.New("index closing"))
			}
			return outcome.deleted, outcome.err
		})
	require.NoError(t, eg.Wait())

	mu.Lock()
	defer mu.Unlock()
	return shardRound{
		deleted: deleted, stopped: stopped, ran: ran,
		filed: ec.ToError(), filings: ec.Len(),
	}
}

// TestDeleteFromShardsDropsAShardThatMadeNoProgress covers a shard whose delete cannot make
// progress. The search cannot exclude it, so each round would hand it the same uuids.
func TestDeleteFromShardsDropsAShardThatMadeNoProgress(t *testing.T) {
	deleteErr := errors.New("shard unavailable")

	tests := []struct {
		name        string
		outcomes    map[string]shardOutcome
		wantDeleted bool
		wantStopped bool
		wantDropped []string
		wantFiled   []string // one rendered fragment per filing; empty means nothing may be filed
	}{
		{
			name:        "a delete that fails is filed",
			outcomes:    map[string]shardOutcome{"s1": {err: deleteErr}},
			wantDropped: []string{"s1"},
			wantFiled:   []string{`"s1": {` + deleteErr.Error()},
		},
		{
			name:        "a delete that reports neither an error nor a count is filed as no progress",
			outcomes:    map[string]shardOutcome{"s1": {}},
			wantDropped: []string{"s1"},
			wantFiled:   []string{`"s1": {` + errTTLNoProgress.Error()},
		},
		{
			name:        "a delete that failed after deleting part of its batch keeps its shard",
			outcomes:    map[string]shardOutcome{"s1": {deleted: true, err: deleteErr}},
			wantDeleted: true,
			wantFiled:   []string{`"s1": {` + deleteErr.Error()},
		},
		{
			name:        "a round where one shard deleted and another did not drops only the other",
			outcomes:    map[string]shardOutcome{"s1": {deleted: true}, "s2": {}},
			wantDeleted: true,
			wantDropped: []string{"s2"},
			wantFiled:   []string{`"s2": {` + errTTLNoProgress.Error()},
		},
		{
			name:        "a shard that deleted is neither filed nor dropped",
			outcomes:    map[string]shardOutcome{"s1": {deleted: true}},
			wantDeleted: true,
		},
		{
			name:        "a sweep stopped while the delete ran is not filed as no progress",
			outcomes:    map[string]shardOutcome{"s1": {stopsSweep: true}},
			wantStopped: true,
			wantDropped: []string{"s1"},
		},
		{
			// only the no-progress arm spares a stopped sweep. A delete that failed
			// is filed whether the sweep was stopped or not
			name:        "a delete that failed on a stopped sweep is filed",
			outcomes:    map[string]shardOutcome{"s1": {err: deleteErr, stopsSweep: true}},
			wantStopped: true,
			wantDropped: []string{"s1"},
			wantFiled:   []string{`"s1": {` + deleteErr.Error()},
		},
		{
			name:        "a delete that deleted on a stopped sweep keeps its shard",
			outcomes:    map[string]shardOutcome{"s1": {deleted: true, stopsSweep: true}},
			wantDeleted: true,
			wantStopped: true,
		},
		{
			name:        "a delete that deleted and failed on a stopped sweep is filed but kept",
			outcomes:    map[string]shardOutcome{"s1": {deleted: true, err: deleteErr, stopsSweep: true}},
			wantDeleted: true,
			wantStopped: true,
			wantFiled:   []string{`"s1": {` + deleteErr.Error()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dropped := map[string]struct{}{}
			run := runShardRound(t, tt.outcomes, dropped, map[string]struct{}{})

			assert.ElementsMatch(t, slices.Collect(maps.Keys(tt.outcomes)), run.ran,
				"every shard with uuids is swept")
			assert.Equal(t, tt.wantDeleted, run.deleted,
				"a round makes progress only where a shard deleted something")
			assert.Equal(t, tt.wantStopped, run.stopped)
			assert.ElementsMatch(t, tt.wantDropped, slices.Collect(maps.Keys(dropped)),
				"only a shard that made no progress is dropped for the rest of the sweep")

			require.Equal(t, len(tt.wantFiled), run.filings, "one filing per shard that could not delete")
			for _, want := range tt.wantFiled {
				assert.ErrorContains(t, run.filed, want)
			}
		})
	}
}

// TestDeleteFromShardsSkipsAShardAnEarlierRoundDropped runs two rounds over one dropped set. The
// shard the first round dropped is not swept again, which is what stops the sweep re-finding the
// uuids no delete of that shard can remove.
func TestDeleteFromShardsSkipsAShardAnEarlierRoundDropped(t *testing.T) {
	outcomes := map[string]shardOutcome{"s1": {deleted: true}, "s2": {}}
	dropped, filed := map[string]struct{}{}, map[string]struct{}{}

	first := runShardRound(t, outcomes, dropped, filed)
	require.ElementsMatch(t, []string{"s1", "s2"}, first.ran)
	require.Equal(t, []string{"s2"}, slices.Collect(maps.Keys(dropped)))

	second := runShardRound(t, outcomes, dropped, filed)
	assert.Equal(t, []string{"s1"}, second.ran, "a shard an earlier round dropped is not swept again")
	assert.True(t, second.deleted)
	assert.NoError(t, second.filed, "a shard already dropped is not filed a second time")

	third := runShardRound(t, outcomes, map[string]struct{}{"s1": {}, "s2": {}}, filed)
	assert.Empty(t, third.ran, "a round whose every shard was dropped sweeps none")
	assert.False(t, third.deleted, "which is how the sweep learns to stop")
}

// TestDeleteFromShardsKeepsAShardThatDeletedPartOfItsBatch pins that the shard is swept again
// and its failure filed once for the sweep.
func TestDeleteFromShardsKeepsAShardThatDeletedPartOfItsBatch(t *testing.T) {
	outcomes := map[string]shardOutcome{"s1": {deleted: true, err: errors.New("one object failed")}}
	dropped, filed := map[string]struct{}{}, map[string]struct{}{}

	first := runShardRound(t, outcomes, dropped, filed)
	require.Equal(t, []string{"s1"}, first.ran)
	require.Equal(t, 1, first.filings)
	require.Empty(t, dropped)

	second := runShardRound(t, outcomes, dropped, filed)
	assert.Equal(t, []string{"s1"}, second.ran,
		"a shard that deleted part of its batch and failed on the rest is swept again")
	assert.Equal(t, 0, second.filings,
		"a shard retried across rounds reports its failure once for the sweep, not once a round")
}

func TestTTLBatchFailedOutright(t *testing.T) {
	deleteErr := errors.New("batch delete: one object failed")

	tests := []struct {
		name    string
		deleted int32
		err     error
		want    bool
	}{
		{name: "a clean batch has not failed", deleted: 5},
		{name: "a batch that deleted nothing without failing has not failed", deleted: 0},
		{
			// the case the pause counter was losing: progress plus a failure is still progress
			name:    "a batch that deleted some and failed on the rest has not failed outright",
			deleted: 5, err: deleteErr,
		},
		{
			name:    "a batch that deleted nothing and failed has failed outright",
			deleted: 0, err: deleteErr, want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ttlBatchFailedOutright(tt.deleted, tt.err))
		})
	}
}

func TestTTLBatchOutcome(t *testing.T) {
	deleteErr := errors.New("batch delete: one object failed")
	pauseErr := errors.New("index closing")

	tests := []struct {
		name     string
		batchErr error
		pauseErr error
		want     []error
	}{
		{name: "a stopped pause alone files its cause", pauseErr: pauseErr, want: []error{pauseErr}},
		{
			// a shutdown during the pause must not replace the failure the batch itself reported
			name:     "a failed batch and a stopped pause file both",
			batchErr: deleteErr, pauseErr: pauseErr, want: []error{deleteErr, pauseErr},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ttlBatchOutcome(tt.batchErr, tt.pauseErr)
			for _, want := range tt.want {
				assert.ErrorIs(t, got, want,
					"an operator investigating a shutdown needs the batch's own failure, not only the cause")
			}
		})
	}
}

func TestTTLPauseAfterBatch(t *testing.T) {
	const pause = time.Millisecond

	tests := []struct {
		name          string
		processed     int
		every         int
		dur           time.Duration
		wantProcessed int
		wantPaused    bool
	}{
		{name: "a batch below the threshold only counts", processed: 0, every: 3, dur: pause, wantProcessed: 1},
		{name: "the batch that reaches the threshold pauses and resets", processed: 2, every: 3, dur: pause, wantProcessed: 0, wantPaused: true},
		{name: "a count already past the threshold still pauses", processed: 9, every: 3, dur: pause, wantProcessed: 0, wantPaused: true},
		{name: "a zero duration counts without pausing", processed: 9, every: 1, dur: 0, wantProcessed: 10},
		{name: "a zero threshold counts without pausing", processed: 9, every: 0, dur: pause, wantProcessed: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			processed := tt.processed
			require.NoError(t, ttlPauseAfterBatch(context.Background(), &processed,
				tt.every, tt.dur, logger))

			assert.Equal(t, tt.wantProcessed, processed)
			if !tt.wantPaused {
				assert.Empty(t, hook.AllEntries(), "nothing pauses, so nothing is logged")
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			assert.Contains(t, hook.LastEntry().Message, "paused for ")
		})
	}
}

// TestTTLPauseAfterBatchReportsAStoppedSweep pins that a sweep aborted while it slept reports the
// cause rather than the pause finishing.
func TestTTLPauseAfterBatchReportsAStoppedSweep(t *testing.T) {
	cause := errors.New("index closing")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	processed := 0

	err := ttlPauseAfterBatch(ctx, &processed, 1, time.Hour, logger)

	require.ErrorIs(t, err, cause)
	assert.Equal(t, 1, processed, "the batch counted before the sweep was stopped")
	assert.Empty(t, hook.AllEntries(), "a pause cut short logs no pause")
}

func TestTTLFailureReason(t *testing.T) {
	deleteErr := errors.New("shard unavailable")
	cause := errors.New("index closing")

	stopped, cancel := context.WithCancelCause(context.Background())
	cancel(cause)

	tests := []struct {
		name    string
		ctx     context.Context
		deleted bool
		err     error
		want    error
	}{
		{name: "a batch that deleted something files nothing", ctx: context.Background(), deleted: true},
		{name: "a batch that failed files its error", ctx: context.Background(), err: deleteErr, want: deleteErr},
		{
			name: "a batch that deleted something and failed files its error",
			ctx:  context.Background(), deleted: true, err: deleteErr, want: deleteErr,
		},
		{
			// the search cannot exclude these uuids, so a later round would hand back the same ones
			name: "a batch that deleted nothing without failing files the sentinel",
			ctx:  context.Background(), want: errTTLNoProgress,
		},
		{
			name: "a stopped sweep also deletes nothing, which is not a failure",
			ctx:  stopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ttlFailureReason(tt.ctx, tt.deleted, tt.err))
		})
	}
}

// TestDeleteFromShardsRecordsEveryShardItDispatchedWhenStopped pins that a sweep stopped part way
// through a round still records every shard it had already dispatched, and dispatches no more.
// The round returns without waiting, so those outcomes land on the group the sweep waits on.
func TestDeleteFromShardsRecordsEveryShardItDispatchedWhenStopped(t *testing.T) {
	const shardCount = 4

	shards2uuids := map[string][]strfmt.UUID{}
	for i := range shardCount {
		shards2uuids[fmt.Sprintf("s%d", i)] = []strfmt.UUID{"uuid-1"}
	}

	logger, _ := logrustest.NewNullLogger()
	eg := enterrors.NewErrorGroupWrapper(logger)
	// one slot, so the round can have at most one shard in flight when the next is dispatched
	eg.SetLimit(1)
	ec := errorcompounder.NewSafe()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })

	var (
		mu      sync.Mutex
		ran     []string
		dropped = map[string]struct{}{}
		filed   = map[string]struct{}{}
	)
	deleted, stopped := deleteFromShards(ctx, eg, ec, "MyClass", shards2uuids, dropped, filed,
		func(shard string, _ []strfmt.UUID) (bool, error) {
			mu.Lock()
			ran = append(ran, shard)
			mu.Unlock()

			cancel(errors.New("index closing"))
			return false, nil
		})

	assert.False(t, deleted)
	assert.True(t, stopped, "a round that saw the sweep stop reports it rather than finishing")

	eg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Less(t, len(ran), shardCount, "a stopped round leaves the rest for the next sweep")
	assert.Len(t, dropped, len(ran), "every shard the round dispatched recorded its outcome")
	for _, shard := range ran {
		assert.Contains(t, dropped, shard)
	}
	assert.True(t, ec.Empty(), "a stopped sweep is not those shards failing")
}
