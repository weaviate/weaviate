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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
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
	batchFn func(ctx context.Context, uuids []strfmt.UUID) error,
) tenantTTLLoop {
	t.Helper()
	if batchFn == nil {
		batchFn = func(context.Context, []strfmt.UUID) error { return nil }
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
		{name: "lazy shard not loaded", shard: &LazyLoadShard{loaded: false}, want: true},
		{name: "lazy shard loaded", shard: &LazyLoadShard{loaded: true}, want: false},
		{name: "non-lazy shard", shard: &Shard{}, want: false},
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
		func(ctx context.Context, _ []strfmt.UUID) error {
			batchesProcessed++
			// Simulate the TTL context being canceled after the first batch
			// (e.g. index drop, node shutdown, or TTL round abort).
			cancel(fmt.Errorf("concurrent raft deactivation canceled ttl context"))
			return nil
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

	calls := atomic.Int32{}
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			if calls.Add(1) == 1 {
				return []strfmt.UUID{"uuid-1"}, nil
			}
			return nil, nil
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.NoError(t, ec.ToError())
	require.Len(t, mgr.deactivateCalled, 1)
	assert.Equal(t, "tenant_0", mgr.deactivateCalled[0].tenant)
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

func TestTenantTTLLoop_DeactivateUsesTimeout(t *testing.T) {
	// Verify the deferred DeactivateTenants call uses a bounded-timeout context,
	// not a bare context.Background().
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	require.Len(t, mgr.deactivateCalled, 1)
	call := mgr.deactivateCalled[0]

	assert.True(t, call.ctxWasLive, "deactivation context must not be expired at call time")
	assert.True(t, call.hasDeadline, "deactivation context must have a deadline (from WithTimeout)")
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
	dispatched int
	stopped    bool
	ran        []string
	inline     []string
	got        map[string][]strfmt.UUID
	freeSlots  int
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
	deleteShard := func(shard string, uuids []strfmt.UUID) {
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
			return
		}
		if !first {
			return
		}

		free := 0
		for eg.TryGo(func() error { <-release; return nil }) {
			free++
		}
		mu.Lock()
		run.freeSlots = free
		mu.Unlock()
		inlineRan <- struct{}{}
	}

	enterrors.GoWrapper(func() {
		defer close(done)
		dispatcherID = currentGoroutineID()
		dispatched, stopped := deleteFromShards(context.Background(), eg, "MyClass", shards2uuids, deleteShard)
		mu.Lock()
		run.dispatched, run.stopped = dispatched, stopped
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
			name:          "the last of three runs inline",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
			wantRan:       []string{"s1", "s2", "s3"},
			wantInline:    1,
			wantFreeSlots: 1,
		},
		{
			name:          "a shard with no expired uuids is neither dispatched nor counted",
			shards2uuids:  map[string][]strfmt.UUID{"s1": {"a"}, "empty": {}, "s2": {"b"}},
			wantRan:       []string{"s1", "s2"},
			wantInline:    1,
			wantFreeSlots: 1,
		},
		{
			name:          "a shard whose uuid list is nil is neither dispatched nor counted",
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
			assert.Equal(t, len(tt.wantRan), run.dispatched)
			assert.False(t, run.stopped)
		})
	}
}

// TestDeleteFromShardsWithNothingExpired pins that a round with nothing to delete dispatches no
// shard and reports none, which is how the sweep loop learns to stop.
func TestDeleteFromShardsWithNothingExpired(t *testing.T) {
	cases := []struct {
		name         string
		shards2uuids map[string][]strfmt.UUID
	}{
		{name: "no shards", shards2uuids: map[string][]strfmt.UUID{}},
		{name: "every shard empty", shards2uuids: map[string][]strfmt.UUID{"s1": {}, "s2": nil}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			run := runShardDispatch(t, tt.shards2uuids, 1)

			assert.Empty(t, run.ran)
			assert.Equal(t, 0, run.dispatched)
			assert.False(t, run.stopped)
		})
	}
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
	dispatched, stopped := deleteFromShards(ctx, eg, "MyClass",
		map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
		func(shard string, _ []strfmt.UUID) {
			mu.Lock()
			defer mu.Unlock()
			ran = append(ran, shard)
		})
	require.NoError(t, eg.Wait())

	assert.Equal(t, 1, dispatched, "the stop is observed after a shard is dispatched, not before")
	assert.True(t, stopped)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, ran, 1, "the shards the stop never reached are not swept")
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
		{
			name:         "one shard of three panics",
			shards2uuids: map[string][]strfmt.UUID{"s1": {"a"}, "s2": {"b"}, "s3": {"c"}},
			panicking:    []string{"s2"},
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
				mu         sync.Mutex
				ran        []string
				dispatched int
				stopped    bool
			)
			require.NotPanics(t, func() {
				dispatched, stopped = deleteFromShards(context.Background(), eg, "MyClass", tt.shards2uuids,
					func(shard string, _ []strfmt.UUID) {
						mu.Lock()
						ran = append(ran, shard)
						mu.Unlock()
						if panicking[shard] {
							panic("shard delete panicked: " + shard)
						}
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
			assert.Equal(t, len(tt.shards2uuids), dispatched)
			assert.False(t, stopped)
			require.Len(t, filed, len(tt.panicking), "one collected panic per panicking shard")
			for _, entry := range filed {
				assert.Contains(t, entry, "[MyClass s", "the panic is filed under its collection and shard")
				assert.Contains(t, entry, "panic occurred")
			}
		})
	}
}
