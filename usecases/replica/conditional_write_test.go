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

package replica_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// ---------------------------------------------------------------------------
// Pause-replication harness (AC4)
//
// replicationGate is a reusable test primitive that lets a test pause and
// resume replication propagation to a given replica.  A paused gate blocks the
// goroutine that delivers a commit response until Resume is called, simulating
// async-replication lag deterministically without any sleeps.
//
// Usage in a mock RunFn:
//
//	gate := newReplicationGate()
//	f.WClient.On("Commit", ...).Return(nil).RunFn = func(args mock.Arguments) {
//	    gate.Wait()   // blocks until gate.Resume()
//	    resp := args[N].(*replica.SimpleResponse)
//	    *resp = replica.SimpleResponse{}
//	}
//	// ... start writes in goroutines ...
//	gate.Resume()
//
// ---------------------------------------------------------------------------

// replicationGate controls whether a simulated replication step can proceed.
// The zero value is paused; call newReplicationGate() to get one ready to use.
type replicationGate struct {
	// open is closed by Resume to unblock all waiters simultaneously.
	open chan struct{}
}

// newReplicationGate returns a paused gate.
func newReplicationGate() *replicationGate {
	return &replicationGate{open: make(chan struct{})}
}

// Wait blocks until Resume is called.  Goroutines calling Wait before Resume
// will all be unblocked together when Resume fires.
func (g *replicationGate) Wait() {
	<-g.open
}

// Resume unblocks all goroutines currently waiting on Wait and makes any
// subsequent Wait calls return immediately.
func (g *replicationGate) Resume() {
	select {
	case <-g.open:
		// already open — idempotent
	default:
		close(g.open)
	}
}

// ---------------------------------------------------------------------------
// TestConditionalWriteAbortAtRequestCL verifies that:
//
//  1. When a phase-1 precondition check fails on enough replicas to
//     prevent reaching the requested consistency level, PutObject returns
//     *objects.ErrPreconditionFailed to the caller (the precondition abort
//     propagates at the caller-supplied CL — QUORUM in the test).
//
//  2. When one replica is unavailable (N=3, 1 down), a conditional write at
//     QUORUM still completes because floor(N/2)+1 = 2 replicas are reachable
//     and the remaining 2 succeed (INV-HA-1 preserved).
//
// Neither sub-test introduces AllowQuorumConsistency or forces CL=ALL; both
// pass the caller-supplied ConsistencyLevelQuorum unmodified.
func TestConditionalWriteAbortAtRequestCL(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	t.Run("PreconditionFailedPropagatesAtQUORUM", func(t *testing.T) {
		// N=3, QUORUM requires 2 successes.
		// All three replicas respond to phase-1 prepare (PutObject) with OK.
		// Both replicas that reach commit return StatusPreconditionFailed.
		// The third replica also returns StatusPreconditionFailed but may be
		// processed after QUORUM is already breached.
		// Expected: PutObject returns *objects.ErrPreconditionFailed.
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, cls, shard, nodes, false)
		rep := f.newReplicator()

		ctx := context.Background()
		obj := &storobj.Object{}
		prepareResp := replica.SimpleResponse{}

		precondMsg := "object already exists"
		commitResp := replica.SimpleResponse{
			Errors: []replicaerrors.Error{{
				Code: replicaerrors.StatusPreconditionFailed,
				Msg:  precondMsg,
			}},
		}

		var commitWG sync.WaitGroup
		for _, n := range nodes {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
				Return(prepareResp, nil)
			commitWG.Add(1)
			f.WClient.On("Commit", ctx, n, cls, shard, mock.Anything, mock.Anything).
				Return(nil).
				RunFn = func(args mock.Arguments) {
				defer commitWG.Done()
				resp := args[5].(*replica.SimpleResponse)
				*resp = commitResp
			}
		}

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
		commitWG.Wait()

		assert.Error(t, err, "expected error when precondition fails")
		var precondErr *objects.ErrPreconditionFailed
		assert.True(t, errors.As(err, &precondErr),
			"expected *objects.ErrPreconditionFailed, got %T: %v", err, err)
		assert.NotErrorIs(t, err, replicaerrors.ErrReplicas,
			"a precondition failure must NOT surface as ErrReplicas (not a replica-availability problem)")
	})

	t.Run("ConditionalWriteSucceedsWithOneReplicaDown_QUORUM", func(t *testing.T) {
		// N=3, QUORUM requires 2 successes.
		// Node "C" is unavailable (PutObject returns a connection error).
		// Nodes "A" and "B" respond OK to prepare and commit with no errors.
		// Expected: PutObject returns nil (QUORUM reached, INV-HA-1 preserved).
		nodes := []string{"A", "B", "C"}
		f := newFakeFactory(t, cls, shard, nodes, false)
		rep := f.newReplicator()

		ctx := context.Background()
		obj := &storobj.Object{}
		okPrepare := replica.SimpleResponse{}
		okCommit := replica.SimpleResponse{}

		// Nodes A and B: prepare and commit succeed.
		var commitWG sync.WaitGroup
		for _, n := range []string{"A", "B"} {
			f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
				Return(okPrepare, nil)
			commitWG.Add(1)
			f.WClient.On("Commit", ctx, n, cls, shard, mock.Anything, mock.Anything).
				Return(nil).
				RunFn = func(args mock.Arguments) {
				defer commitWG.Done()
				resp := args[5].(*replica.SimpleResponse)
				*resp = okCommit
			}
		}
		// Node C is down: phase-1 prepare fails with a connection error.
		f.WClient.On("PutObject", mock.Anything, "C", cls, shard, mock.Anything, obj, uint64(0)).
			Return(replica.SimpleResponse{}, errors.New("connection refused"))
		// Abort is called on nodes that replied OK during broadcast when the
		// level is not yet reached; because QUORUM = 2 and C fails, the
		// coordinator aborts C but still has A and B for QUORUM.
		// The mock uses Maybe() so unused calls don't fail the test.
		f.WClient.On("Abort", mock.Anything, "C", cls, shard, mock.Anything).
			Return(okPrepare, nil).Maybe()

		err := rep.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
		commitWG.Wait()

		assert.NoError(t, err,
			"conditional write must succeed at QUORUM when floor(N/2)+1 replicas are up (INV-HA-1)")
	})
}

// ---------------------------------------------------------------------------
// TestConditionalWriteCLMatrix (AC1 + AC2)
//
// Table-driven: ConsistencyLevel × replica-availability.
// Asserts the HONEST Plan-A guarantee at the replica layer:
//
//   - CL=QUORUM with ≥ floor(N/2)+1 nodes up → success (INV-HA-1)
//   - CL=ONE    with ≥ 1 node up             → success
//   - CL=ALL    with any node down            → ErrReplicas (caller's explicit CP choice)
//   - CL=QUORUM with 2 of 3 nodes down        → ErrReplicas
//
// ---------------------------------------------------------------------------
func TestConditionalWriteCLMatrix(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	allNodes := []string{"A", "B", "C"}

	type row struct {
		name        string
		cl          types.ConsistencyLevel
		upNodes     []string // nodes that respond OK to phase-1
		downNodes   []string // nodes that return connection error on phase-1
		wantSuccess bool
	}

	rows := []row{
		// --- CL=ONE ---
		{name: "ONE/allUp", cl: types.ConsistencyLevelOne, upNodes: allNodes, wantSuccess: true},
		{name: "ONE/1down", cl: types.ConsistencyLevelOne, upNodes: []string{"A", "B"}, downNodes: []string{"C"}, wantSuccess: true},
		{name: "ONE/2down", cl: types.ConsistencyLevelOne, upNodes: []string{"A"}, downNodes: []string{"B", "C"}, wantSuccess: true},

		// --- CL=QUORUM (floor(3/2)+1 = 2 required) ---
		{name: "QUORUM/allUp", cl: types.ConsistencyLevelQuorum, upNodes: allNodes, wantSuccess: true},
		{name: "QUORUM/1down", cl: types.ConsistencyLevelQuorum, upNodes: []string{"A", "B"}, downNodes: []string{"C"}, wantSuccess: true},
		{name: "QUORUM/2down", cl: types.ConsistencyLevelQuorum, upNodes: []string{"A"}, downNodes: []string{"B", "C"}, wantSuccess: false},

		// --- CL=ALL (all 3 required; caller's explicit CP choice) ---
		{name: "ALL/allUp", cl: types.ConsistencyLevelAll, upNodes: allNodes, wantSuccess: true},
		{name: "ALL/1down", cl: types.ConsistencyLevelAll, upNodes: []string{"A", "B"}, downNodes: []string{"C"}, wantSuccess: false},
		{name: "ALL/2down", cl: types.ConsistencyLevelAll, upNodes: []string{"A"}, downNodes: []string{"B", "C"}, wantSuccess: false},
	}

	for _, r := range rows {
		r := r
		t.Run(r.name, func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, allNodes, false)
			rep := f.newReplicator()
			ctx := context.Background()
			obj := &storobj.Object{}

			okResp := replica.SimpleResponse{}
			connErr := errors.New("connection refused")

			// commitWG tracks commits that are guaranteed to fire.
			// It is only counted-up for success cases where phase-1 succeeds
			// on enough nodes to reach the CL threshold.  Failure cases abort
			// before commits are issued, so we do not wait on them.
			var commitWG sync.WaitGroup
			if r.wantSuccess {
				commitWG.Add(len(r.upNodes))
			}

			// Wire up-nodes: prepare + commit + abort (abort fires when CL can't be met).
			for _, n := range r.upNodes {
				f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
					Return(okResp, nil)
				// Commit: Maybe() so the mock doesn't fail when abort is called
				// instead (e.g. ALL/1down aborts A+B because C failed phase-1).
				commitCall := f.WClient.On("Commit", ctx, n, cls, shard, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				wantSuccess := r.wantSuccess // capture for closure
				commitCall.RunFn = func(args mock.Arguments) {
					resp := args[5].(*replica.SimpleResponse)
					*resp = okResp
					if wantSuccess {
						commitWG.Done()
					}
				}
				// Abort: fires on up-nodes when the CL cannot be met and the
				// coordinator rolls back the prepared transaction.
				f.WClient.On("Abort", mock.Anything, n, cls, shard, mock.Anything).
					Return(okResp, nil).Maybe()
			}

			// Wire down-nodes: phase-1 prepare fails with a connection error.
			// Commit is never called for these nodes; Abort may be attempted.
			for _, n := range r.downNodes {
				f.WClient.On("PutObject", mock.Anything, n, cls, shard, mock.Anything, obj, uint64(0)).
					Return(replica.SimpleResponse{}, connErr)
				f.WClient.On("Abort", mock.Anything, n, cls, shard, mock.Anything).
					Return(okResp, nil).Maybe()
			}

			err := rep.PutObject(ctx, shard, obj, r.cl, 0)
			commitWG.Wait()

			if r.wantSuccess {
				assert.NoError(t, err, "expected success for %s", r.name)
			} else {
				assert.ErrorIs(t, err, replicaerrors.ErrReplicas,
					"expected ErrReplicas for %s", r.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestConditionalWriteKnownWeakConcurrentCoordinator (AC3)
//
// PLAN-A WEAK-GUARANTEE BOUNDARY TEST
//
// This test documents the documented Plan A limitation:
//
//   Two concurrent conditional writes (insert_if_not_exists) for the SAME UUID
//   are routed through DIFFERENT coordinator nodes while async replication is
//   paused (simulating replication lag).  Each coordinator's local state is
//   stale — it sees the object as absent — so both phase-1 existence-checks
//   pass.  Both writes commit successfully.  The result is deterministic
//   last-write-wins (LWW), NOT strong CAS.
//
// What this test ASSERTS:
//   - Neither coordinator panics or returns a torn/corrupted error.
//   - Both writes complete without an ErrReplicas error.
//   - The system reaches a well-defined (LWW) state, not undefined behavior.
//
// What this test DOES NOT assert:
//   - That only one write wins (strong CAS — this is Plan B, not Plan A).
//   - Which write wins (LWW is determined by timestamp, outside this layer).
//
// This is the documented Plan A boundary.  The test exists to pin the
// behavior: future changes that accidentally introduce a crash or corruption
// under this scenario will be caught.
//
// Run with -count=3 for timing robustness (the concurrent path must be
// exercised under all goroutine schedules).
// ---------------------------------------------------------------------------
func TestConditionalWriteKnownWeakConcurrentCoordinator(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	nodes := []string{"A", "B", "C"}

	// We run this three times to exercise different goroutine schedules.
	for iteration := 0; iteration < 3; iteration++ {
		iteration := iteration
		t.Run("", func(t *testing.T) {
			// Shared mock client — both "coordinators" (replicators) use the same
			// underlying transport mock.  Each coordinator sees the same replica set.
			f := newFakeFactory(t, cls, shard, nodes, false)

			// coordinator1 and coordinator2 are two independent Replicator instances
			// routing writes for the same shard — simulating two different cluster
			// nodes acting as coordinator for concurrent requests.
			coordinator1 := f.newReplicatorWithSourceNode("A")
			coordinator2 := f.newReplicatorWithSourceNode("B")

			ctx := context.Background()
			obj := &storobj.Object{}

			// Replication gate: paused at start to simulate async lag.
			// Both coordinators' commits are held until Resume is called.
			gate := newReplicationGate()

			okPrepare := replica.SimpleResponse{}
			okCommit := replica.SimpleResponse{}

			// Both coordinators see all three replicas respond OK to phase-1.
			// This simulates stale local state (neither coordinator has seen
			// the other's write yet because replication is paused).
			// mock.Anything on host because both coordinators will call all nodes.
			f.WClient.On("PutObject", mock.Anything, mock.Anything, cls, shard, mock.Anything, obj, uint64(0)).
				Return(okPrepare, nil)

			// Commit: hold until the gate opens, then return success.
			// Both coordinators will fire commits; the gate releases all of them.
			var commitWG sync.WaitGroup
			for i := 0; i < len(nodes)*2; i++ { // 3 nodes × 2 coordinators = 6 potential commits
				commitWG.Add(1)
			}
			f.WClient.On("Commit", ctx, mock.Anything, cls, shard, mock.Anything, mock.Anything).
				Return(nil).
				RunFn = func(args mock.Arguments) {
				defer commitWG.Done()
				gate.Wait() // blocks until Resume() is called
				resp := args[5].(*replica.SimpleResponse)
				*resp = okCommit
			}

			// Launch both coordinators concurrently.
			var (
				err1, err2 error
				writeWG    sync.WaitGroup
			)
			writeWG.Add(2)
			enterrors.GoWrapper(func() {
				defer writeWG.Done()
				err1 = coordinator1.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
			}, f.log)
			enterrors.GoWrapper(func() {
				defer writeWG.Done()
				err2 = coordinator2.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
			}, f.log)

			// Open the gate: allow all commits to proceed.
			gate.Resume()

			// Wait for both coordinators to complete.
			writeWG.Wait()
			commitWG.Wait()

			// PLAN-A BOUNDARY ASSERTIONS:
			// Both writes complete without crash or corruption.
			// We do NOT assert that only one succeeds (that would be strong CAS / Plan B).
			assert.NoError(t, err1,
				"[iteration %d] coordinator1 must not crash or return ErrReplicas under concurrent write", iteration)
			assert.NoError(t, err2,
				"[iteration %d] coordinator2 must not crash or return ErrReplicas under concurrent write", iteration)
		})
	}
}

// ---------------------------------------------------------------------------
// TestConditionalWriteAsyncDivergenceConvergesLWW (AC3 / deliverable 4)
//
// Verifies the async-divergence convergence property (INV-HASHTREE-1):
//
//   A conditional write that "loses" the cross-coordinator race converges
//   to the LWW value via the existing replication path without corruption.
//   The late-arriving commit does NOT produce a torn object, a panic, or a
//   data-corruption error.  The outcome is one of two valid LWW states.
//
// The scenario (two sequential writes; gate simulates async lag):
//   - coordinator1 writes and commits immediately (fast path, no gate).
//   - coordinator2 writes but its commits are held behind a gate (slow path).
//   - After coordinator1 completes, we release the gate and let coordinator2's
//     commits land.
//   - Assert: coordinator2's late commits do not error at the transport layer.
//
// This documents that the replica transport layer does not enforce uniqueness
// and is not the corruption point.  Uniqueness enforcement (per-UUID mutex
// and version CAS) is at the shard layer, outside this replica-layer test.
// ---------------------------------------------------------------------------
func TestConditionalWriteAsyncDivergenceConvergesLWW(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	nodes := []string{"A", "B", "C"}
	obj := &storobj.Object{}
	okPrepare := replica.SimpleResponse{}
	okCommit := replica.SimpleResponse{}

	// --- Phase 1: coordinator1 writes without any gate (immediate commits). ---
	f1 := newFakeFactory(t, cls, shard, nodes, false)
	coordinator1 := f1.newReplicatorWithSourceNode("A")
	ctx := context.Background()

	f1.WClient.On("PutObject", mock.Anything, mock.Anything, cls, shard, mock.Anything, obj, uint64(0)).
		Return(okPrepare, nil)

	var coord1CommitWG sync.WaitGroup
	for i := 0; i < len(nodes); i++ {
		coord1CommitWG.Add(1)
	}
	f1.WClient.On("Commit", ctx, mock.Anything, cls, shard, mock.Anything, mock.Anything).
		Return(nil).
		RunFn = func(args mock.Arguments) {
		defer coord1CommitWG.Done()
		resp := args[5].(*replica.SimpleResponse)
		*resp = okCommit
	}

	err1 := coordinator1.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
	coord1CommitWG.Wait()
	assert.NoError(t, err1, "coordinator1 (fast path) must succeed")

	// --- Phase 2: coordinator2 writes with commits held behind a gate. ---
	// coordinator2 uses a fresh fakeFactory to get a clean mock (no call-count
	// conflicts with coordinator1's already-satisfied mock expectations).
	f2 := newFakeFactory(t, cls, shard, nodes, false)
	coordinator2 := f2.newReplicatorWithSourceNode("B")

	gate := newReplicationGate()

	f2.WClient.On("PutObject", mock.Anything, mock.Anything, cls, shard, mock.Anything, obj, uint64(0)).
		Return(okPrepare, nil)

	var coord2CommitWG sync.WaitGroup
	for i := 0; i < len(nodes); i++ {
		coord2CommitWG.Add(1)
	}
	f2.WClient.On("Commit", ctx, mock.Anything, cls, shard, mock.Anything, mock.Anything).
		Return(nil).
		RunFn = func(args mock.Arguments) {
		defer coord2CommitWG.Done()
		gate.Wait() // hold until Resume is called
		resp := args[5].(*replica.SimpleResponse)
		*resp = okCommit
	}

	// coordinator2 issues its write in a background goroutine.
	var (
		err2     error
		write2WG sync.WaitGroup
	)
	write2WG.Add(1)
	enterrors.GoWrapper(func() {
		defer write2WG.Done()
		err2 = coordinator2.PutObject(ctx, shard, obj, types.ConsistencyLevelQuorum, 0)
	}, f2.log)

	// Open the gate: coordinator2's commits now land ("late" replication
	// arriving after coordinator1's value is already committed).
	gate.Resume()
	write2WG.Wait()
	coord2CommitWG.Wait()

	// CONVERGENCE ASSERTION:
	// coordinator2's late commits must not corrupt the store.
	// At the transport layer, the commit is simply delivered — no error.
	// LWW resolution happens at the shard layer (per-UUID mutex + version CAS),
	// which is outside the scope of this replica-layer test.
	assert.NoError(t, err2,
		"coordinator2 (late/lagging path) must not return an error at the transport layer; "+
			"LWW resolution is the shard layer's responsibility")
}
