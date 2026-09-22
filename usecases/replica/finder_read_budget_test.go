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

// Replicated reads and writes while one replica is restarting.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// incidentBudget is the wall clock allowed with one replica restarting.
const incidentBudget = 2 * time.Second

// errReplicaRestarting fails a restarting replica's RPC once nobody waits.
var errReplicaRestarting = errors.New("replica restarting")

// errNodeNotReady is what a booting peer answers with over gRPC.
var errNodeNotReady = errors.New("rpc error: code = Unavailable desc = " + replica.NodeNotReadyMsg)

// hang models a replica that accepts the connection but never answers.
func hang(ctx context.Context, gate <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
		return errReplicaRestarting
	}
}

// newGate returns a gate and an idempotent release, also run at test end.
func newGate(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate, release
}

type outcome[T any] struct {
	value   T
	err     error
	elapsed time.Duration
}

// callWithin fails a call that exceeds budget, then unwinds it so it cannot hang.
func callWithin[T any](t *testing.T, budget time.Duration, unwind func(), call func() (T, error)) (outcome[T], bool) {
	t.Helper()
	ch := make(chan outcome[T], 1)
	start := time.Now()
	go func() {
		v, err := call()
		ch <- outcome[T]{value: v, err: err, elapsed: time.Since(start)}
	}()

	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case o := <-ch:
		return o, true
	case <-timer.C:
		unwind()
		select {
		case o := <-ch:
			return o, false
		case <-time.After(30 * time.Second):
			t.Fatal("read did not unwind after its context was cancelled")
			return outcome[T]{}, false
		}
	}
}

// A read's pull and the repair it triggers share one budget instead of stacking.
func TestFinderReadHasNoEndToEndBudget(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		adds  = additional.Properties{}
		proj  = search.SelectProperties{}
	)

	for _, tt := range []struct {
		name string
		// zero means none, as for a read without a client-side timeout
		callerDeadline time.Duration
		// localUTime / peerUTime make the replicas disagree so repair runs.
		localUTime int64
		peerUTime  int64
		hangStage  string
	}{
		{
			// the peer holds the newer version, so repair must fetch it first
			name:       "repair fetch from the restarting replica never returns",
			localUTime: 1,
			peerUTime:  3,
			hangStage:  "fetch",
		},
		{
			// the local copy wins, so repair pushes it to the restarting replica
			name:       "repair overwrite to the restarting replica never returns",
			localUTime: 3,
			peerUTime:  1,
			hangStage:  "overwrite",
		},
		{
			// control: a caller deadline already bounds both stages
			name:           "caller deadline bounds the chain",
			callerDeadline: 200 * time.Millisecond,
			localUTime:     1,
			peerUTime:      3,
			hangStage:      "fetch",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f      = newFakeFactory(t, cls, shard, nodes, false)
				finder = f.newFinder("A")
			)
			gate, release := newGate(t)
			var (
				digestIDs = []strfmt.UUID{id}
				local     = replica.Replica{ID: id, Object: object(id, tt.localUTime)}
				peerR     = []types.RepairResponse{{ID: id.String(), UpdateTime: tt.peerUTime}}
			)

			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, 0).
				Return(local, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, 0).
				Return(peerR, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, 0).
				Return(peerR, nil).Maybe()

			switch tt.hangStage {
			case "fetch":
				f.RClient.EXPECT().FetchObject(anyVal, nodes[1], cls, shard, id, proj, adds, 9).
					RunAndReturn(func(ctx context.Context, _, _, _ string, _ strfmt.UUID,
						_ search.SelectProperties, _ additional.Properties, _ int,
					) (replica.Replica, error) {
						return replica.Replica{}, hang(ctx, gate)
					})
			case "overwrite":
				f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[1], cls, shard, anyVal).
					RunAndReturn(func(ctx context.Context, _, _, _ string,
						_ []*objects.VObject,
					) ([]types.RepairResponse, error) {
						return nil, hang(ctx, gate)
					})
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.callerDeadline > 0 {
				ctx, cancel = context.WithTimeout(ctx, tt.callerDeadline)
				defer cancel()
			}

			got, returned := callWithin(t, incidentBudget, func() { cancel(); release() },
				func() (*storobj.Object, error) {
					return finder.GetOne(ctx, types.ConsistencyLevelQuorum, shard, id, proj, adds)
				})

			assert.Truef(t, returned,
				"GetOne must return within an end-to-end budget of %v; it was still blocked in the %s repair stage after %v",
				incidentBudget, tt.hangStage, incidentBudget)
			if returned {
				assert.Lessf(t, got.elapsed, incidentBudget,
					"GetOne took %v, longer than the %v end-to-end budget", got.elapsed, incidentBudget)
			}
		})
	}
}

// A silent replica is abandoned for an idle healthy one instead of stalling the read.
func TestFinderAbandonsUnresponsiveReplica(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		adds  = additional.Properties{}
		proj  = search.SelectProperties{}
	)

	for _, tt := range []struct {
		name string
		cl   types.ConsistencyLevel
		// accepts the connection and never answers; the rest are healthy
		restarting string
	}{
		{
			// one worker, pinned to the direct candidate, nothing to hand to
			name:       "ONE: direct candidate restarting, two healthy replicas idle",
			cl:         types.ConsistencyLevelOne,
			restarting: "A",
		},
		{
			// two workers, third replica left idle in the retry queue
			name:       "QUORUM: one of two chosen replicas restarting, third healthy",
			cl:         types.ConsistencyLevelQuorum,
			restarting: "B",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f      = newFakeFactory(t, cls, shard, nodes, false)
				finder = f.newFinder("A")
			)
			gate, release := newGate(t)
			var (
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				digestR   = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
			)

			for _, node := range nodes {
				if node == tt.restarting {
					f.RClient.EXPECT().FetchObject(anyVal, node, cls, shard, id, proj, adds, anyVal).
						RunAndReturn(func(ctx context.Context, _, _, _ string, _ strfmt.UUID,
							_ search.SelectProperties, _ additional.Properties, _ int,
						) (replica.Replica, error) {
							return replica.Replica{}, hang(ctx, gate)
						}).Maybe()
					f.RClient.EXPECT().DigestObjects(anyVal, node, cls, shard, digestIDs, anyVal).
						RunAndReturn(func(ctx context.Context, _, _, _ string, _ []strfmt.UUID, _ int,
						) ([]types.RepairResponse, error) {
							return nil, hang(ctx, gate)
						}).Maybe()
					continue
				}
				f.RClient.EXPECT().FetchObject(anyVal, node, cls, shard, id, proj, adds, anyVal).
					Return(item, nil).Maybe()
				f.RClient.EXPECT().DigestObjects(anyVal, node, cls, shard, digestIDs, anyVal).
					Return(digestR, nil).Maybe()
			}

			// no caller deadline: the budget must come from the coordinator
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			got, returned := callWithin(t, incidentBudget, func() { cancel(); release() },
				func() (*storobj.Object, error) {
					return finder.GetOne(ctx, tt.cl, shard, id, proj, adds)
				})

			assert.Truef(t, returned,
				"GetOne must abandon the restarting replica %q and answer from a healthy one within %v",
				tt.restarting, incidentBudget)
			if !returned {
				return
			}
			assert.Lessf(t, got.elapsed, incidentBudget,
				"GetOne waited %v on replica %q while healthy replicas were idle", got.elapsed, tt.restarting)
			assert.NoErrorf(t, got.err,
				"a read must succeed from the healthy replicas while %q is restarting", tt.restarting)
			assert.Equal(t, item.Object, got.value)
		})
	}
}

// An unready repair target must neither block nor fail a read that is already decided.
func TestFinderInlineRepairAgainstUnreadyReplica(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
		adds  = additional.Properties{}
		proj  = search.SelectProperties{}
	)

	for _, tt := range []struct {
		name string
		// never answer the repair RPC, instead of answering node-not-ready
		hangs bool
	}{
		{
			name:  "repair target answers node-not-ready",
			hangs: false,
		},
		{
			name:  "repair target accepts the connection and never answers",
			hangs: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f      = newFakeFactory(t, cls, shard, nodes, false)
				finder = f.newFinder("A")
			)
			gate, release := newGate(t)
			var (
				digestIDs = []strfmt.UUID{id}
				item      = replica.Replica{ID: id, Object: object(id, 3)}
				freshR    = []types.RepairResponse{{ID: id.String(), UpdateTime: 3}}
				staleR    = []types.RepairResponse{{ID: id.String(), UpdateTime: 1}}
				// inline repair round trips aimed at the unready replica
				repairRPCs atomic.Int64
			)

			// A and B agree on the newest version; C is behind, which triggers repair
			f.RClient.EXPECT().FetchObject(anyVal, nodes[0], cls, shard, id, proj, adds, 0).
				Return(item, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, digestIDs, 0).
				Return(freshR, nil)
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, digestIDs, 0).
				Return(staleR, nil)

			f.RClient.EXPECT().OverwriteObjects(anyVal, nodes[2], cls, shard, anyVal).
				RunAndReturn(func(ctx context.Context, _, _, _ string,
					_ []*objects.VObject,
				) ([]types.RepairResponse, error) {
					repairRPCs.Add(1)
					if tt.hangs {
						return nil, hang(ctx, gate)
					}
					return nil, errNodeNotReady
				}).Maybe()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			got, returned := callWithin(t, incidentBudget, func() { cancel(); release() },
				func() (*storobj.Object, error) {
					return finder.GetOne(ctx, types.ConsistencyLevelAll, shard, id, proj, adds)
				})

			assert.Truef(t, returned,
				"GetOne must not block on inline read repair against the unready replica %q; it was still blocked after %v",
				nodes[2], incidentBudget)
			// one attempt is what it costs to learn a replica is unready; a retry budget against it is not
			assert.LessOrEqualf(t, repairRPCs.Load(), int64(1),
				"read repair must not retry inline on the user's read path against unready replica %q", nodes[2])
			if !returned {
				return
			}
			assert.NotErrorIsf(t, got.err, replicaerrors.ErrRepair,
				"a read must not fail with a repair error because replica %q is restarting", nodes[2])
			assert.Equal(t, item.Object, got.value,
				"the newest version was agreed by the healthy replicas and its content already held by the coordinator")
		})
	}
}

// Each write phase runs under a deadline of its own and survives the caller's cancellation.
func TestReplicatorWritePhasesOutliveCaller(t *testing.T) {
	var (
		id    = strfmt.UUID("123")
		cls   = "C1"
		shard = "SH1"
		nodes = []string{"A", "B", "C"}
	)

	for _, tt := range []struct {
		name string
		// phase in which the slow replica holds the write
		phase   string
		timeout time.Duration
	}{
		{
			name:    "prepare",
			phase:   "prepare",
			timeout: replica.DefaultPrepareTimeout,
		},
		{
			name:    "commit",
			phase:   "commit",
			timeout: replica.DefaultCommitTimeout,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, false)
			rep := f.newReplicator()
			gate, release := newGate(t)
			obj := object(id, 3)
			slow := nodes[1]

			// context the slow replica received for tt.phase
			var phaseCtx atomic.Value
			entered := make(chan struct{})
			holdPhase := func(ctx context.Context) error {
				phaseCtx.Store(ctx)
				close(entered)
				<-gate
				return ctx.Err()
			}

			for _, node := range nodes {
				if node == slow && tt.phase == "prepare" {
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						RunAndReturn(func(ctx context.Context, _, _, _, _ string,
							_ *storobj.Object, _ uint64,
						) (replica.SimpleResponse, error) {
							return replica.SimpleResponse{}, holdPhase(ctx)
						})
				} else {
					f.WClient.EXPECT().PutObject(anyVal, node, cls, shard, anyVal, obj, anyVal).
						Return(replica.SimpleResponse{}, nil)
				}

				if node == slow && tt.phase == "commit" {
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						RunAndReturn(func(ctx context.Context, _, _, _, _ string, _ interface{}) error {
							return holdPhase(ctx)
						})
				} else {
					f.WClient.EXPECT().Commit(anyVal, node, cls, shard, anyVal, anyVal).
						Return(nil)
				}
				// issued only if a prepare fails
				f.WClient.EXPECT().Abort(anyVal, node, cls, shard, anyVal).
					Return(replica.SimpleResponse{}, nil).Maybe()
			}

			// a caller deadline far beyond the phase timeout, so an inherited one is detectable
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- rep.PutObject(ctx, shard, obj, types.ConsistencyLevelAll, 123)
			}()

			select {
			case <-entered:
			case <-time.After(incidentBudget):
				t.Fatalf("replica %q never received the %s request", slow, tt.phase)
			}
			cancel()

			got := phaseCtx.Load().(context.Context)
			assert.NoErrorf(t, got.Err(), "the %s phase must survive the caller's cancellation", tt.phase)
			deadline, ok := got.Deadline()
			if assert.Truef(t, ok, "the %s phase must run under a deadline of its own", tt.phase) {
				assert.LessOrEqualf(t, time.Until(deadline), tt.timeout,
					"the %s phase deadline must come from its own %v timeout, not the caller's", tt.phase, tt.timeout)
			}

			release()
			select {
			case err := <-done:
				assert.NoErrorf(t, err, "the write must complete on every replica after the caller left in the %s phase", tt.phase)
			case <-time.After(incidentBudget):
				t.Fatalf("PutObject did not return after replica %q answered", slow)
			}
		})
	}
}
