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

package replication_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// seedSpec declares one op to inject through Restore. ChangeState writes
// Status.Current.StartTimeUnixMs from time.Now(), so the mutators cannot set an
// op's age; Restore is the only exported route that preserves timestamps.
type seedSpec struct {
	id              uint64
	state           api.ShardReplicationState
	stateStartMs    int64
	shouldCancel    bool
	shouldDelete    bool
	srcNode         string
	tgtNode         string
	collection      string
	shard           string
	transferType    api.ShardReplicationTransferType
	opCreatedUnixMs int64
}

func (s seedSpec) withDefaults() seedSpec {
	if s.srcNode == "" {
		s.srcNode = fmt.Sprintf("source%d", s.id)
	}
	if s.tgtNode == "" {
		s.tgtNode = "target"
	}
	if s.collection == "" {
		s.collection = "TestClass"
	}
	if s.shard == "" {
		s.shard = "shard1"
	}
	return s
}

// seedViaRestore builds a snapshot from specs and restores it into fsm. Every op
// needs a distinct UUID: insertOpIntoFSM writes idsByUuid[op.UUID], so ops left
// with the constructor's empty UUID all collide on "".
func seedViaRestore(t testing.TB, fsm *replication.ShardReplicationFSM, specs ...seedSpec) {
	t.Helper()

	ops := make(map[replication.ShardReplicationOp]replication.ShardReplicationOpStatus, len(specs))
	for _, spec := range specs {
		spec = spec.withDefaults()
		op := replication.NewShardReplicationOp(spec.id, spec.srcNode, spec.tgtNode, spec.collection, spec.shard, spec.transferType)
		op.UUID = strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", spec.id))
		op.StartTimeUnixMs = spec.opCreatedUnixMs

		status := replication.NewShardReplicationStatus(spec.state)
		status.Current.StartTimeUnixMs = spec.stateStartMs
		status.ShouldCancel = spec.shouldCancel
		status.ShouldDelete = spec.shouldDelete

		ops[op] = status
	}

	// The outer key must be "Ops", matching the unexported snapshot struct's field.
	b, err := json.Marshal(struct {
		Ops map[replication.ShardReplicationOp]replication.ShardReplicationOpStatus
	}{Ops: ops})
	require.NoError(t, err)
	require.NoError(t, fsm.Restore(b))
}

func selectedIDs(ops []replication.StaleOp) []uint64 {
	ids := make([]uint64, 0, len(ops))
	for _, op := range ops {
		ids = append(ids, op.ID)
	}
	return ids
}

func TestSelectStaleOps(t *testing.T) {
	const cutoff int64 = 1_000_000

	cases := []struct {
		name             string
		specs            []seedSpec
		includeCancelled bool
		limit            int
		cutoff           int64
		wantIDs          []uint64
		wantFlagged      int
	}{
		{
			name:    "just older than cutoff is selected",
			specs:   []seedSpec{{id: 1, state: api.READY, stateStartMs: cutoff - 1}},
			limit:   10,
			wantIDs: []uint64{1},
		},
		{
			name:    "exactly at cutoff is not selected",
			specs:   []seedSpec{{id: 1, state: api.READY, stateStartMs: cutoff}},
			limit:   10,
			wantIDs: []uint64{},
		},
		{
			name:    "just younger than cutoff is not selected",
			specs:   []seedSpec{{id: 1, state: api.READY, stateStartMs: cutoff + 1}},
			limit:   10,
			wantIDs: []uint64{},
		},
		{
			name:    "zero timestamp is infinitely old",
			specs:   []seedSpec{{id: 1, state: api.READY, stateStartMs: 0}},
			limit:   10,
			wantIDs: []uint64{1},
		},
		{
			// A very large max-age makes the cutoff negative; the pre-upgrade ops
			// carrying no timestamp must stay eligible even then.
			name:    "zero timestamp is selected under a negative cutoff",
			specs:   []seedSpec{{id: 1, state: api.READY, stateStartMs: 0}},
			cutoff:  -5_000_000,
			limit:   10,
			wantIDs: []uint64{1},
		},
		{
			name: "non-terminal states are never selected however ancient",
			specs: []seedSpec{
				{id: 1, state: api.REGISTERED, stateStartMs: 1},
				{id: 2, state: api.HYDRATING, stateStartMs: 1},
				{id: 3, state: api.FINALIZING, stateStartMs: 1},
				{id: 4, state: api.INTEGRATING, stateStartMs: 1},
				{id: 5, state: api.DEHYDRATING, stateStartMs: 1},
			},
			limit:   10,
			wantIDs: []uint64{},
		},
		{
			name:    "ancient CANCELLED is skipped by default",
			specs:   []seedSpec{{id: 1, state: api.CANCELLED, stateStartMs: 1}},
			limit:   10,
			wantIDs: []uint64{},
		},
		{
			name:             "ancient CANCELLED is selected when opted in",
			specs:            []seedSpec{{id: 1, state: api.CANCELLED, stateStartMs: 1}},
			includeCancelled: true,
			limit:            10,
			wantIDs:          []uint64{1},
		},
		{
			name:        "ancient READY carrying ShouldDelete is excluded and counted",
			specs:       []seedSpec{{id: 1, state: api.READY, stateStartMs: 1, shouldCancel: true, shouldDelete: true}},
			limit:       10,
			wantIDs:     []uint64{},
			wantFlagged: 1,
		},
		{
			name:        "ancient READY carrying only ShouldCancel is excluded and counted",
			specs:       []seedSpec{{id: 1, state: api.READY, stateStartMs: 1, shouldCancel: true}},
			limit:       10,
			wantIDs:     []uint64{},
			wantFlagged: 1,
		},
		{
			name:             "ancient flagged CANCELLED is excluded and counted when opted in",
			specs:            []seedSpec{{id: 1, state: api.CANCELLED, stateStartMs: 1, shouldDelete: true}},
			includeCancelled: true,
			limit:            10,
			wantIDs:          []uint64{},
			wantFlagged:      1,
		},
		{
			name:        "a flagged op that fails the age test is not counted",
			specs:       []seedSpec{{id: 1, state: api.READY, stateStartMs: cutoff + 1, shouldDelete: true}},
			limit:       10,
			wantIDs:     []uint64{},
			wantFlagged: 0,
		},
		{
			name: "limit takes the lowest ids and does not shrink flaggedSkipped",
			specs: []seedSpec{
				{id: 10, state: api.READY, stateStartMs: 1},
				{id: 20, state: api.READY, stateStartMs: 1},
				{id: 30, state: api.READY, stateStartMs: 1},
				{id: 40, state: api.READY, stateStartMs: 1},
				{id: 50, state: api.READY, stateStartMs: 1, shouldDelete: true},
				{id: 60, state: api.READY, stateStartMs: 1, shouldDelete: true},
			},
			limit:       2,
			wantIDs:     []uint64{10, 20},
			wantFlagged: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedViaRestore(t, fsm, tc.specs...)

			effectiveCutoff := cutoff
			if tc.cutoff != 0 {
				effectiveCutoff = tc.cutoff
			}

			ops, flagged := fsm.SelectStaleOps(effectiveCutoff, tc.includeCancelled, tc.limit)

			require.Equal(t, tc.wantIDs, selectedIDs(ops))
			require.Equal(t, tc.wantFlagged, flagged)
			for _, op := range ops {
				require.Contains(t, []api.ShardReplicationState{api.READY, api.CANCELLED}, op.State,
					"the eligible state must travel with the id")
			}
		})
	}
}

// Sweeping the selected set must not move any of the three gate predicates the
// rest of the database reads. That is the property the design rests on.
func TestSelectStaleOps_ExcludesOpsThatArmGatePredicates(t *testing.T) {
	const (
		coll   = "TestClass"
		shard  = "shard1"
		target = "target"
	)

	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedViaRestore(t, fsm,
		seedSpec{id: 1, state: api.READY, stateStartMs: 1, collection: coll, shard: shard, tgtNode: target},
		seedSpec{id: 2, state: api.READY, stateStartMs: 1, collection: coll, shard: shard, tgtNode: target, shouldCancel: true, shouldDelete: true},
		seedSpec{id: 3, state: api.HYDRATING, stateStartMs: 1, collection: coll, shard: shard, tgtNode: target},
	)

	ops, flagged := fsm.SelectStaleOps(1_000_000, false, 100)
	require.Equal(t, []uint64{1}, selectedIDs(ops), "only the clean terminal op is eligible")
	require.Equal(t, 1, flagged)

	before := []bool{
		fsm.HasActiveReplicationForCollection(coll),
		fsm.HasActiveReplicationForShard(coll, shard),
		fsm.HasActiveTargetReplicationForShard(coll, shard, target),
	}

	require.NoError(t, fsm.ForceDeleteByIds(selectedIDs(ops)))

	after := []bool{
		fsm.HasActiveReplicationForCollection(coll),
		fsm.HasActiveReplicationForShard(coll, shard),
		fsm.HasActiveTargetReplicationForShard(coll, shard, target),
	}
	require.Equal(t, before, after, "sweeping the selected set must not move any gate predicate")
}

// The same property with the non-terminal op removed. That op arms every gate on
// its own, so the sibling test would read true either way. Here only the flagged
// op holds the gates up, so a selection that swept it flips them.
func TestSelectStaleOps_FlaggedOpAloneKeepsGatesArmed(t *testing.T) {
	const (
		coll   = "TestClass"
		shard  = "shard1"
		target = "target"
	)

	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedViaRestore(t, fsm,
		seedSpec{id: 1, state: api.READY, stateStartMs: 1, collection: coll, shard: shard, tgtNode: target},
		seedSpec{id: 2, state: api.READY, stateStartMs: 1, collection: coll, shard: shard, tgtNode: target, shouldDelete: true},
	)

	// ShouldDelete on a terminal op means the consumer still owes it teardown,
	// so it counts as active replication for the collection and shard gates.
	require.True(t, fsm.HasActiveReplicationForCollection(coll))
	require.True(t, fsm.HasActiveReplicationForShard(coll, shard))

	ops, flagged := fsm.SelectStaleOps(1_000_000, false, 100)
	require.Equal(t, []uint64{1}, selectedIDs(ops), "the flagged op must not be selected")
	require.Equal(t, 1, flagged)

	require.NoError(t, fsm.ForceDeleteByIds(selectedIDs(ops)))

	require.True(t, fsm.HasActiveReplicationForCollection(coll),
		"the flagged op alone must keep the collection gate armed")
	require.True(t, fsm.HasActiveReplicationForShard(coll, shard),
		"the flagged op alone must keep the shard gate armed")

	// Removing the flagged op too disarms them, proving the assertions above are
	// about that op and not about an FSM that never had anything active.
	require.NoError(t, fsm.ForceDeleteByIds([]uint64{2}))
	require.False(t, fsm.HasActiveReplicationForCollection(coll))
	require.False(t, fsm.HasActiveReplicationForShard(coll, shard))
}

func TestForceDeleteByIds_SkipsUnknownIds(t *testing.T) {
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedViaRestore(t, fsm,
		seedSpec{id: 1, state: api.READY},
		seedSpec{id: 2, state: api.READY},
		seedSpec{id: 3, state: api.READY},
	)

	require.NoError(t, fsm.ForceDeleteByIds([]uint64{1, 999999, 3}))

	_, ok := fsm.GetOpById(1)
	require.False(t, ok)
	_, ok = fsm.GetOpById(3)
	require.False(t, ok)
	_, ok = fsm.GetOpById(2)
	require.True(t, ok, "the unlisted op must be untouched")
}

// The apply must be a pure function of its id-list payload. This goes red the
// moment anyone reintroduces a cutoff, or any clock read, into the apply path.
func TestForceDeleteByIds_IsDeterministicAcrossDivergentTimestamps(t *testing.T) {
	build := func(offsetMs int64) *replication.ShardReplicationFSM {
		fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
		specs := make([]seedSpec, 0, 6)
		for id := uint64(1); id <= 6; id++ {
			specs = append(specs, seedSpec{
				id:              id,
				state:           api.READY,
				stateStartMs:    int64(id)*1000 + offsetMs,
				opCreatedUnixMs: int64(id)*1000 + offsetMs,
			})
		}
		seedViaRestore(t, fsm, specs...)
		return fsm
	}

	// The two FSMs hold the same ops with timestamps hours apart, as two nodes do
	// after each stamps its own time.Now() at apply.
	nodeA := build(0)
	nodeB := build(6 * 60 * 60 * 1000)

	ids := []uint64{2, 4, 6}
	require.NoError(t, nodeA.ForceDeleteByIds(ids))
	require.NoError(t, nodeB.ForceDeleteByIds(ids))

	remaining := func(fsm *replication.ShardReplicationFSM) []uint64 {
		var out []uint64
		for id := uint64(1); id <= 6; id++ {
			if _, ok := fsm.GetOpById(id); ok {
				out = append(out, id)
			}
		}
		return out
	}
	require.Equal(t, []uint64{1, 3, 5}, remaining(nodeA))
	require.Equal(t, remaining(nodeA), remaining(nodeB))
}

// Removing the last READY target op for a replica deletes its opsByTargetFQDN
// key, which unmasks the replica's source-side DEHYDRATING state and drops it
// from both replica sets. This characterises that routing change.
func TestForceDeleteByIds_UnmasksDehydratingSource(t *testing.T) {
	const (
		coll  = "Foo"
		shard = "s1"
	)

	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())

	// COPY A -> C, driven to READY before op2 is admitted.
	seedOpFull(t, fsm, 1, "A", "C", coll, shard, api.COPY)
	driveToState(t, fsm, 1, api.READY)

	// MOVE C -> D. Admission permits sourcing from a READY target.
	seedOpFull(t, fsm, 2, "C", "D", coll, shard, api.MOVE)
	driveToState(t, fsm, 2, api.DEHYDRATING)

	replicas := []string{"A", "C"}
	require.Equal(t, []string{"A", "C"}, fsm.FilterOneShardReplicasRead(coll, shard, replicas),
		"C is masked as a READY target while op1 is present")

	require.NoError(t, fsm.ForceDeleteByIds([]uint64{1}))

	require.Equal(t, []string{"A"}, fsm.FilterOneShardReplicasRead(coll, shard, replicas),
		"removing op1 unmasks C's DEHYDRATING source state")
}

// BenchmarkForceDeleteByIds documents the complexity change of the batched
// removal. It asserts nothing.
func BenchmarkForceDeleteByIds(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			specs := make([]seedSpec, 0, n)
			ids := make([]uint64, 0, n)
			for i := 1; i <= n; i++ {
				specs = append(specs, seedSpec{id: uint64(i), state: api.READY, collection: "Bench"})
				ids = append(ids, uint64(i))
			}

			for b.Loop() {
				b.StopTimer()
				fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
				seedViaRestore(b, fsm, specs...)
				b.StartTimer()

				if err := fsm.ForceDeleteByIds(ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
