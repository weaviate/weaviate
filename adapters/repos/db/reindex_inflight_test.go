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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/schema"
)

// makeActivityBuilder builds a ShardReindexActivityLookupBuilder that
// reports a fixed set of (collection, shard) pairs as live.
func makeActivityBuilder(live map[[2]string]bool) ShardReindexActivityLookupBuilder {
	return func() (ShardReindexActivityLookup, error) {
		return func(collection, shardName string) bool {
			return live[[2]string{collection, shardName}]
		}, nil
	}
}

// TestReindexGate_ActivityArmDecisionTree pins how the gate reads a DTM
// snapshot: the tuple must match on both collection and shard, and a builder
// that is unwired or yields no lookup allows rather than refuses.
//
// Fail-open on an unwired builder is deliberate — see
// [DB.SetShardReindexActivityLookup].
func TestReindexGate_ActivityArmDecisionTree(t *testing.T) {
	tests := []struct {
		name       string
		builder    ShardReindexActivityLookupBuilder
		wantRefuse bool
	}{
		{
			name:       "live task on the queried tuple",
			builder:    makeActivityBuilder(map[[2]string]bool{{"MyClass", "shard1"}: true}),
			wantRefuse: true,
		},
		{
			name:    "no live task anywhere",
			builder: makeActivityBuilder(map[[2]string]bool{}),
		},
		{
			name:    "live task in another collection",
			builder: makeActivityBuilder(map[[2]string]bool{{"OtherClass", "shard1"}: true}),
		},
		{
			name:    "live task on another shard of the same collection",
			builder: makeActivityBuilder(map[[2]string]bool{{"MyClass", "shard2"}: true}),
		},
		{
			name:    "builder never installed",
			builder: nil,
		},
		{
			name: "builder yields no lookup",
			builder: func() (ShardReindexActivityLookup, error) {
				return nil, nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{}
			if tc.builder != nil {
				db.SetShardReindexActivityLookup(tc.builder)
			}
			assert.Equal(t, tc.wantRefuse,
				db.newReindexGate().anyLiveReindexForShard("MyClass", "shard1"))
		})
	}
}

// TestReindexGate_CleanupArmRefuses pins the second arm, which no other test
// reaches: a terminal task still tearing its sidecars down must refuse, and
// must say so rather than reusing the live-task remediation.
func TestReindexGate_CleanupArmRefuses(t *testing.T) {
	tests := []struct {
		name            string
		cleanupInFlight bool
		wantRefuse      bool
	}{
		{name: "cleanup still draining", cleanupInFlight: true, wantRefuse: true},
		{name: "cleanup finished", cleanupInFlight: false, wantRefuse: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{}
			// Activity arm allows, so only the cleanup arm can refuse here.
			db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
			db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
				return func(collection, shard string) bool {
					return tc.cleanupInFlight && collection == "MyClass" && shard == "shard1"
				}
			})
			idx := &Index{db: db, Config: IndexConfig{ClassName: "MyClass"}}

			gate := idx.newReindexGate()
			require.Equal(t, tc.wantRefuse, gate.anyLiveReindexForShard("MyClass", "shard1"))
			require.False(t, gate.anyLiveReindexForShard("MyClass", "other-shard"),
				"the cleanup arm is probed per shard, so an untouched shard stays allowed")

			err := idx.refuseIfReindexInFlight("shard1", gate)
			if !tc.wantRefuse {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
			require.Contains(t, err.Error(), "__reindex / __ingest",
				"cleanup refusals must name the sidecar teardown, not a migration to poll")
			require.NotContains(t, err.Error(), "retry after the migration finishes")
		})
	}
}

// TestReindexGate_DTMUnreachableIsItsOwnRefusal pins that a failed snapshot is
// reported as "could not find out", not as "a reindex is running": the
// live-task remediation tells the operator to poll a migration that does not
// exist.
func TestReindexGate_DTMUnreachableIsItsOwnRefusal(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return nil, errors.New("list distributed tasks: leader unreachable")
	})
	idx := &Index{db: db, Config: IndexConfig{ClassName: "MyClass"}}

	err := idx.refuseIfReindexInFlight("shard1", idx.newReindexGate())
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
	require.Contains(t, err.Error(), "cannot read reindex state")
	require.NotContains(t, err.Error(), "retry after the migration finishes",
		"there is no migration to wait for when the query itself failed")
}

// TestReindexGate_BuildersAreReadAtResolveNotConstruction pins that a gate
// built between the two setter calls in configure_api.go still picks the
// cleanup arm up, instead of silently dropping it for the whole pass.
func TestReindexGate_BuildersAreReadAtResolveNotConstruction(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))

	gate := db.newReindexGate()

	// Installed after construction, before first use — the startup window.
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) bool { return true }
	})

	assert.True(t, gate.anyLiveReindexForShard("MyClass", "shard1"),
		"the cleanup builder installed before the first resolve must be used")
}

// TestRefuseIfReindexInFlight_ErrorShape pins that the error wraps the
// sentinel, names the collection and shard, and surfaces the operator
// remediation hint.
func TestRefuseIfReindexInFlight_ErrorShape(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"JourneyClass", "ABC123"}: true,
	}))
	idx := &Index{
		db:     db,
		Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")},
	}

	err := idx.refuseIfReindexInFlight("ABC123", idx.newReindexGate())
	require.Error(t, err)
	assert.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
		"error must wrap the sentinel so REST handlers can map via errors.Is")
	assert.Contains(t, err.Error(), "ABC123", "error must name the shard")
	assert.Contains(t, err.Error(), "JourneyClass", "error must name the collection")
	assert.Contains(t, err.Error(), "indexes/", "error must include the remediation URL hint")
}

// TestRefuseIfReindexInFlight_AllowsWhenNoLiveTask pins the happy
// path: no live task means no rejection.
func TestRefuseIfReindexInFlight_AllowsWhenNoLiveTask(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	idx := &Index{
		db:     db,
		Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")},
	}
	require.NoError(t, idx.refuseIfReindexInFlight("ABC123", idx.newReindexGate()))
}

// TestRefuseIfReindexInFlight_DbNilIsConservative pins that an Index
// without its DB back-reference refuses rather than letting a backup
// proceed unchecked.
func TestRefuseIfReindexInFlight_DbNilIsConservative(t *testing.T) {
	idx := &Index{Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}}
	err := idx.refuseIfReindexInFlight("ABC123", idx.newReindexGate())
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.True(t, strings.Contains(err.Error(), "startup window"))
}

// TestReindexInFlightError_PreWire pins the wording variant used
// during the pre-wire startup window.
func TestReindexInFlightError_PreWire(t *testing.T) {
	err := reindexInFlightError("MyClass", "shard1", reindexRefusalPreWire)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "shard1")
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "startup window")
}

// TestReindexInFlightError_DTMHit pins the wording variant used when
// DTM reports a live task.
func TestReindexInFlightError_DTMHit(t *testing.T) {
	err := reindexInFlightError("MyClass", "shard1", reindexRefusalLiveTask)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "shard1")
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "active runtime-reindex task in DTM")
	require.Contains(t, err.Error(), "retry after the migration finishes")
}

// TestShard_HaltForTransfer_RefusesWhenReindexInFlight asserts that
// the shard-level halt-for-backup path delegates the gate decision to
// the same DTM-backed lookup as the inactive-shard path.
func TestShard_HaltForTransfer_RefusesWhenReindexInFlight(t *testing.T) {
	ctx := testCtx()
	className := "ShardHaltRefuseClass"
	shd, idx := testShard(t, ctx, className)

	// Install the activity lookup so the gate sees a live task.
	require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
	idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{className, shd.Name()}: true,
	}))

	err := shd.HaltForTransfer(ctx, false, 100*time.Millisecond, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), shd.Name())

	// Flip the lookup so the next call allows the halt; this also
	// proves the gate consults a fresh snapshot rather than a cached
	// boolean.
	idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))

	require.NoError(t, shd.HaltForTransfer(ctx, false, 100*time.Millisecond, nil))
	require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
}

// TestShard_HaltForTransfer_OffloadIgnoresInFlightReindex pins that
// the refusal is scoped to backup callers; offload (offloading=true)
// must pass through.
func TestShard_HaltForTransfer_OffloadIgnoresInFlightReindex(t *testing.T) {
	ctx := testCtx()
	className := "ShardHaltOffloadClass"
	shd, idx := testShard(t, ctx, className)

	require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
	idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{className, shd.Name()}: true,
	}))

	require.NoError(t, shd.HaltForTransfer(ctx, true, 100*time.Millisecond, nil))
	require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
}

// TestReindexGate_FormattingIsRaceFreeAgainstResolve pins that formatting a
// gate does not read what resolve writes.
//
// The mockery-generated ShardLike mock hands the gate to testify, which
// formats every argument with %v on every call, and the transfer loops share
// one gate across goroutines. Without [reindexGate.String] this is a data race
// reported inside fmt and reflect frames that name neither the gate nor the
// mock.
func TestReindexGate_FormattingIsRaceFreeAgainstResolve(t *testing.T) {
	const formatters = 8

	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) bool { return false }
	})
	gate := db.newReindexGate()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for f := 0; f < formatters; f++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// The exact shape testify's Arguments.Diff uses.
			require.Equal(t, "(*db.reindexGate=reindexGate)",
				fmt.Sprintf("(%[1]T=%[1]v)", gate))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		gate.anyLiveReindexForShard("MyClass", "shard1")
	}()

	close(start)
	wg.Wait()
}
