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
	"slices"
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

// countingActivityBuilder counts snapshot builds and probed shards.
// Production meters the same count as
// schema_reads_leader_seconds_count{type="TYPE_DISTRIBUTED_TASK_LIST"}.
// Mutex-guarded: the descriptor pass runs one goroutine per shard.
type countingActivityBuilder struct {
	snapshots ShardReindexActivityLookupBuilder

	mu     sync.Mutex
	builds int
	probed [][2]string
}

func (c *countingActivityBuilder) install(db *DB) {
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		c.mu.Lock()
		c.builds++
		c.mu.Unlock()
		lookup, err := c.snapshots()
		if err != nil {
			return nil, err
		}
		return func(collection, shardName string) bool {
			c.mu.Lock()
			c.probed = append(c.probed, [2]string{collection, shardName})
			c.mu.Unlock()
			return lookup(collection, shardName)
		}, nil
	})
}

func (c *countingActivityBuilder) stats() (builds int, probed [][2]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builds, slices.Clone(c.probed)
}

// TestReindexGate_LiveTask pins that a DTM lookup reporting
// a live task for the (collection, shard) tuple causes the gate to
// refuse.
func TestReindexGate_LiveTask(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"MyClass", "shard1"}: true,
	}))
	assert.True(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"gate must refuse when DTM reports a live task on the tuple")
}

// TestReindexGate_TerminalTask pins that a lookup whose
// snapshot contains only terminal-status tasks (none reported as live)
// lets the gate allow the backup.
func TestReindexGate_TerminalTask(t *testing.T) {
	db := &DB{}
	// Builder reports no live tasks at all — equivalent to a snapshot
	// containing only Finished/Cancelled/Failed tasks after the
	// configure_api filter.
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	assert.False(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"gate must allow when no live task targets the tuple")
}

// TestReindexGate_DifferentCollection pins that a live task
// in another collection does not block a backup of the queried
// collection.
func TestReindexGate_DifferentCollection(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"OtherClass", "shard1"}: true,
	}))
	assert.False(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"gate must scope by collection")
}

// TestReindexGate_DifferentShard pins that a live task on
// the right collection but a different shard does not block a backup
// of the queried shard.
func TestReindexGate_DifferentShard(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"MyClass", "shard2"}: true,
	}))
	assert.False(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"gate must scope by shard, not just by collection")
}

// TestReindexGate_BuilderUnwired pins that an unwired lookup
// defaults to allow, with a one-time WARN (see
// [DB.SetShardReindexActivityLookup]).
func TestReindexGate_BuilderUnwired(t *testing.T) {
	db := &DB{}
	assert.False(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"unwired gate must allow (with WARN); production gates HTTP on bootstrap")
}

// TestReindexGate_BuilderReturnsNil pins the same fail-open
// when the installed builder returns a nil closure (defensive against
// a misconfigured wiring).
func TestReindexGate_BuilderReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return nil, nil
	})
	assert.False(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"nil lookup must allow (same path as unwired)")
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

	err := idx.refuseIfReindexInFlight("ABC123")
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
	require.NoError(t, idx.refuseIfReindexInFlight("ABC123"))
}

// TestRefuseIfReindexInFlight_DbNilIsConservative pins that an Index
// without its DB back-reference refuses rather than letting a backup
// proceed unchecked.
func TestRefuseIfReindexInFlight_DbNilIsConservative(t *testing.T) {
	idx := &Index{Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}}
	err := idx.refuseIfReindexInFlight("ABC123")
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.True(t, strings.Contains(err.Error(), "startup window"))
}

// TestReindexInFlightError_PreWire pins the wording variant used
// during the pre-wire startup window.
func TestReindexInFlightError_PreWire(t *testing.T) {
	err := reindexInFlightError("MyClass", "shard1", true)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "shard1")
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "startup window")
}

// TestReindexInFlightError_DTMHit pins the wording variant used when
// DTM reports a live task.
func TestReindexInFlightError_DTMHit(t *testing.T) {
	err := reindexInFlightError("MyClass", "shard1", false)
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

	err := shd.HaltForTransfer(ctx, false, 100*time.Millisecond)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), shd.Name())

	// Flip the lookup so the next call allows the halt; this also
	// proves the gate consults a fresh snapshot rather than a cached
	// boolean.
	idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))

	require.NoError(t, shd.HaltForTransfer(ctx, false, 100*time.Millisecond))
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

	require.NoError(t, shd.HaltForTransfer(ctx, true, 100*time.Millisecond))
	require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
}

// TestRefuseIfReindexInFlight_UnreachableLeaderStatesTheCause pins that
// the single-shard caller (replica movement, and each shard of a backup
// execution pass) reports the leader failure as itself, rather than
// borrowing the live-reindex wording.
func TestRefuseIfReindexInFlight_UnreachableLeaderStatesTheCause(t *testing.T) {
	leaderErr := errors.New("list DTM tasks: leader not found")
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return nil, leaderErr
	})
	idx := &Index{
		db:     db,
		Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")},
	}

	err := idx.refuseIfReindexInFlight("ABC123")
	require.Error(t, err)
	assert.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
		"unknown state stays fail-closed")
	assert.Contains(t, err.Error(), "cluster leader could not be reached")
	assert.Contains(t, err.Error(), leaderErr.Error())
	assert.NotContains(t, err.Error(), "active runtime-reindex task in DTM")
	assert.NotContains(t, err.Error(), "cancel")
}

// TestReindexGate_UnreachableLeaderIsBlocked pins that the
// boolean form stays fail-closed when cluster state cannot be read.
func TestReindexGate_UnreachableLeaderIsBlocked(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return nil, errors.New("leader not found")
	})
	assert.True(t, newReindexGate(db).anyLiveReindexForShard("MyClass", "shard1"),
		"unknown reindex state must block, not allow")
}

// TestReindexStateUnknownError_ReadsAsItsOwnCause pins that the refusal
// for an unreachable leader states the leader failure from its first
// word. Wrapping the sentinel with %w rendered "backup blocked:
// runtime-reindex in flight on this shard" first, so the opening of the
// response asserted the one thing this refusal exists to deny.
func TestReindexStateUnknownError_ReadsAsItsOwnCause(t *testing.T) {
	cause := errors.New("list DTM tasks: leader not found")
	err := reindexStateUnknownError(cause)

	sentinelText := entitiesbackup.ErrBackupBlockedByInFlightReindex.Error()
	require.False(t, strings.HasPrefix(err.Error(), sentinelText),
		"the refusal must not open with a claim that a reindex is in flight")
	require.NotContains(t, err.Error(), sentinelText,
		"and must not make that claim anywhere else either")
	require.True(t, strings.HasPrefix(err.Error(),
		"backup blocked: the cluster leader could not be reached"),
		"the real cause comes first")

	// Matching still works, including across the canCommit RPC, where
	// classifyCanCommitErr decides the response kind with errors.Is.
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
		"the sentinel must still match")
	require.True(t, errors.Is(err, cause), "the cause must still match")
}

// TestReindexInFlightError_GenuineRefusalIsUnchanged pins the genuine
// refusal byte for byte: it is parsed downstream, and the unknown-state
// rewording must not touch it.
func TestReindexInFlightError_GenuineRefusalIsUnchanged(t *testing.T) {
	want := `backup blocked: runtime-reindex in flight on this shard: shard "shard1" (collection "MyClass") has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status="ready") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {"<indexType>":{"cancel":true}}`
	require.Equal(t, want, reindexInFlightError("MyClass", "shard1", false).Error())

	wantPreWire := `backup blocked: runtime-reindex in flight on this shard: shard "shard1" (collection "MyClass"): backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping`
	require.Equal(t, wantPreWire, reindexInFlightError("MyClass", "shard1", true).Error())
}
