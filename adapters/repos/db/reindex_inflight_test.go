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
	"strings"
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
	return func() ShardReindexActivityLookup {
		return func(collection, shardName string) bool {
			return live[[2]string{collection, shardName}]
		}
	}
}

// TestAnyLiveReindexForShard_LiveTask pins that a DTM lookup reporting
// a live task for the (collection, shard) tuple causes the gate to
// refuse.
func TestAnyLiveReindexForShard_LiveTask(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"MyClass", "shard1"}: true,
	}))
	assert.True(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must refuse when DTM reports a live task on the tuple")
}

// TestAnyLiveReindexForShard_TerminalTask pins that a lookup whose
// snapshot contains only terminal-status tasks (none reported as live)
// lets the gate allow the backup.
func TestAnyLiveReindexForShard_TerminalTask(t *testing.T) {
	db := &DB{}
	// Builder reports no live tasks at all — equivalent to a snapshot
	// containing only Finished/Cancelled/Failed tasks after the
	// configure_api filter.
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	assert.False(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must allow when no live task targets the tuple")
}

// TestAnyLiveReindexForShard_DifferentCollection pins that a live task
// in another collection does not block a backup of the queried
// collection.
func TestAnyLiveReindexForShard_DifferentCollection(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"OtherClass", "shard1"}: true,
	}))
	assert.False(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must scope by collection")
}

// TestAnyLiveReindexForShard_DifferentShard pins that a live task on
// the right collection but a different shard does not block a backup
// of the queried shard.
func TestAnyLiveReindexForShard_DifferentShard(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"MyClass", "shard2"}: true,
	}))
	assert.False(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must scope by shard, not just by collection")
}

// TestAnyLiveReindexForShard_BuilderUnwired pins the conservative
// stance: until the lookup builder is installed (boot window before
// MakeAppState's scheduler-Start goroutine runs), the gate must refuse
// so a backup landing pre-wire does not race a real reindex.
func TestAnyLiveReindexForShard_BuilderUnwired(t *testing.T) {
	db := &DB{}
	assert.True(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must refuse during pre-wire startup window")
}

// TestAnyLiveReindexForShard_BuilderReturnsNil pins the same
// conservative stance when the installed builder returns a nil
// closure (defensive against a misconfigured wiring).
func TestAnyLiveReindexForShard_BuilderReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return nil
	})
	assert.True(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"gate must refuse when builder returns a nil lookup")
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
