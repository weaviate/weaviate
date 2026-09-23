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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/replication"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

var errTaskManagerUnreachable = errors.New("leader not found")

func unreachableActivityBuilder() (ShardReindexActivityLookup, error) {
	return nil, errTaskManagerUnreachable
}

func anyLiveReindex(t *testing.T, db *DB, collection, shardName string) bool {
	t.Helper()
	live, err := db.AnyLiveReindexForShard(collection, shardName)
	require.NoError(t, err)
	return live
}

// makeCleanupBuilder builds a CleanupInProgressLookupBuilder reporting a fixed
// set of (collection, shard) pairs as mid-cleanup.
func makeCleanupBuilder(inProgress map[[2]string]bool) CleanupInProgressLookupBuilder {
	return func() CleanupInProgressLookup {
		return func(collection, shard string) bool {
			return inProgress[[2]string{collection, shard}]
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
	assert.True(t, anyLiveReindex(t, db, "MyClass", "shard1"),
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
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
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
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
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
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"gate must scope by shard, not just by collection")
}

// TestAnyLiveReindexForShard_BuilderUnwired pins that an unwired
// lookup defaults to "no live reindex" — production gates HTTP serving
// on bootstrap completion so the unwired window is unreachable by
// external traffic, and the prior refuse-by-default broke every
// module-test fixture that spins up Weaviate without going through
// the post-bootstrap install path. A one-time WARN fires to surface
// the unwired path if it ever shows up in production logs.
func TestAnyLiveReindexForShard_BuilderUnwired(t *testing.T) {
	db := &DB{}
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"unwired gate must allow (with WARN); production gates HTTP on bootstrap")
}

// TestAnyLiveReindexForShard_BuilderReturnsNil pins the same fail-open
// when the installed builder returns a nil closure (defensive against
// a misconfigured wiring).
func TestAnyLiveReindexForShard_BuilderReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return nil, nil
	})
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"nil lookup must allow (same path as unwired)")
}

// TestAnyLiveReindexForShard_CleanupInProgress pins the OR-d cleanup branch:
// once the DTM task goes terminal (activity lookup false) but
// autoCleanupAfterTerminal is still draining sidecars, the gate must still
// refuse — a backup mid-cleanup would capture torn __reindex/__ingest state.
func TestAnyLiveReindexForShard_CleanupInProgress(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{})) // no live task
	db.SetReindexCleanupInProgressLookup(makeCleanupBuilder(map[[2]string]bool{
		{"MyClass", "shard1"}: true,
	}))
	assert.True(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"gate must refuse while terminal-task cleanup is still draining sidecars")
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard2"),
		"cleanup branch must scope by shard")
}

// TestAnyLiveReindexForShard_CleanupBuilderUnwired pins that with no cleanup
// builder installed the gate keeps activity-only semantics — older wiring paths
// and fixtures install only the activity lookup.
func TestAnyLiveReindexForShard_CleanupBuilderUnwired(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"no cleanup builder → activity-only semantics → allow")
}

// TestAnyLiveReindexForShard_CleanupReturnsNil pins fail-open when the cleanup
// builder returns a nil closure (defensive against a misconfigured wiring).
func TestAnyLiveReindexForShard_CleanupReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup { return nil })
	assert.False(t, anyLiveReindex(t, db, "MyClass", "shard1"),
		"nil cleanup closure → allow (same as unwired)")
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
	assert.Contains(t, err.Error(), "/index/<indexType>/cancel", "error must include the GA cancel-route remediation hint")
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

// Each way refuseIfReindexInFlight can fail to look must still refuse and count as a movement error.
func TestRefuseIfReindexInFlight_CannotCheck(t *testing.T) {
	for _, tc := range []struct {
		name        string
		index       func() *Index
		wantContain string
	}{
		{
			name:        "no database back-reference",
			index:       func() *Index { return &Index{Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}} },
			wantContain: "no database back-reference",
		},
		{
			name: "task manager unreachable",
			index: func() *Index {
				db := &DB{}
				db.SetShardReindexActivityLookup(unreachableActivityBuilder)
				return &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}}
			},
			wantContain: errTaskManagerUnreachable.Error(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.index().refuseIfReindexInFlight("ABC123")
			require.ErrorIs(t, err, ErrReindexGateUnavailable)
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
				"the backup path answers the same whether the gate saw a task or could not look")
			require.Contains(t, err.Error(), "ABC123")
			require.Contains(t, err.Error(), "JourneyClass")
			require.Contains(t, err.Error(), tc.wantContain)
		})
	}
}

// TestReindexInFlightError_DTMHit pins the wording variant used when
// DTM reports a live task.
//
// The gate cannot see the task's status, so the cancel it names has to
// carry the condition under which the API accepts one. Without it the
// message points an operator whose task carries a status this build
// cannot classify at a cancel that answers 409 on every node.
func TestReindexInFlightError_DTMHit(t *testing.T) {
	err := reindexInFlightError("MyClass", "shard1")
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "shard1")
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "active runtime-reindex task in DTM")
	require.Contains(t, err.Error(), "reaches a terminal state")
	// A STARTED task with no working unit and no progress reads as
	// "pending" on the GET while the gate is already refusing, so naming
	// only "indexing" leaves an operator watching a pill that never
	// showed up.
	require.Contains(t, err.Error(), `status="pending"`)
	require.Contains(t, err.Error(), `status="indexing"`)
	require.Contains(t, err.Error(), "accepted only while the task is STARTED")
	require.Contains(t, err.Error(), "409")
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

// A movement that waited on an unreachable task manager would retry for the whole outage and report no error.
func TestReplicaSnapshotDefersOnlyForALiveReindex(t *testing.T) {
	for _, tc := range []struct {
		name      string
		builder   ShardReindexActivityLookupBuilder
		wantDefer bool
	}{
		{
			name:      "a live reindex task",
			builder:   makeActivityBuilder(map[[2]string]bool{{"TestClass", "shard1"}: true}),
			wantDefer: true,
		},
		{
			name:      "the task manager is unreachable",
			builder:   unreachableActivityBuilder,
			wantDefer: false,
		},
		{
			// A wrapped status here would let IsReversibleRefusal match its message and make the movement wait.
			name: "the task manager answers with the text the wait path matches",
			builder: func() (ShardReindexActivityLookup, error) {
				return nil, status.Error(codes.FailedPrecondition, enterrors.ErrShardBusyStructuralOp.Error())
			},
			wantDefer: false,
		},
	} {
		// IncomingCreateReplicaSnapshot defers on both the hardlink snapshot and the halt-for-duration fallback.
		for _, mode := range []struct {
			name         string
			withHardlink bool
		}{{name: "hardlink mode", withHardlink: true}, {name: "fallback halt-for-duration mode"}} {
			t.Run(tc.name+", "+mode.name, func(t *testing.T) {
				if !mode.withHardlink {
					t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
				}
				index, _ := newSharedHaltTestShard(t)
				putSharedHaltObject(t, index, strfmt.UUID("2b1d4bd0-3f52-4f0f-9f75-b0cb2e29b3a3"), 0)
				index.db.SetShardReindexActivityLookup(tc.builder)

				_, err := index.IncomingCreateReplicaSnapshot(context.Background(), "shard1", "op-reindex")
				require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
					"the reindex sentinel must survive so the backup path keeps its own response")
				require.Equal(t, tc.wantDefer, replication.IsReversibleRefusal(err),
					"only a refusal that names a live task may make the movement wait")
				require.Equal(t, tc.wantDefer, errors.Is(err, enterrors.ErrShardBusyStructuralOp),
					"the shard-busy sentinel is what writes the text a remote caller matches on")
			})
		}
	}
}
