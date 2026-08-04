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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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

// TestAnyLiveReindexForShard_BuilderUnwired pins that an unwired
// lookup defaults to "no live reindex" — production gates HTTP serving
// on bootstrap completion so the unwired window is unreachable by
// external traffic, and the prior refuse-by-default broke every
// module-test fixture that spins up Weaviate without going through
// the post-bootstrap install path. A one-time WARN fires to surface
// the unwired path if it ever shows up in production logs.
func TestAnyLiveReindexForShard_BuilderUnwired(t *testing.T) {
	db := &DB{}
	assert.False(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
		"unwired gate must allow (with WARN); production gates HTTP on bootstrap")
}

// TestAnyLiveReindexForShard_BuilderReturnsNil pins the same fail-open
// when the installed builder returns a nil closure (defensive against
// a misconfigured wiring).
func TestAnyLiveReindexForShard_BuilderReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return nil
	})
	assert.False(t, db.AnyLiveReindexForShard("MyClass", "shard1"),
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
	assert.Contains(t, err.Error(), "JourneyClass", "error must name the collection")
	assert.Contains(t, err.Error(), "indexes/", "error must include the remediation URL hint")
	assert.NotContains(t, err.Error(), "ABC123",
		"this text reaches an API response body; backing up grants nothing on shard ids")
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
	err := reindexInFlightError("MyClass", reindexBlockedPreWire)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "startup window")
}

// TestReindexInFlightError_DTMHit pins the wording variant used when
// DTM reports a live task.
func TestReindexInFlightError_DTMHit(t *testing.T) {
	err := reindexInFlightError("MyClass", reindexBlockedByLiveTask)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
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
	require.Contains(t, err.Error(), idx.Config.ClassName.String())
	require.NotContains(t, err.Error(), shd.Name(),
		"this path answers a backup caller too; shard ids stay in the log")

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

// Pins: shard ids and node names must not leak into the backup-refusal body,
// but must still reach the operator through the log.
func TestRefuseIfReindexInFlight_RedactsNodeAndShard(t *testing.T) {
	const (
		collection = "JourneyClass"
		shard      = "zmDMRo4olU4c"
		node       = "weaviate-0"
	)

	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger, localNodeName: node}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{collection, shard}: true,
	}))
	idx := &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName(collection)}}

	err := idx.refuseIfReindexInFlight(shard)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive so the coordinator still answers 422")

	body := err.Error()
	assert.Contains(t, body, collection, "the caller named this collection itself")
	for _, leaked := range []string{shard, node} {
		assert.NotContainsf(t, body, leaked, "the refusal body leaked %q", leaked)
	}

	var logged *logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "refused a backup") {
			logged = entry
		}
	}
	require.NotNil(t, logged, "the operator needs a log line naming what was refused")
	assert.Equal(t, shard, logged.Data["shard"])
	assert.Equal(t, node, logged.Data["node"])
	assert.Equal(t, collection, logged.Data["collection"])
}

// Pins: DB.Backupable must not reassemble node/shard names across per-shard
// refusals.
func TestBackupable_RefusalRedactsNodeAndShard(t *testing.T) {
	const (
		collection = "JourneyClass"
		shard      = "zmDMRo4olU4c"
		node       = "weaviate-0"
	)

	logger, _ := logrustest.NewNullLogger()
	shardState := &sharding.State{
		IndexID:  collection,
		Physical: map[string]sharding.Physical{shard: {Name: shard, BelongsToNodes: []string{node}}},
	}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(&models.Class{Class: collection}, shardState))
	})
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.On("NodeName").Return(node)

	db := &DB{logger: logger, localNodeName: node}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{collection, shard}: true,
	}))
	idx := &Index{
		db:           db,
		Config:       IndexConfig{ClassName: schema.ClassName(collection)},
		schemaReader: reader,
		getSchema:    getter,
	}
	db.indices = map[string]*Index{indexID(schema.ClassName(collection)): idx}

	err := db.Backupable(context.Background(), []string{collection})
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive the join so the coordinator still answers 422")

	body := err.Error()
	assert.Contains(t, body, collection)
	for _, leaked := range []string{shard, node} {
		assert.NotContainsf(t, body, leaked, "DB.Backupable leaked %q into the refusal", leaked)
	}
}

// The real wrappers between the shard that refused and the stored failure meta
// name the shard: "snapshot shard <id>: halt for snapshot: ...". That is what
// the operator log should say. The publishable message has to survive the
// wrapping intact so the boundary can pull it back out.
func TestCreateBackupSnapshot_RefusalKeepsAPublishableMessage(t *testing.T) {
	ctx := testCtx()
	className := "ShardSnapshotRedactClass"
	shd, idx := testShard(t, ctx, className)

	require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
	idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{className, shd.Name()}: true,
	}))

	var sd entitiesbackup.ShardDescriptor
	_, err := shd.(*Shard).CreateBackupSnapshot(ctx, &sd, t.TempDir())
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive the snapshot wrappers")

	// The log form keeps the traversal, shard and all.
	require.Contains(t, err.Error(), "halt for snapshot",
		"the operator needs to know which step refused")

	var blocked entitiesbackup.ReindexBlockedError
	require.ErrorAs(t, err, &blocked,
		"the publishable message must stay reachable under the wrappers")
	require.Contains(t, blocked.Error(), className)
	require.NotContains(t, blocked.Error(), shd.Name(),
		"the publishable message is what reaches the status API")
	require.NotContains(t, blocked.Error(), "halt for snapshot",
		"and it carries the condition, not the path that found it")
}

// The cleanup window must not advise cancelling: the task is already cancelled,
// and the teardown is what that cancel produced.
func TestReindexInFlightError_CleanupAdviceDoesNotSayCancel(t *testing.T) {
	err := reindexInFlightError("MyClass", reindexBlockedByCleanup)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

	body := err.Error()
	assert.Contains(t, body, "MyClass")
	assert.Contains(t, body, "still removing its temporary index files")
	assert.Contains(t, body, "retry once the cleanup finishes")
	assert.NotContains(t, body, "cancel it via",
		"the task is already cancelled; this advice sends the operator after a task that is gone")
	assert.NotContains(t, body, `"cancel":true`)

	// The live-task branch keeps its cancel advice: there the task is real.
	live := reindexInFlightError("MyClass", reindexBlockedByLiveTask).Error()
	assert.Contains(t, live, "cancel it via")
}
