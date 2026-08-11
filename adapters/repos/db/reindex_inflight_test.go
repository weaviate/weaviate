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
// sentinel, names the collection, and surfaces the operator remediation hint.
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
	err := reindexInFlightError("MyClass", true)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "startup window")
}

// TestReindexInFlightError_DTMHit pins the wording variant used when
// DTM reports a live task.
func TestReindexInFlightError_DTMHit(t *testing.T) {
	err := reindexInFlightError("MyClass", false)
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Contains(t, err.Error(), "MyClass")
	require.Contains(t, err.Error(), "active runtime-reindex task in DTM")
	require.Contains(t, err.Error(), "retry after the migration finishes")

	// Property/index type aren't known here, so the message points at the
	// poll instead of a guessable <placeholder> URL that would 202 NO_OP.
	require.Contains(t, err.Error(), "GET /v1/schema/MyClass/indexes")
	require.Contains(t, err.Error(), "PUT /v1/schema/MyClass/indexes/{that property}")
	require.NotContains(t, err.Error(), "<class>")
	require.NotContains(t, err.Error(), "<prop>")
	require.NotContains(t, err.Error(), "<indexType>")
}

// On a namespace-enabled cluster the class name is stored qualified. The
// rendered URLs keep the prefix: a global operator has to type it for the
// request to reach the right collection, and the REST error path removes it
// again for the namespace-confined caller who must not.
func TestReindexInFlightError_QualifiedCollectionKeepsItsPrefix(t *testing.T) {
	err := reindexInFlightError("customer1:MyClass", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GET /v1/schema/customer1:MyClass/indexes")
	require.Contains(t, err.Error(), "PUT /v1/schema/customer1:MyClass/indexes/{that property}")
	require.NotContains(t, err.Error(), "/v1/schema/MyClass/")
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
	idx := &Index{db: db, logger: logger, Config: IndexConfig{ClassName: schema.ClassName(collection)}}

	err := idx.refuseIfReindexInFlight(shard)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive so the coordinator still answers 422")

	body := err.Error()
	assert.Contains(t, body, collection, "the caller named this collection itself")
	for _, leaked := range []string{shard, node} {
		assert.NotContainsf(t, body, leaked, "the refusal body leaked %q", leaked)
	}

	// The check itself runs once per shard of a whole-collection pass, so the
	// pass owns the log. This is the single-shard form of it.
	idx.logReindexRefusal(shard, err)

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

// logReindexRefusal sits on error paths that carry every kind of failure, so it
// has to stay silent for the ones that are not a gate refusal.
func TestLogReindexRefusal_IgnoresUnrelatedErrors(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	idx := &Index{logger: logger, Config: IndexConfig{ClassName: schema.ClassName("AnyClass")}}

	idx.logReindexRefusal("s1", nil)
	idx.logReindexRefusal("s1", errors.New("disk full"))

	assert.Empty(t, hook.AllEntries(), "only a gate refusal produces a gate log line")
}

// The refusal has to stay reachable under the wrappers the backup path adds on
// the way out, which name the shard. That copy is what the operator log wants;
// the status API gets the publishable one.
func TestReindexRefusal_SurvivesWrappingAsAPublishableMessage(t *testing.T) {
	const (
		collection = "WrappedClass"
		shard      = "zmDMRo4olU4c"
	)

	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{collection, shard}: true,
	}))
	idx := &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName(collection)}}

	refusal := idx.refuseIfReindexInFlight(shard)
	require.Error(t, refusal)
	wrapped := fmt.Errorf("snapshot shard %s: halt for snapshot: %w", shard, refusal)

	require.ErrorIs(t, wrapped, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive the snapshot wrappers")

	var blocked entitiesbackup.ReindexBlockedError
	require.ErrorAs(t, wrapped, &blocked,
		"the publishable message must stay reachable under the wrappers")
	assert.Contains(t, blocked.Error(), collection)
	assert.NotContains(t, blocked.Error(), shard,
		"the publishable message is what reaches the status API")
	assert.NotContains(t, blocked.Error(), "halt for snapshot",
		"and it carries the condition, not the path that found it")
}

// backupableFixture wires the minimum a DB.Backupable call needs: one index
// whose sharding state lists the given local shards.
func backupableFixture(t *testing.T, collection, node string, shards ...string) *DB {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger, localNodeName: node}
	db.indices = map[string]*Index{}

	physical := make(map[string]sharding.Physical, len(shards))
	for _, s := range shards {
		physical[s] = sharding.Physical{Name: s, BelongsToNodes: []string{node}}
	}
	shardState := &sharding.State{IndexID: collection, Physical: physical}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(&models.Class{Class: collection}, shardState))
	})
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.On("NodeName").Return(node)

	idx := &Index{
		db:           db,
		Config:       IndexConfig{ClassName: schema.ClassName(collection)},
		schemaReader: reader,
		getSchema:    getter,
	}
	db.indices[indexID(schema.ClassName(collection))] = idx
	return db
}

// Every refusing shard produces the same sentence, because the text names no
// shard. A per-shard join therefore returns one identical line per shard: 60
// copies on a 60-shard node, and shard counts in this repo reach five figures.
func TestBackupableRefusesOncePerReasonNotOncePerShard(t *testing.T) {
	const (
		collection = "WideClass"
		node       = "weaviate-0"
	)
	shards := make([]string, 0, 60)
	for i := range 60 {
		shards = append(shards, fmt.Sprintf("s%02d", i))
	}

	db := backupableFixture(t, collection, node, shards...)
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true } // every shard refuses
	})

	err := db.Backupable(context.Background(), []string{collection})
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

	lines := strings.Count(err.Error(), "\n") + 1
	require.Equalf(t, 1, lines,
		"every shard refuses for the same reason and the text names no shard, so the body must carry "+
			"that reason once, not %d times", len(shards))
	require.Equal(t, reindexInFlightError(collection, false).Error(), err.Error(),
		"deduping must not change the sentence itself")
}

// The log has to consolidate the same way the response body does. A per-shard
// WARN turns a 1-line body into 61 entries on a 60-shard refusal, which is the
// same O(shards) growth one tier down. The shard list carried in the aggregate
// line has the same problem if it is uncapped, since this pass can cover
// five-figure shard counts.
func TestBackupableLogsOnceForAWideRefusal(t *testing.T) {
	const (
		collection = "WideClass"
		node       = "weaviate-0"
		shardCount = 60
	)
	shards := make([]string, 0, shardCount)
	for i := range shardCount {
		shards = append(shards, fmt.Sprintf("s%02d", i))
	}

	logger, hook := logrustest.NewNullLogger()
	// Debug on, so nothing hides behind the level. The per-shard Debug lines in
	// AnyLiveReindexForShard are O(shards) and stay that way on purpose: they are
	// the only per-shard visibility into which side of the gate fired, Debug is
	// off in production, and the bound that matters is on what an operator
	// actually sees. That is what the warn-and-above count below pins.
	logger.SetLevel(logrus.DebugLevel)
	db := backupableFixture(t, collection, node, shards...)
	db.logger = logger
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})

	require.Error(t, db.Backupable(context.Background(), []string{collection}))

	// Counted by LEVEL, not by message. The per-shard risk is that some future
	// edit promotes one of the per-shard Debug lines to Warn — a 60-shard
	// refusal is then 61 operator-facing entries for a 1-line body. Matching a
	// message string cannot catch that: the only string naming a single shard
	// at Warn comes from logReindexRefusal, which this path never calls, so
	// such an assertion is 0 no matter what the code does.
	var warnAndAbove, aggregate int
	var sample []string
	var reportedCount int
	for _, e := range hook.AllEntries() {
		if e.Level <= logrus.WarnLevel {
			warnAndAbove++
		}
		if strings.Contains(e.Message, "are held by the reindex gate") {
			aggregate++
			if v, ok := e.Data["blocked_shards"].([]string); ok {
				sample = v
			}
			if v, ok := e.Data["blocked_shard_count"].(int); ok {
				reportedCount = v
			}
		}
	}

	require.Equalf(t, 1, warnAndAbove,
		"a refusal of one collection is one operator-facing entry regardless of width; "+
			"%d shards produced %d warn-or-above entries, so the per-shard growth is back",
		shardCount, warnAndAbove)
	require.Equal(t, 1, aggregate, "one refusal of one collection is one operator-facing line")
	require.Equal(t, shardCount, reportedCount, "the count must be exact even though the names are sampled")
	// A literal, not the constant the code caps with: asserting the bound
	// against itself cannot fail, so raising the constant to 100000 would keep
	// this green while 60 names went into the field. The number is deliberately
	// duplicated — that duplication is what makes the assertion able to fail.
	const wantSampleCap = 10
	require.LessOrEqualf(t, len(sample), wantSampleCap,
		"the shard list must be capped at %d, or the growth just moves into a log field; got %d",
		wantSampleCap, len(sample))
}

// A gate refusal on one collection must not swallow an unrelated failure on
// another: the caller is told to retry the backup, and would retry into the
// same second failure with no idea it is there. Neither message names the node.
func TestBackupableReportsNonGateErrorsAlongsideARefusal(t *testing.T) {
	const (
		blocked = "BlockedClass"
		broken  = "BrokenClass"
		node    = "weaviate-0"
	)

	logger, _ := logrustest.NewNullLogger()
	db := backupableFixture(t, blocked, node, "s1")
	db.logger = logger
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})

	// A second collection whose shard enumeration fails.
	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("Read", broken, true, mock.Anything).Return(errors.New("raft read failed"))
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.On("NodeName").Return(node)
	db.indices[indexID(schema.ClassName(broken))] = &Index{
		db:           db,
		Config:       IndexConfig{ClassName: schema.ClassName(broken)},
		schemaReader: reader,
		getSchema:    getter,
	}

	err := db.Backupable(context.Background(), []string{blocked, broken})
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the gate refusal must still classify, so the coordinator answers 422")
	assert.NotContains(t, err.Error(), node, "the response must not name the node")
	assert.Contains(t, err.Error(), "raft read failed",
		"the second collection's failure is the caller's to act on too")
	assert.Contains(t, err.Error(), broken, "and it has to say which collection it belongs to")

	// The gate refusal leads, so canCommitErrFromResponse's prefix check still
	// recognizes the joined message as a refusal.
	assert.True(t, strings.HasPrefix(err.Error(),
		entitiesbackup.ErrBackupBlockedByInFlightReindex.Error()))
}
