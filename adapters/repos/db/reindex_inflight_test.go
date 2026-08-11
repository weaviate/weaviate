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
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// makeActivityBuilder builds a ShardReindexActivityLookupBuilder that
// reports a fixed set of (collection, shard) pairs as live.
func makeActivityBuilder(live map[[2]string]bool) ShardReindexActivityLookupBuilder {
	return func(context.Context) ShardReindexActivityLookup {
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
	assert.True(t, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"),
		"gate must refuse when DTM reports a live task on the tuple")
}

// A task on the shard only holds the gate while its DTM status is live. The
// builder here filters a real task list through [IsLiveReindexTaskStatus], the
// same predicate production's builder applies, so the terminal statuses are
// exercised rather than asserted away by a hand-written empty map.
func TestAnyLiveReindexForShard_TaskStatusDecides(t *testing.T) {
	tests := []struct {
		status    distributedtask.TaskStatus
		wantBlock bool
	}{
		{status: distributedtask.TaskStatusStarted, wantBlock: true},
		{status: distributedtask.TaskStatusFinished},
		{status: distributedtask.TaskStatusCancelled},
		{status: distributedtask.TaskStatusFailed},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			task := &distributedtask.Task{Status: tc.status}
			db := &DB{}
			db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
				live := map[[2]string]bool{}
				if IsLiveReindexTaskStatus(task.Status) {
					live[[2]string{"MyClass", "shard1"}] = true
				}
				return func(collection, shardName string) bool {
					return live[[2]string{collection, shardName}]
				}
			})
			assert.Equal(t, tc.wantBlock, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"))
		})
	}
}

// Collection and shard scoping is not pinned here: the compare lives entirely
// inside the injected lookup, which production builds in
// adapters/handlers/rest. A test written against makeActivityBuilder would
// assert that the fixture does what the fixture does. See
// TestShardReindexActivityBuilderScopesByCollectionAndShard in
// adapters/handlers/rest.

// TestAnyLiveReindexForShard_BuilderUnwired pins that an unwired
// lookup defaults to "no live reindex" — production gates HTTP serving
// on bootstrap completion so the unwired window is unreachable by
// external traffic, and the prior refuse-by-default broke every
// module-test fixture that spins up Weaviate without going through
// the post-bootstrap install path. A rate-limited WARN fires to surface
// the unwired path if it ever shows up in production logs.
func TestAnyLiveReindexForShard_BuilderUnwired(t *testing.T) {
	db := &DB{}
	assert.False(t, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"),
		"unwired gate must allow (with WARN); production gates HTTP on bootstrap")
}

// TestAnyLiveReindexForShard_BuilderReturnsNil pins the same fail-open
// when the installed builder returns a nil closure (defensive against
// a misconfigured wiring).
func TestAnyLiveReindexForShard_BuilderReturnsNil(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		return nil
	})
	assert.False(t, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"),
		"nil lookup must allow (same path as unwired)")
}

// The unwired gate is a persistent misconfiguration, so the WARN is rate
// limited rather than once-ever — but it must still not fire once per shard
// checked. Reopening after the window is covered by logrusext's own test.
func TestWarnUnwiredReindexGate_RateLimited(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	for range 5 {
		db.warnUnwiredReindexGate()
	}

	entries := hook.AllEntries()
	require.Len(t, entries, 1, "the unwired WARN must be rate limited, not repeated per call")
	assert.Equal(t, logrus.WarnLevel, entries[0].Level)
	assert.Equal(t, "backup_reindex_gate", entries[0].Data["action"])
	assert.Contains(t, entries[0].Message, "ShardReindexActivityLookup not yet installed")
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

	err := idx.refuseIfReindexInFlight(context.Background(), "ABC123")
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
	require.NoError(t, idx.refuseIfReindexInFlight(context.Background(), "ABC123"))
}

// TestRefuseIfReindexInFlight_DbNilIsConservative pins that an Index
// without its DB back-reference refuses rather than letting a backup
// proceed unchecked.
func TestRefuseIfReindexInFlight_DbNilIsConservative(t *testing.T) {
	idx := &Index{Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}}
	err := idx.refuseIfReindexInFlight(context.Background(), "ABC123")
	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.True(t, strings.Contains(err.Error(), "startup window"))
}

// The multi-shard variant is the one [DB.Backupable] reaches, i.e. the
// coordinator's canCommit gate. It carries its own nil-db check, and an Index
// without its back-reference there would otherwise admit an unchecked backup.
func TestRefuseIfReindexInFlightIn_DbNilIsConservative(t *testing.T) {
	// A snapshot that reports every shard free: the refusal must come from the
	// missing back-reference, not from anything the snapshot says.
	snap := reindexGateSnapshot{
		activity: func(string, string) bool { return false },
		cleanup:  func(string, string) ReindexHold { return ReindexHoldNone },
	}
	idx := &Index{Config: IndexConfig{ClassName: schema.ClassName("JourneyClass")}}

	err := idx.refuseIfReindexInFlightIn(snap, "ABC123")
	require.Error(t, err, "canCommit must not admit a backup it could not check")
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
	require.Contains(t, err.Error(), "startup window")
}

// TestReindexInFlightError_NoDBBackref pins the wording variant used
// for an Index that has no DB back-reference yet.
func TestReindexInFlightError_NoDBBackref(t *testing.T) {
	err := reindexInFlightError("MyClass", reindexBlockedNoDBBackref)
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
	// This reason covers PREPARING and SWAPPING too, and DTM accepts a cancel in
	// both: it refuses one only for a task that already reached a terminal
	// status, and such a task does not hold this gate. So the advice offers
	// cancel without conditions, and must not send the operator to a restart.
	require.Contains(t, err.Error(), "or lift this refusal now by cancelling it via")
	require.Contains(t, err.Error(), `{"cancel":true}`)
	require.Contains(t, err.Error(), "Cancel is accepted at every stage of a migration")
	require.NotContains(t, err.Error(), "RUNTIME_REINDEX_ENABLED",
		"a wedged gate clears with a cancel; a cluster restart is not the escape hatch")
	require.NotContains(t, err.Error(), "can only be waited out")
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

	err := idx.refuseIfReindexInFlight(context.Background(), shard)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"the sentinel must survive so the coordinator still answers 422")

	body := err.Error()
	assert.Contains(t, body, collection, "the caller named this collection itself")
	for _, leaked := range []string{shard, node} {
		assert.NotContainsf(t, body, leaked, "the refusal body leaked %q", leaked)
	}

	// Single-shard form of the pass-level log.
	idx.logReindexRefusal(shard, err)

	var logged *logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "refused a replica shard copy") {
			logged = entry
		}
	}
	require.NotNil(t, logged, "the operator needs a log line naming what was refused")
	assert.Equal(t, shard, logged.Data["shard"])
	assert.Equal(t, node, logged.Data["node"])
	assert.Equal(t, collection, logged.Data["collection"])
}

// Pins: a replica-snapshot RPC refusal must still name the shard via the
// local WARN, in both snapshot modes, since the RPC response can't carry it.
func TestIncomingCreateReplicaSnapshot_LogsGateRefusal(t *testing.T) {
	tests := []struct {
		name        string
		className   string
		noHardlinks bool
	}{
		{name: "hardlink snapshot path", className: "ReplicaSnapGateHardlink"},
		{name: "halt-for-duration fallback path", className: "ReplicaSnapGateFallback", noHardlinks: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.noHardlinks {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			}
			ctx := testCtx()
			shd, idx := testShard(t, ctx, tc.className)
			// The fixture skips NewIndex, which normally initializes this.
			idx.replicaSnapshotOpLocks = esync.NewKeyRWLocker()

			logger, hook := logrustest.NewNullLogger()
			idx.logger = logger
			require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
			idx.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
				{tc.className, shd.Name()}: true,
			}))

			_, err := idx.IncomingCreateReplicaSnapshot(ctx, shd.Name(), "op-gate")
			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

			var logged *logrus.Entry
			for _, e := range hook.AllEntries() {
				if strings.Contains(e.Message, "refused a replica shard copy") {
					logged = e
				}
			}
			require.NotNil(t, logged, "the operator needs a log line naming the refused shard")
			assert.Equal(t, logrus.WarnLevel, logged.Level)
			assert.Equal(t, shd.Name(), logged.Data["shard"])
			assert.Equal(t, tc.className, logged.Data["collection"])
			assert.Equal(t, idx.db.localNodeName, logged.Data["node"])
		})
	}
}

// Pins: the pass log sorts its shard sample on a copy, leaving the
// caller's slice order untouched.
func TestLogReindexRefusalPass_SortsACopy(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	shards := []string{"s-c", "s-a", "s-b"}

	logReindexRefusalPass(logger, "test pass", "node1", "SortClass", shards)

	require.Len(t, hook.AllEntries(), 1)
	entry := hook.AllEntries()[0]
	sample, ok := entry.Data["blocked_shards"].([]string)
	require.True(t, ok, "blocked_shards must be a []string field")
	assert.Equal(t, []string{"s-a", "s-b", "s-c"}, sample,
		"the sample must be sorted so repeated refusals diff cleanly")
	assert.Equal(t, []string{"s-c", "s-a", "s-b"}, shards,
		"the caller's slice must keep its order")
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

// The publishable message must survive wrapping (which adds the shard) so
// the status API doesn't get the operator-log copy.
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

	refusal := idx.refuseIfReindexInFlight(context.Background(), shard)
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
	return multiCollectionBackupableFixture(t, node, map[string][]string{collection: shards})
}

// Building the activity snapshot is a leader-forwarded RAFT query, so a
// per-shard rebuild costs one leader round trip per shard. Pins one build per
// admission pass regardless of shard count.
func TestBackupable_BuildsGateSnapshotOncePerCall(t *testing.T) {
	const (
		collection = "SnapshotBuildCountClass"
		node       = "weaviate-0"
	)

	db := backupableFixture(t, collection, node, "s1", "s2", "s3", "s4", "s5")

	var activityBuilds, cleanupBuilds int
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		activityBuilds++
		return func(string, string) bool { return false }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		cleanupBuilds++
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	require.NoError(t, db.Backupable(context.Background(), []string{collection}))

	assert.Equal(t, 1, activityBuilds, "activity snapshot must be built once per admission pass, not once per shard")
	assert.Equal(t, 1, cleanupBuilds, "cleanup snapshot must be built once per admission pass, not once per shard")
}

// The gate's activity snapshot is a leader-forwarded RAFT query. Bound to the
// process-lifetime wiring context instead of the caller's, a leader that
// accepts the connection and stops answering parks the participant's canCommit
// goroutine until shutdown, long after the coordinator gave up.
func TestBackupable_PassesTheCallerContextToTheGate(t *testing.T) {
	const (
		collection = "GateContextClass"
		node       = "weaviate-0"
	)

	db := backupableFixture(t, collection, node, "s1")
	db.SetShardReindexActivityLookup(func(ctx context.Context) ShardReindexActivityLookup {
		// A leader that never answers: the only way out is the caller's ctx.
		<-ctx.Done()
		return func(string, string) bool { return true }
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- db.Backupable(ctx, []string{collection}) }()
	select {
	case err := <-done:
		require.Error(t, err, "a gate that could not be consulted must refuse")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Backupable ignored the caller's context and waited on the leader instead")
	}
}

// The per-shard capture paths each build their own gate snapshot, so each must
// hand its caller's context to the leader query behind it. Bound to anything
// else, a leader that accepts the connection and stops answering parks the
// capture until process shutdown, long after the coordinator gave up. Covers
// every call site of [Index.refuseIfReindexInFlight], including the shared
// function itself.
func TestPerShardCapturePathsPassTheCallerContextToTheGate(t *testing.T) {
	shd, idx := testShard(t, testCtx(), "GateCtxPerShardClass")
	require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
	idx.db.SetShardReindexActivityLookup(func(ctx context.Context) ShardReindexActivityLookup {
		// A leader that never answers: the only way out is the caller's ctx.
		<-ctx.Done()
		return func(string, string) bool { return true }
	})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	stagingRoot := t.TempDir()

	tests := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{"refuseIfReindexInFlight", func(ctx context.Context) error {
			return idx.refuseIfReindexInFlight(ctx, shd.Name())
		}},
		{"backupInactiveShardWithHardlinks", func(ctx context.Context) error {
			var sd entitiesbackup.ShardDescriptor
			return idx.backupInactiveShardWithHardlinks(ctx, shd.Name(), &sd, nil, stagingRoot)
		}},
		{"backupInactiveShardWithoutHardlinks", func(ctx context.Context) error {
			var sd entitiesbackup.ShardDescriptor
			return idx.backupInactiveShardWithoutHardlinks(ctx, shd.Name(), &sd, nil)
		}},
		{"HaltForTransfer", func(ctx context.Context) error {
			return shd.HaltForTransfer(ctx, false, 0)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- tc.call(cancelled) }()
			select {
			case err := <-done:
				require.Error(t, err, "a gate that could not be consulted must refuse")
			case <-time.After(5 * time.Second):
				require.Failf(t, "context not threaded",
					"%s ignored the caller's context and waited on the leader instead", tc.name)
			}
		})
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
	assert.NotContains(t, body, "cancelling it via",
		"the task is already cancelled; this advice sends the operator after a task that is gone")
	assert.NotContains(t, body, `"cancel":true`)

	// The live-task branch keeps its cancel advice: there the task is real.
	live := reindexInFlightError("MyClass", reindexBlockedByLiveTask).Error()
	assert.Contains(t, live, "cancelling it via")
}

// The submit window's advice must stay its own: nothing was cancelled and no
// task exists yet, so either sibling arm's text would send the operator after
// something that is not there.
func TestReindexInFlightError_SubmitAdviceNamesTheSubmission(t *testing.T) {
	body := reindexInFlightError("MyClass", reindexBlockedBySubmit).Error()
	assert.Contains(t, body, "MyClass")
	assert.Contains(t, body, "a reindex submission is preparing this collection")
	assert.Contains(t, body, "retry in a moment")
	assert.NotContains(t, body, "removing its temporary index files",
		"that is the cleanup arm's text; here it would misdescribe the block")
	assert.NotContains(t, body, "cancelling it via",
		"there is no task to cancel during a submission")
}

// unknownReindexHold stands in for a hold kind added to the enum after this
// build shipped. Derived from the last named constant so it stays out of range
// as the enum grows.
const unknownReindexHold = ReindexHoldSubmit + 1

// The node-local hold lookup decides admission, so its unknown arm must fail
// closed: a hold this build cannot classify still means something is holding
// the shard.
func TestReindexBlockReasonIn_HoldKinds(t *testing.T) {
	tests := []struct {
		name       string
		hold       ReindexHold
		wantReason reindexBlockReason
		wantRefuse bool
	}{
		{name: "no hold admits", hold: ReindexHoldNone, wantReason: reindexNotBlocked},
		{name: "cleanup refuses", hold: ReindexHoldCleanup, wantReason: reindexBlockedByCleanup, wantRefuse: true},
		{name: "submit refuses", hold: ReindexHoldSubmit, wantReason: reindexBlockedBySubmit, wantRefuse: true},
		{name: "unknown hold refuses", hold: unknownReindexHold, wantReason: reindexBlockedByUnknownHold, wantRefuse: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			db := &DB{logger: logger}
			db.SetShardReindexActivityLookup(makeActivityBuilder(nil))
			db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
				return func(string, string) ReindexHold { return test.hold }
			})

			assert.Equal(t, test.wantReason, db.reindexBlockReason(context.Background(), "MyClass", "shard1"))

			idx := &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName("MyClass")}}
			err := idx.refuseIfReindexInFlight(context.Background(), "shard1")
			if !test.wantRefuse {
				require.NoError(t, err, "backup must be admitted")
				return
			}
			require.Error(t, err, "backup must be refused")
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
		})
	}
}

// The refusal text names no shard, so per-shard joining must not repeat the
// same sentence once per shard.
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
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		return func(string, string) bool { return true } // every shard refuses
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	err := db.Backupable(context.Background(), []string{collection})
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

	lines := strings.Count(err.Error(), "\n") + 1
	require.Equalf(t, 1, lines,
		"every shard refuses for the same reason and the text names no shard, so the body must carry "+
			"that reason once, not %d times", len(shards))
	require.Equal(t, reindexInFlightError(collection, reindexBlockedByLiveTask).Error(), err.Error(),
		"deduping must not change the sentence itself")
}

// The log has to consolidate the same way the response body does. A per-shard
// WARN turns a 1-line body into 121 entries on a 60-shard refusal, which is the
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
	// reindexBlockReasonIn are O(shards) and stay that way on purpose: they are the
	// only per-shard visibility into which side of the gate fired, Debug is off in
	// production, and the bound that matters is on what an operator actually sees.
	// That is what the warn-and-above count below pins.
	logger.SetLevel(logrus.DebugLevel)
	db := backupableFixture(t, collection, node, shards...)
	db.logger = logger
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	require.Error(t, db.Backupable(context.Background(), []string{collection}))

	// Counted by LEVEL, not by message. The per-shard risk is that some future
	// edit promotes one of the per-shard Debug lines in refuseIfReindexInFlightIn
	// to Warn — a 60-shard refusal is then 61 operator-facing entries for a
	// 1-line body. Matching a message string cannot catch that: the only string
	// naming a single shard at Warn comes from logReindexRefusal, which this
	// path never calls, so such an assertion is 0 no matter what the code does.
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
	// A literal, not the constant the code caps with, so raising the constant
	// alone can't fool this assertion.
	const wantSampleCap = 10
	require.LessOrEqualf(t, len(sample), wantSampleCap,
		"the shard list must be capped at %d, or the growth just moves into a log field; got %d",
		wantSampleCap, len(sample))
}

func multiCollectionBackupableFixture(t *testing.T, node string, byCollection map[string][]string) *DB {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger, localNodeName: node}
	db.indices = map[string]*Index{}
	for collection, shards := range byCollection {
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
	}
	return db
}

// TestAnyLiveReindexForShard_DifferentCollection pins that a live task
// in another collection does not block a backup of the queried
// collection.
func TestAnyLiveReindexForShard_DifferentCollection(t *testing.T) {
	db := &DB{}
	db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"OtherClass", "shard1"}: true,
	}))
	assert.False(t, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"),
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
	assert.False(t, db.AnyLiveReindexForShard(context.Background(), "MyClass", "shard1"),
		"gate must scope by shard, not just by collection")
}

// On a namespace-enabled cluster the class name is stored qualified. The
// rendered URLs keep the prefix: a global operator has to type it for the
// request to reach the right collection, and the REST error path removes it
// again for the namespace-confined caller who must not.
func TestReindexInFlightError_QualifiedCollectionKeepsItsPrefix(t *testing.T) {
	err := reindexInFlightError("customer1:MyClass", reindexBlockedByLiveTask)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GET /v1/schema/customer1:MyClass/indexes")
	require.Contains(t, err.Error(), "PUT /v1/schema/customer1:MyClass/indexes/{that property}")
	require.NotContains(t, err.Error(), "/v1/schema/MyClass/")
}

// A gate refusal on one collection must not swallow an unrelated failure on
// another, or the caller retries blind into it.
func TestBackupableReportsNonGateErrorsAlongsideARefusal(t *testing.T) {
	const (
		blocked = "BlockedClass"
		broken  = "BrokenClass"
		node    = "weaviate-0"
	)

	logger, _ := logrustest.NewNullLogger()
	db := backupableFixture(t, blocked, node, "s1")
	db.logger = logger
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
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
