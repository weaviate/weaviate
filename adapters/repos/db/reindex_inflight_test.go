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
	"slices"
	"strings"
	"sync"
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

// Collection and shard scoping is not pinned here: the compare lives entirely
// inside the injected lookup, which production builds in
// adapters/handlers/rest. A test written against makeActivityBuilder would
// assert that the fixture does what the fixture does. See
// TestShardReindexActivityBuilderScopesByCollectionAndShard in
// adapters/handlers/rest.

// TestReindexGate_BuilderUnwired pins that an unwired lookup defaults to
// allow, with a rate-limited WARN. Refusing instead broke every
// module-test fixture that spins up Weaviate outside the post-bootstrap
// install path — and the window is reachable from outside, so the WARN is
// the only signal a backup took it (see [DB.SetShardReindexActivityLookup]).
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
	// This reason covers PREPARING and SWAPPING too, and DTM refuses to cancel
	// a task in either. Promising cancel outright loops the operator between a
	// backup this text refuses and a cancel that refuses back.
	require.Contains(t, err.Error(), "While it is still building indexes you can cancel it")
	require.Contains(t, err.Error(), "once it has started committing its result it can only be waited out")
	// Nor is the wait promised to end; only a restart with the flag off lifts it.
	require.Contains(t, err.Error(),
		"if a node that owned part of it left the cluster it never finishes at all")
	require.Contains(t, err.Error(),
		"a restart with RUNTIME_REINDEX_ENABLED=false is then the only way to lift this refusal")
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
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		activityBuilds++
		return func(string, string) bool { return false }, nil
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		cleanupBuilds++
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	require.NoError(t, db.Backupable(context.Background(), []string{collection}))

	assert.Equal(t, 1, activityBuilds, "activity snapshot must be built once per admission pass, not once per shard")
	assert.Equal(t, 1, cleanupBuilds, "cleanup snapshot must be built once per admission pass, not once per shard")
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
	assert.NotContains(t, err.Error(), leaderErr.Error(),
		"the RAFT-transport cause is log-only; the body grants nothing on cluster internals")
	assert.True(t, errors.Is(err, leaderErr), "but it stays matchable")
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

// causeFirstRefusal is the shape of a refusal whose message states what
// actually happened, keeping the sentinel reachable through Unwrap so
// errors.Is still matches across the canCommit RPC. The stand-in that drives
// that boundary in usecases/backup is held to the same shape, so narrowing
// either one's Unwrap breaks a build.
type causeFirstRefusal interface {
	error
	Unwrap() []error
}

var _ causeFirstRefusal = reindexStateUnknown{}

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

	// Redacted, so the stored failure meta can publish it verbatim.
	var publishable entitiesbackup.ReindexBlockedError
	require.True(t, errors.As(err, &publishable),
		"the refusal must be publishable in the stored failure meta")
	require.Equal(t, err.Error(), publishable.Error())

	// Matching still works, including across the canCommit RPC, where
	// classifyCanCommitErr decides the response kind with errors.Is.
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
		"the sentinel must still match")
	require.True(t, errors.Is(err, cause), "the cause must still match")
}

// TestReindexInFlightError_GenuineRefusalIsPinned holds the genuine
// refusal byte for byte: it is parsed downstream, and the unknown-state
// rewording must not touch it.
//
// The text names the collection and nothing else. Both the shard id and
// the "on this shard" tail of the sentinel are redacted, so this is not
// the string a 12471-only build produced — the redaction is deliberate
// and the shard reaches the operator through the log instead.
func TestReindexInFlightError_GenuineRefusalIsPinned(t *testing.T) {
	want := `backup blocked: runtime-reindex in flight: collection "MyClass" has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status="ready"). While it is still building indexes you can cancel it via PUT /v1/schema/<class>/indexes/<prop> {"<indexType>":{"cancel":true}}; once it has started committing its result it can only be waited out, and if a node that owned part of it left the cluster it never finishes at all — a restart with RUNTIME_REINDEX_ENABLED=false is then the only way to lift this refusal. If every index already reports "ready", the task holding this gate is one this server cannot attribute to a collection — the same cancel call, on any collection, clears it`
	require.Equal(t, want, reindexInFlightError("MyClass", reindexBlockedByLiveTask).Error())

	wantPreWire := `backup blocked: runtime-reindex in flight: collection "MyClass": backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping`
	require.Equal(t, wantPreWire, reindexInFlightError("MyClass", reindexBlockedPreWire).Error())
}

// unknownReindexHold stands in for a hold kind added to the enum after this
// build shipped. Derived from the last named constant so it stays out of range
// as the enum grows.
const unknownReindexHold = ReindexHoldSubmit + 1

// The node-local hold lookup decides admission, so its unknown arm must fail
// closed: a hold this build cannot classify still means something is holding
// the shard.
func TestReindexGate_HoldKinds(t *testing.T) {
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

			assert.Equal(t, test.wantReason, newReindexGate(db).blockReason("MyClass", "shard1"))

			idx := &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName("MyClass")}}
			err := idx.refuseIfReindexInFlight("shard1")
			if !test.wantRefuse {
				require.NoError(t, err, "backup must be admitted")
				return
			}
			require.Error(t, err, "backup must be refused")
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
		})
	}
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
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return func(string, string) bool { return true }, nil // every shard refuses
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
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		return func(string, string) bool { return true }, nil
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
	// A literal, not the constant the code caps with: asserting the bound
	// against itself cannot fail, so raising the constant to 100000 would keep
	// this green while 60 names went into the field. The number is deliberately
	// duplicated — that duplication is what makes the assertion able to fail.
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
