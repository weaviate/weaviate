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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

// makeActivityBuilder builds a ShardReindexActivityLookupBuilder that
// reports a fixed set of (collection, shard) pairs as live.
func makeActivityBuilder(live map[[2]string]bool) ShardReindexActivityLookupBuilder {
	return func() ShardReindexActivityLookup {
		return func(collection, shardName string) (bool, bool) {
			return live[[2]string{collection, shardName}], false
		}
	}
}

// A nil live or tasks field leaves that lookup uninstalled.
type gateFixtures struct {
	live  map[[2]string]bool
	holds map[string]ReindexHold
	tasks []*distributedtask.Task
}

type gateCounters struct{ activity, shard int }

func gatedDB(t *testing.T, f gateFixtures) (*DB, *logrustest.Hook, *gateCounters) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	db := &DB{logger: logger, localNodeName: "node-7"}
	var built gateCounters
	if f.live != nil {
		db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
			built.shard++
			return makeActivityBuilder(f.live)()
		})
	}
	for collection, hold := range f.holds {
		db.reindexHolds.acquire(collection, hold)
	}
	if f.tasks != nil {
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup {
			built.activity++
			return NewAnyReindexActivityLookup(f.tasks)
		})
	}
	return db, hook, &built
}

func warnOrAbove(hook *logrustest.Hook) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.WarnLevel {
			out = append(out, entry)
		}
	}
	return out
}

func gatedIndex(db *DB, className string) *Index {
	return &Index{db: db, Config: IndexConfig{ClassName: schema.ClassName(className)}}
}

func TestAnyLiveReindexForShard(t *testing.T) {
	tests := []struct {
		name    string
		live    map[[2]string]bool
		builder ShardReindexActivityLookupBuilder
		want    bool
	}{
		{name: "live task on the tuple", live: map[[2]string]bool{{"MyClass", "shard1"}: true}, want: true},
		{name: "no live task anywhere", live: map[[2]string]bool{}},
		{
			name: "live only on other keys, including the swapped tuple",
			live: map[[2]string]bool{
				{"OtherClass", "shard1"}: true,
				{"MyClass", "shard2"}:    true,
				{"shard1", "MyClass"}:    true,
			},
		},
		{
			name:    "builder never installed",
			builder: nil,
		},
		{name: "builder hands back nothing", builder: func() ShardReindexActivityLookup { return nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{}
			switch {
			case tt.live != nil:
				db.SetShardReindexActivityLookup(makeActivityBuilder(tt.live))
			case tt.builder != nil:
				db.SetShardReindexActivityLookup(tt.builder)
			}
			live, _ := db.AnyLiveReindexForShard("MyClass", "shard1")
			require.Equal(t, tt.want, live)
		})
	}
}

func TestReindexHoldForCollection(t *testing.T) {
	tests := []struct {
		name     string
		disabled bool
		holds    map[string]ReindexHold
		query    string
		want     ReindexHold
	}{
		{
			name:  "cleanup hold",
			holds: map[string]ReindexHold{"MyClass": ReindexHoldCleanup},
			query: "MyClass",
			want:  ReindexHoldCleanup,
		},
		{name: "nothing held", query: "MyClass"},
		{
			name:     "feature off",
			disabled: true,
			holds:    map[string]ReindexHold{"MyClass": ReindexHoldCleanup},
			query:    "MyClass",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			for collection, hold := range tt.holds {
				db.reindexHolds.acquire(collection, hold)
			}
			require.Equal(t, tt.want, db.ReindexHoldFor(tt.query))
		})
	}
}

func holdRefusalAfterTerminal(t *testing.T, status distributedtask.TaskStatus) error {
	logger, _ := logrustest.NewNullLogger()
	p := &ReindexProvider{logger: logger, serverCtx: context.Background(), db: &DB{}}
	held := ReindexHoldNone
	// The cleanup logs from inside the hold, so a hook sees what a gate would.
	logger.AddHook(onEachLogLine(func() { held = max(held, p.db.reindexHolds.HoldFor("Movies")) }))
	require.NoError(t, p.OnTaskCompleted(reindexTask("T_terminal", status,
		`{"migrationType":"change-tokenization","collection":"Movies","properties":["title"]}`)))
	require.Equal(t, ReindexHoldCleanup, held, "%s must raise the cleanup hold", status)
	return reindexHoldRefusal("Movies", held)
}

type onEachLogLine func()

func (onEachLogLine) Levels() []logrus.Level     { return logrus.AllLevels }
func (h onEachLogLine) Fire(*logrus.Entry) error { h(); return nil }

func TestStalePartialReindexSweepRaisesTheHold(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{indices: map[string]*Index{indexID("Movies"): {Config: IndexConfig{ClassName: "Movies"}, logger: logger}}}
	held := ReindexHoldNone
	logger.AddHook(onEachLogLine(func() { held = max(held, db.reindexHolds.HoldFor("Movies")) }))
	require.NoError(t, db.NewStalePartialReindexSweep()(context.Background(), "Movies", "title", "searchable"))
	require.Equal(t, ReindexHoldCleanup, held, "the sweep must run inside the hold")
	require.Equal(t, ReindexHoldNone, db.reindexHolds.HoldFor("Movies"), "and must not leave it held")
}

func TestReindexRefusalTexts(t *testing.T) {
	tests := []struct {
		name        string
		refusal     error
		mustContain []string
		mustNotHave []string
	}{
		{
			name:    "a task DTM still lists",
			refusal: reindexLiveTaskRefusal("Movies"),
			mustContain: []string{
				"active runtime-reindex task in DTM",
				"retry after the migration finishes",
				"POST /v1/schema/Movies/properties/<property>/index/<indexType>/cancel",
				"accepted only while the task is STARTED",
			},
		},
		{
			name:    "a stopped task still tearing its files down",
			refusal: holdRefusalAfterTerminal(t, distributedtask.TaskStatusFailed),
			mustContain: []string{
				"runtime-reindex cleanup is still removing its temporary index files",
				"retry once the cleanup finishes",
			},
			mustNotHave: []string{"/cancel", "STARTED", "POST /v1/schema"},
		},
		{
			name:    "a hold kind this build does not know",
			refusal: reindexHoldRefusal("Movies", ReindexHold(99)),
			mustContain: []string{
				"does not recognize",
				"retry once every migration on it has finished",
			},
			mustNotHave: []string{"/cancel", "STARTED", "POST /v1/schema"},
		},
		{
			name:        "no back-reference to ask",
			refusal:     gatedIndex(nil, "Movies").refuseIfReindexInFlight("shard-1"),
			mustContain: []string{"startup window", "retry once the node has finished bootstrapping"},
			mustNotHave: []string{"/cancel", "STARTED", "POST /v1/schema"},
		},
		{
			name:        "no back-reference, from the gate a backup goes through",
			refusal:     gatedIndex(nil, "Movies").refuseIfAnyShardReindexInFlight([]string{"shard-1"}),
			mustContain: []string{"startup window", "retry once the node has finished bootstrapping"},
			mustNotHave: []string{"/cancel", "STARTED", "POST /v1/schema"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.refusal)
			msg := tt.refusal.Error()
			require.ErrorIs(t, tt.refusal, entitiesbackup.ErrBackupBlockedByInFlightReindex,
				"every arm must map to 422")
			var blocked entitiesbackup.ReindexBlockedError
			require.ErrorAs(t, tt.refusal, &blocked)
			require.Equal(t, msg, blocked.Msg)
			require.True(t, strings.HasPrefix(msg,
				entitiesbackup.ErrBackupBlockedByInFlightReindex.Error()),
				"the refusal must lead with the sentinel; got: %s", msg)
			require.Contains(t, msg, `"Movies"`, "every arm names the collection")
			for _, want := range tt.mustContain {
				assert.Contains(t, msg, want)
			}
			for _, unwanted := range tt.mustNotHave {
				assert.NotContains(t, msg, unwanted)
			}
		})
	}
}

func TestReindexRefusalsRedactPlacement(t *testing.T) {
	const (
		shardName = "vT4Kq9LmShardId"
		nodeName  = "node-7"
	)
	db, _, _ := gatedDB(t, gateFixtures{
		live:  map[[2]string]bool{{"Movies", shardName}: true},
		holds: map[string]ReindexHold{"Held": ReindexHoldCleanup},
	})
	refusals := map[string]error{
		"live task": gatedIndex(db, "Movies").refuseIfReindexInFlight(shardName),
		"hold":      gatedIndex(db, "Held").refuseIfReindexInFlight(shardName),
		"many shards": gatedIndex(db, "Movies").
			refuseIfAnyShardReindexInFlight([]string{shardName}),
		"no back-reference": gatedIndex(nil, "Movies").refuseIfReindexInFlight(shardName),
	}
	for name, refusal := range refusals {
		t.Run(name, func(t *testing.T) {
			require.Error(t, refusal)
			var blocked entitiesbackup.ReindexBlockedError
			require.ErrorAs(t, refusal, &blocked,
				"the publishable text must survive to the caller")
			assert.NotContains(t, blocked.Msg, shardName, "a shard id must not reach the caller")
			assert.NotContains(t, blocked.Msg, nodeName, "a node name must not reach the caller")
			assert.NotContains(t, blocked.Msg, `shard "`)
		})
	}
}

func TestReindexRefusalWarnCarriesPlacement(t *testing.T) {
	db, hook, _ := gatedDB(t, gateFixtures{live: map[[2]string]bool{{"Movies", "shard-1"}: true}})
	require.Error(t, gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight([]string{"shard-1"}))
	warned := warnOrAbove(hook)
	require.Len(t, warned, 1, "one refusal, one operator-facing entry")
	assert.Equal(t, "Movies", warned[0].Data["collection"])
	assert.Equal(t, "shard-1", warned[0].Data["shard"])
	assert.Equal(t, "node-7", warned[0].Data["node"])
	assert.Equal(t, reindexReasonLiveTask, warned[0].Data["reason"])
}

func TestReindexRefusalAggregatesWideRefusals(t *testing.T) {
	const shardCount = 60
	live := map[[2]string]bool{}
	shards := make([]string, 0, shardCount)
	for i := range shardCount {
		shard := fmt.Sprintf("shard-%02d", i)
		shards = append(shards, shard)
		live[[2]string{"Movies", shard}] = true
	}

	perShardDB, perShardHook, _ := gatedDB(t, gateFixtures{live: live})
	perShardIdx := gatedIndex(perShardDB, "Movies")
	for _, shard := range shards {
		require.Error(t, perShardIdx.refuseIfReindexInFlight(shard))
	}
	assert.Empty(t, perShardHook.AllEntries(),
		"the per-shard path must stay silent; the aggregating gate carries the refusal")
	db, hook, _ := gatedDB(t, gateFixtures{live: live})
	refusal := gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(shards)
	require.Error(t, refusal)
	require.Equal(t, 1, len(strings.Split(refusal.Error(), "\n")),
		"a refusal covering %d shards must still be one body line", shardCount)
	warned := warnOrAbove(hook)
	require.Len(t, warned, 1, "one refusal, one operator-facing entry")
	entry := warned[0]
	assert.Equal(t, shardCount, entry.Data["blocked_shard_count"],
		"the count must be exact, not the sample size")
	assert.Len(t, entry.Data["blocked_shards"], reindexRefusalSampleLimit,
		"the sample must be capped")
	assert.Nil(t, entry.Data["shard"],
		"the singular field would name one of 60 shards as if it were the one")
}

func TestRefuseIfAnyShardReindexInFlight_ArmSelection(t *testing.T) {
	for _, order := range [][]string{
		{"shard-a", "shard-b"},
		{"shard-b", "shard-a"},
	} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			db, hook, _ := gatedDB(t, gateFixtures{
				live:  map[[2]string]bool{{"Movies", "shard-b"}: true},
				holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup},
			})
			refusal := gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(order)
			require.Error(t, refusal)
			assert.Contains(t, refusal.Error(), "active runtime-reindex task in DTM")
			// The list must name the shard the reason points at, or it sends the
			// operator to shards nothing is happening on.
			warned := warnOrAbove(hook)
			require.Len(t, warned, 1)
			assert.Equal(t, reindexReasonLiveTask, warned[0].Data["reason"])
			assert.Equal(t, []string{"shard-b"}, warned[0].Data["blocked_shards"])
			assert.Equal(t, 1, warned[0].Data["blocked_shard_count"])
		})
	}
}

func TestBackupGateRanksAnUnreadableTaskList(t *testing.T) {
	const (
		undetermined = "could not be read"
		observed     = "has an active runtime-reindex task"
		heldRemoving = "still removing its temporary index files"
	)
	gates := map[string]func(*Index) error{
		"one shard":   func(i *Index) error { return i.refuseIfReindexInFlight("s1") },
		"every shard": func(i *Index) error { return i.refuseIfAnyShardReindexInFlight([]string{"s1", "s2"}) },
	}
	tests := []struct {
		name       string
		live, hold bool
		want       string
	}{
		{name: "nothing observed and the list unreadable", want: undetermined},
		{name: "a hold outranks an unreadable list", hold: true, want: heldRemoving},
		{name: "a live task outranks both", live: true, hold: true, want: observed},
	}
	for gateName, gate := range gates {
		for _, tt := range tests {
			t.Run(gateName+"/"+tt.name, func(t *testing.T) {
				db, _, _ := gatedDB(t, gateFixtures{})
				db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
					return func(string, string) (bool, bool) { return tt.live, true }
				})
				if tt.hold {
					db.reindexHolds.acquire("Movies", ReindexHoldCleanup)
				}
				err := gate(gatedIndex(db, "Movies"))
				require.ErrorContains(t, err, tt.want)
				if tt.want != undetermined {
					return
				}
				require.ErrorIs(t, err, entitiesbackup.ErrBackupReindexActivityUndetermined)
				assert.NotErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
					"a list this node could not read is not a migration it observed")
				assert.NotContains(t, err.Error(), "/cancel",
					"there is no task here to cancel: the node saw none")
			})
		}
	}
}

// Replica movement defers on a gate refusal instead of spending its error budget: fifty
// budgeted errors cancel the movement outright, and waiting is the reversible direction.
func TestIncomingCreateReplicaSnapshotDefersUnderTheReindexGate(t *testing.T) {
	ctx := context.Background()
	index, shard := newSharedHaltTestShard(t)
	held, _, _ := gatedDB(t, gateFixtures{holds: map[string]ReindexHold{"TestClass": ReindexHoldCleanup}})
	index.db = held

	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", "op-1")
	require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp)
	assert.Contains(t, err.Error(), "still removing its temporary index files")
	require.NotErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"restated as text, not chained: only the busy sentinel may be reachable, or an upstream "+
			"gate check reads a deferred movement as a refused backup")

	require.NotErrorIs(t, shard.HaltForTransfer(ctx, false, 0), enterrors.ErrShardBusyStructuralOp,
		"only the movement entry restates the refusal; the shared halt keeps it as it is")

	// An outage defers too: it is the same "not now", and waiting is still reversible.
	index.db, _, _ = gatedDB(t, gateFixtures{})
	index.db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) (bool, bool) { return false, true }
	})
	_, err = index.IncomingCreateReplicaSnapshot(ctx, "shard1", "op-2")
	require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp)
	require.ErrorContains(t, err, entitiesbackup.ErrBackupReindexActivityUndetermined.Error())
	require.NotErrorIs(t, err, entitiesbackup.ErrBackupReindexActivityUndetermined)
}

func TestBackupGateListsTheClusterOncePerCall(t *testing.T) {
	db, _, built := gatedDB(t, gateFixtures{live: map[[2]string]bool{}})
	require.NoError(t, gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight([]string{"s1", "s2", "s3"}))
	require.Equal(t, 1, built.shard,
		"one task-list read per gate call: building per shard reads it N times and lets one call see the cluster two ways")
}

func TestRefuseIfReindexInFlight_HoldRaisedWhileTheClusterIsAsked(t *testing.T) {
	held := func() *Index {
		db, _, _ := gatedDB(t, gateFixtures{})
		db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
			return func(string, string) (bool, bool) {
				db.reindexHolds.acquire("Movies", ReindexHoldCleanup)
				return false, false
			}
		})
		return gatedIndex(db, "Movies")
	}
	const removing = "still removing its temporary index files"
	require.ErrorContains(t, held().refuseIfAnyShardReindexInFlight([]string{"s1", "s2"}), removing)
	require.ErrorContains(t, held().refuseIfReindexInFlight("s1"), removing)
}

func TestRefuseIfReindexInFlight_Allows(t *testing.T) {
	db, hook, _ := gatedDB(t, gateFixtures{live: map[[2]string]bool{}, holds: map[string]ReindexHold{}})
	require.NoError(t, gatedIndex(db, "Movies").refuseIfReindexInFlight("shard-1"))
	require.NoError(t, gatedIndex(db, "Movies").
		refuseIfAnyShardReindexInFlight([]string{"shard-1", "shard-2"}))
	require.Empty(t, hook.AllEntries(), "an admitted backup must log nothing")
}

func TestRefuseIfReindexInFlight_HoldArm(t *testing.T) {
	const shardCount = 50
	shards := make([]string, 0, shardCount)
	for i := range shardCount {
		shards = append(shards, fmt.Sprintf("shard-%02d", i))
	}
	db, hook, _ := gatedDB(t, gateFixtures{
		live:  map[[2]string]bool{},
		holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup},
	})
	err := gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(shards)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
	assert.Contains(t, err.Error(), "still removing its temporary index files")
	warned := warnOrAbove(hook)
	require.Len(t, warned, 1)
	assert.Equal(t, ReindexHoldCleanup.String(), warned[0].Data["reason"])
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
	require.Contains(t, err.Error(), className)

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

// The WARN is where an operator learns the scope, and a hold covers the collection, so
// it must not report the live-task loop's narrower count.
func TestBackupGateHoldRefusalReportsEveryShard(t *testing.T) {
	shards := make([]string, 0, 12)
	for i := range 12 {
		shards = append(shards, fmt.Sprintf("shard-%02d", i))
	}
	db, hook, _ := gatedDB(t, gateFixtures{})
	// Raised while the last shard is asked, so a hold read before or during the loop misses it.
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(_, shardName string) (bool, bool) {
			if shardName == shards[len(shards)-1] {
				db.reindexHolds.acquire("Movies", ReindexHoldCleanup)
			}
			return false, false
		}
	})

	require.ErrorContains(t, gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(shards),
		"still removing its temporary index files")
	warned := warnOrAbove(hook)
	require.Len(t, warned, 1, "one refused call, one line")
	assert.Equal(t, ReindexHoldCleanup.String(), warned[0].Data["reason"])
	assert.Equal(t, len(shards), warned[0].Data["blocked_shard_count"])
	assert.Equal(t, shards[:reindexRefusalSampleLimit], warned[0].Data["blocked_shards"])
}

// One token: an empty ReindexHoldFor means every collection, so a node sweeping one
// would refuse every backup and restore in the cluster.
func TestReindexGatesAreScopedToTheirCollection(t *testing.T) {
	newDB := func() *DB {
		db, _, _ := gatedDB(t, gateFixtures{holds: map[string]ReindexHold{"Other": ReindexHoldCleanup}})
		db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
			return func(string, string) (bool, bool) { return false, false }
		})
		return db
	}
	assert.NoError(t, gatedIndex(newDB(), "Movies").refuseIfReindexInFlight("s1"))
	assert.NoError(t, gatedIndex(newDB(), "Movies").refuseIfAnyShardReindexInFlight([]string{"s1", "s2"}))
	assert.NoError(t, newDB().RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
}

// The gate's wiring, distinct from what it answers: deleting the call left it green.
func TestBackupableConsultsTheReindexGate(t *testing.T) {
	ctx := testCtx()
	const className = "BackupableGated"
	_, idx := testShard(t, ctx, className)
	db, _, _ := gatedDB(t, gateFixtures{})
	db.indices = map[string]*Index{indexID(idx.Config.ClassName): idx}
	idx.db = db
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) (bool, bool) { return false, false }
	})

	require.NoError(t, db.Backupable(ctx, []string{className}), "nothing held, nothing to refuse")
	db.reindexHolds.acquire(className, ReindexHoldCleanup)
	require.ErrorContains(t, db.Backupable(ctx, []string{className}),
		"still removing its temporary index files")
}
