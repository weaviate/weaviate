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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/reindex"
)

// makeActivityBuilder builds a ShardReindexActivityLookupBuilder that
// reports a fixed set of (collection, shard) pairs as live.
func makeActivityBuilder(live map[[2]string]bool) ShardReindexActivityLookupBuilder {
	return func() (ShardReindexActivityLookup, bool) {
		return func(collection, shardName string) bool {
			return live[[2]string{collection, shardName}]
		}, false
	}
}

// The builder a node produces when its cluster task list cannot be listed.
func unreadableActivityBuilder() ShardReindexActivityLookupBuilder {
	return func() (ShardReindexActivityLookup, bool) { return nil, true }
}

// A nil live or tasks field leaves that lookup uninstalled. Holds are raised
// on the DB's own registry, which production folds case on the way in and on
// the way out of.
type gateFixtures struct {
	live    map[[2]string]bool
	holds   map[string]ReindexHold
	tasks   []*distributedtask.Task
	overlap []*distributedtask.Task
}

type gateCounters struct{ activity, overlap int }

func gatedDB(t *testing.T, f gateFixtures) (*DB, *logrustest.Hook, *gateCounters) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	db := &DB{logger: logger, localNodeName: "node-7"}
	var built gateCounters
	if f.live != nil {
		db.SetShardReindexActivityLookup(makeActivityBuilder(f.live))
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
	if f.overlap != nil {
		db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup {
			built.overlap++
			return NewReindexOverlapLookup(f.overlap, 24*time.Hour, noLocalWorker,
				func() time.Time { return commitTime })
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

// The counter vector itself is unexported by the reindex package, so its
// registry is the only way in.
func gateRefusalCount(t *testing.T, registry *prometheus.Registry, gate, verdict string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "weaviate_reindex_gate_refusals_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["gate"] == gate && labels["verdict"] == verdict {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("the gate wrote no %s/%s series", gate, verdict)
	return 0
}

// The swapped-tuple rows are live with the arguments the other way round,
// so a call site that passed (shardName, collection) reds them.
func TestAnyLiveReindexForShard(t *testing.T) {
	tests := []struct {
		name           string
		live           map[[2]string]bool
		builder        ShardReindexActivityLookupBuilder
		want           bool
		wantUnreadable bool
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
			// Unwired admits: refusing broke every fixture that builds a bare DB.
			// The window is real but short: the cluster listener serves before
			// the install lands.
			name:    "builder never installed",
			builder: nil,
		},
		{name: "builder hands back nothing", builder: func() (ShardReindexActivityLookup, bool) { return nil, false }},
		{
			// Nothing was read, so nothing is live; the caller is told which.
			name:           "the cluster task list could not be read",
			builder:        unreadableActivityBuilder(),
			wantUnreadable: true,
		},
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
			live, unreadable := db.AnyLiveReindexForShard("MyClass", "shard1")
			require.Equal(t, tt.want, live)
			require.Equal(t, tt.wantUnreadable, unreadable)
		})
	}
}

// A cluster whose task list cannot be listed refuses every backup on this
// collection, and an operator repairs that rather than waiting it out, so it
// must not read as the migration verdict.
func TestReindexBackupRefusal_TaskListUnreadable(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	registry := prometheus.NewPedanticRegistry()
	db := &DB{logger: logger, localNodeName: "node-7"}
	db.SetShardReindexActivityLookup(unreadableActivityBuilder())
	db.SetReindexGateMetrics(reindex.NewGateMetrics(registry, nil))

	require.Error(t, gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight([]string{"shard-1"}))

	warned := warnOrAbove(hook)
	require.Len(t, warned, 1, "one refusal, one operator-facing entry")
	assert.Equal(t, reindexReasonTaskListUnreadable, warned[0].Data["reason"])
	assert.Equal(t, 1.0,
		gateRefusalCount(t, registry, reindex.GateBackup, reindex.VerdictTaskListUnreadable))
	assert.Zero(t, gateRefusalCount(t, registry, reindex.GateBackup, reindex.VerdictLiveTask),
		"an unreachable cluster and a running migration are the two states an operator "+
			"most needs to tell apart")
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

// The cleanup hold is raised on FAILED as well as on CANCELLED, so the
// refusal is read off a real terminal run. The e2e drives the cancel; this
// drives the failure.
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

// The cancel handler and the submit-time pre-cleanup run this sweep with no hold
// of their own and no live task left for the other gate. It logs from inside it.
func TestStalePartialReindexSweepRaisesTheHold(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{indices: map[string]*Index{indexID("Movies"): {Config: IndexConfig{ClassName: "Movies"}, logger: logger}}}
	held := ReindexHoldNone
	logger.AddHook(onEachLogLine(func() { held = max(held, db.reindexHolds.HoldFor("Movies")) }))
	require.NoError(t, db.NewStalePartialReindexSweep()(context.Background(), "Movies", "title", "searchable"))
	require.Equal(t, ReindexHoldCleanup, held, "the sweep must run inside the hold")
	require.Equal(t, ReindexHoldNone, db.reindexHolds.HoldFor("Movies"), "and must not leave it held")
}

// The arms are not interchangeable, so each one's wording is pinned
// against the reading that inverts it.
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
			// The task is already terminal, so a cancel has nothing left
			// to stop. Offering one sends an operator at a call that
			// answers NO_OP and leaves them believing they acted.
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
			// The gate DB.Backupable calls. Admit here and a backup captures
			// a collection a migration may be halfway through rewriting.
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

// The other half of the redaction: what the body drops, the log must keep,
// or an operator cannot find the shard the migration is on.
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

// Counting by level rather than by message is deliberate: a per-shard line
// at any level makes the log grow with the collection.
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

// The counter is per operation, so the shard count behind one refused backup
// must not reach it: a sixty-tenant collection refused once counts once.
func TestReindexRefusalCountsOperationsNotShards(t *testing.T) {
	const shardCount = 60
	live := map[[2]string]bool{}
	shards := make([]string, 0, shardCount)
	for i := range shardCount {
		shard := fmt.Sprintf("shard-%02d", i)
		shards = append(shards, shard)
		live[[2]string{"Movies", shard}] = true
	}

	registry := prometheus.NewPedanticRegistry()
	db, _, _ := gatedDB(t, gateFixtures{live: live})
	db.SetReindexGateMetrics(reindex.NewGateMetrics(registry, nil))
	idx := gatedIndex(db, "Movies")

	require.Error(t, idx.refuseIfAnyShardReindexInFlight(shards))
	// The per-shard rung runs inside that same backup, once per shard.
	for _, shard := range shards {
		require.Error(t, idx.refuseIfReindexInFlight(shard))
	}

	assert.Equal(t, 1.0, gateRefusalCount(t, registry, reindex.GateBackup, reindex.VerdictLiveTask),
		"one refused backup is one count, whatever the tenant count behind it")
}

// The transfer's own gate, counted where the transfer is. A replica snapshot is
// one shard and one operation, so it counts once on whichever of its two
// branches runs, and never as a backup.
func TestReindexRefusalCountsAReplicaSnapshotOnce(t *testing.T) {
	tests := []struct {
		name            string
		forceNoHardlink bool
	}{
		{name: "hardlink branch"},
		{name: "halt-for-duration branch", forceNoHardlink: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.forceNoHardlink {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			}
			index, _ := newSharedHaltTestShard(t)
			registry := prometheus.NewPedanticRegistry()
			index.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
				{"TestClass", "shard1"}: true,
			}))
			index.db.SetReindexGateMetrics(reindex.NewGateMetrics(registry, nil))

			_, err := index.IncomingCreateReplicaSnapshot(context.Background(), "shard1",
				"00000000-0000-0000-0000-0000000000f1")
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

			assert.Equal(t, 1.0,
				gateRefusalCount(t, registry, reindex.GateTransfer, reindex.VerdictLiveTask),
				"a refused replica snapshot is one refused operation")
			assert.Zero(t, gateRefusalCount(t, registry, reindex.GateBackup, reindex.VerdictLiveTask),
				"counting a transfer as a backup sends an operator to the wrong runbook")
		})
	}
}

// A migration that starts after admission is caught by the per-shard walk
// instead, and that is still one refused backup of this class.
func TestReindexRefusalCountsALateBackupRefusalOnce(t *testing.T) {
	index, _ := newSharedHaltTestShard(t)
	registry := prometheus.NewPedanticRegistry()
	index.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{"TestClass", "shard1"}: true,
	}))
	index.db.SetReindexGateMetrics(reindex.NewGateMetrics(registry, nil))

	var desc entitiesbackup.ClassDescriptor
	err := index.descriptor(context.Background(), "reindex-late-refusal", &desc, nil)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)

	assert.Equal(t, 1.0, gateRefusalCount(t, registry, reindex.GateBackup, reindex.VerdictLiveTask),
		"a backup refused after admission is one refused operation on this class")
	assert.Zero(t, gateRefusalCount(t, registry, reindex.GateTransfer, reindex.VerdictLiveTask),
		"the backup walk shares a rung with the transfer gate but is not one")
}

// A live task can be ended by the operator; a hold cannot, so the arm that
// offers a remedy has to win regardless of the order the shards come in.
func TestRefuseIfAnyShardReindexInFlight_ArmSelection(t *testing.T) {
	db, _, _ := gatedDB(t, gateFixtures{
		live:  map[[2]string]bool{{"Movies", "shard-b"}: true},
		holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup},
	})
	for _, order := range [][]string{
		{"shard-a", "shard-b"},
		{"shard-b", "shard-a"},
	} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			refusal := gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(order)
			require.Error(t, refusal)
			assert.Contains(t, refusal.Error(), "active runtime-reindex task in DTM")
		})
	}
}

func TestRefuseIfReindexInFlight_Allows(t *testing.T) {
	db, hook, _ := gatedDB(t, gateFixtures{live: map[[2]string]bool{}, holds: map[string]ReindexHold{}})
	require.NoError(t, gatedIndex(db, "Movies").refuseIfReindexInFlight("shard-1"))
	require.NoError(t, gatedIndex(db, "Movies").
		refuseIfAnyShardReindexInFlight([]string{"shard-1", "shard-2"}))
	require.Empty(t, hook.AllEntries(), "an admitted backup must log nothing")
}

// A hold refuses on its own, in the window between a task going terminal
// in DTM and this node finishing its teardown.
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
