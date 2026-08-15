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

func makeHoldBuilder(holds map[string]ReindexHold) ReindexHoldLookupBuilder {
	return func() ReindexHoldLookup {
		return func(collections []string) ReindexHold {
			if len(collections) == 0 {
				strongest := ReindexHoldNone
				for _, hold := range holds {
					strongest = max(strongest, hold)
				}
				return strongest
			}
			strongest := ReindexHoldNone
			for _, collection := range collections {
				strongest = max(strongest, holds[collection])
			}
			return strongest
		}
	}
}

// A nil live or tasks field leaves that lookup uninstalled. The hold lookup
// is always installed, holding nothing when the fixture names none: an
// uninstalled one warns, and the tests that want that build their own DB.
type gateFixtures struct {
	live  map[[2]string]bool
	holds map[string]ReindexHold
	tasks []*distributedtask.Task
}

type gateCounters struct{ activity, hold int }

func gatedDB(t *testing.T, f gateFixtures) (*DB, *logrustest.Hook, *gateCounters) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	db := &DB{logger: logger, localNodeName: "node-7"}
	var built gateCounters
	if f.live != nil {
		db.SetShardReindexActivityLookup(makeActivityBuilder(f.live))
	}
	holdBuilder := makeHoldBuilder(f.holds)
	db.SetReindexHoldLookup(func() ReindexHoldLookup {
		built.hold++
		return holdBuilder()
	})
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

// The swapped-tuple rows are live with the arguments the other way round,
// so a call site that passed (shardName, collection) reds them.
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
			// Unwired admits: the install lands before the server serves,
			// and refusing broke every fixture that builds a bare DB.
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
			require.Equal(t, tt.want, db.AnyLiveReindexForShard("MyClass", "shard1"))
		})
	}
}

func TestReindexHoldForCollection(t *testing.T) {
	tests := []struct {
		name      string
		disabled  bool
		holds     map[string]ReindexHold
		builder   ReindexHoldLookupBuilder
		query     string
		want      ReindexHold
		wantBuilt int
	}{
		{
			name:      "cleanup hold",
			holds:     map[string]ReindexHold{"MyClass": ReindexHoldCleanup},
			query:     "MyClass",
			want:      ReindexHoldCleanup,
			wantBuilt: 1,
		},
		{
			name:     "feature off",
			disabled: true,
			holds:    map[string]ReindexHold{"MyClass": ReindexHoldCleanup},
			query:    "MyClass",
		},
		{name: "builder never installed", query: "MyClass", builder: nil},
		{name: "builder hands back nothing", query: "MyClass", builder: func() ReindexHoldLookup { return nil }, wantBuilt: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			built := 0
			counted := func(builder ReindexHoldLookupBuilder) ReindexHoldLookupBuilder {
				return func() ReindexHoldLookup { built++; return builder() }
			}
			switch {
			case tt.holds != nil:
				db.SetReindexHoldLookup(counted(makeHoldBuilder(tt.holds)))
			case tt.builder != nil:
				db.SetReindexHoldLookup(counted(tt.builder))
			}
			require.Equal(t, tt.want, db.ReindexHoldFor(tt.query))
			require.Equal(t, tt.wantBuilt, built,
				"the flag must be read before any lookup is built")
		})
	}
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
			name:    "a cancelled task still tearing its files down",
			refusal: reindexHoldRefusal("Movies", ReindexHoldCleanup),
			mustContain: []string{
				"a cancelled migration is still removing its temporary index files",
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
			name:    "no back-reference to ask",
			refusal: reindexStartupWindowRefusal("Movies"),
			mustContain: []string{
				"startup window",
				"retry once the node has finished bootstrapping",
			},
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
		"no back-reference": (&Index{
			Config: IndexConfig{ClassName: schema.ClassName("Movies")},
		}).refuseIfReindexInFlight(shardName),
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
	db, hook, built := gatedDB(t, gateFixtures{
		live:  map[[2]string]bool{},
		holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup},
	})
	err := gatedIndex(db, "Movies").refuseIfAnyShardReindexInFlight(shards)
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
	assert.Contains(t, err.Error(), "still removing its temporary index files")
	assert.Equal(t, 1, built.hold, "one hold read for the whole collection")
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
