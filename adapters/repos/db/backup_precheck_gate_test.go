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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/schema"
)

// precheckClass is a collection and the local node's shards for it.
type precheckClass struct {
	name   string
	shards []string
}

func precheckShards(className string, n int) []string {
	shards := make([]string, n)
	for i := range shards {
		shards[i] = fmt.Sprintf("%s-shard-%d", className, i)
	}
	return shards
}

// precheckDB builds the minimum DB a Backupable pass walks: one index
// per class, with all shards owned by the local node.
func precheckDB(t *testing.T, classes []precheckClass) *DB {
	t.Helper()

	db := &DB{indices: map[string]*Index{}, localNodeName: "node1"}
	for _, c := range classes {
		db.indices[indexID(schema.ClassName(c.name))] = &Index{
			Config:       IndexConfig{ClassName: schema.ClassName(c.name)},
			db:           db,
			getSchema:    &fakeSchemaGetter{},
			schemaReader: scannableSchemaReader(c.name, c.shards),
		}
	}
	return db
}

func precheckClassNames(classes []precheckClass) []string {
	names := make([]string, len(classes))
	for i, c := range classes {
		names[i] = c.name
	}
	return names
}

// refusalCount counts the refusals a precheck error carries, so a test
// can tell "refused everything" from "refused only the first".
//
// It counts rather than matching shard names because the refusal body
// names no shard: a backup caller has no grant on shard ids, so they go
// to the log instead. Which shard was judged is observed through the
// counting fixture's probe list.
func refusalCount(err error) int {
	if err == nil {
		return 0
	}
	// errors.Join renders one leaf per line, which is also the shape the
	// caller sees. Counting lines avoids mistaking a single error's
	// unwrap list for a join.
	return strings.Count(err.Error(), "\n") + 1
}

// namesAnyShard reports whether a refusal body leaked a shard id.
func namesAnyShard(err error, candidates []string) bool {
	if err == nil {
		return false
	}
	for _, shardName := range candidates {
		if strings.Contains(err.Error(), shardName) {
			return true
		}
	}
	return false
}

// TestBackupable_BuildsReindexLookupOncePerPrecheck pins that a precheck
// builds exactly one DTM snapshot, no matter how many shards or classes.
func TestBackupable_BuildsReindexLookupOncePerPrecheck(t *testing.T) {
	tests := []struct {
		name          string
		classes       []precheckClass
		wantBuilds    int
		wantShardsHit int
	}{
		{
			// Lazy resolution: a pass that reaches no shard must not query.
			name:       "no local shards",
			classes:    []precheckClass{{name: "Empty"}},
			wantBuilds: 0,
		},
		{
			// Guard against over-correcting to zero builds.
			name:          "one shard",
			classes:       []precheckClass{{name: "Single", shards: precheckShards("Single", 1)}},
			wantBuilds:    1,
			wantShardsHit: 1,
		},
		{
			name:          "three shards",
			classes:       []precheckClass{{name: "Few", shards: precheckShards("Few", 3)}},
			wantBuilds:    1,
			wantShardsHit: 3,
		},
		{
			name:          "twelve shards",
			classes:       []precheckClass{{name: "Dozen", shards: precheckShards("Dozen", 12)}},
			wantBuilds:    1,
			wantShardsHit: 12,
		},
		{
			name:          "fifty shards",
			classes:       []precheckClass{{name: "Many", shards: precheckShards("Many", 50)}},
			wantBuilds:    1,
			wantShardsHit: 50,
		},
		{
			// The snapshot is keyed (collection, shard), so it answers
			// for every class in the pass, not just the first.
			name: "three collections",
			classes: []precheckClass{
				{name: "AlphaCls", shards: precheckShards("AlphaCls", 4)},
				{name: "BetaCls", shards: precheckShards("BetaCls", 4)},
				{name: "GammaCls", shards: precheckShards("GammaCls", 4)},
			},
			wantBuilds:    1,
			wantShardsHit: 12,
		},
		{
			name: "collection without local shards alongside a populated one",
			classes: []precheckClass{
				{name: "NoShards"},
				{name: "WithShards", shards: precheckShards("WithShards", 5)},
			},
			wantBuilds:    1,
			wantShardsHit: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := precheckDB(t, tt.classes)
			counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
			counter.install(db)

			require.NoError(t, db.Backupable(testCtx(), precheckClassNames(tt.classes)))

			builds, probed := counter.stats()
			assert.Equal(t, tt.wantBuilds, builds, "DTM snapshots built by one precheck")
			// Anti-vacuity: a fixture whose shards never reach the gate
			// reports zero builds whether or not the pass shares one.
			assert.Len(t, probed, tt.wantShardsHit, "shards the pass actually judged")
		})
	}
}

// TestBackupable_OneGateStillJudgesEachShardByItsOwnName pins that a
// shared snapshot judges each shard by its own name, not a merged verdict.
func TestBackupable_OneGateStillJudgesEachShardByItsOwnName(t *testing.T) {
	const className = "PerShardCls"
	shards := precheckShards(className, 8)
	live := shards[5]

	db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(map[[2]string]bool{
		{className, live}: true,
	})}
	counter.install(db)

	err := db.Backupable(testCtx(), []string{className})

	require.Error(t, err)
	require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
	require.Equal(t, 1, refusalCount(err),
		"only the shard with a live task may be refused")
	require.False(t, namesAnyShard(err, shards), "the body names no shard")
	builds, probed := counter.stats()
	require.Equal(t, 1, builds)

	wantProbed := make([][2]string, len(shards))
	for i, shardName := range shards {
		wantProbed[i] = [2]string{className, shardName}
	}
	require.ElementsMatch(t, wantProbed, probed,
		"a snapshot asked about one name repeatedly cannot tell the shards apart")
}

// TestBackupable_AllShardsJudgedAgainstOneSnapshot pins as deliberate
// that every shard in a pass is judged against the same snapshot, taken
// at the first shard, so the verdict can't change mid-pass.
func TestBackupable_AllShardsJudgedAgainstOneSnapshot(t *testing.T) {
	const className = "CoherentCls"
	shards := precheckShards(className, 4)
	db := precheckDB(t, []precheckClass{{name: className, shards: shards}})

	// First snapshot reports every shard busy, later ones none — this
	// distinguishes "one snapshot per pass" from "one per shard".
	taken := 0
	counter := &countingActivityBuilder{snapshots: func() (ShardReindexActivityLookup, error) {
		taken++
		busy := taken == 1
		return func(string, string) bool { return busy }, nil
	}}
	counter.install(db)

	err := db.Backupable(testCtx(), []string{className})

	require.Error(t, err)
	builds, probed := counter.stats()
	require.Equal(t, 1, builds)
	require.Len(t, probed, len(shards),
		"a later shard judged against a fresh snapshot would have been allowed")
	require.Equal(t, 1, refusalCount(err),
		"one snapshot, one reason, so one line however many shards it covers")
}

// TestBackupable_LiveReindexOnEveryShardRefusesTheWholePass pins that a
// genuine reindex covering the whole node refuses the whole pass, not just
// the first shard. The body names no shard and carries the reason once, so
// what proves every shard was judged is the probe list, not the text.
func TestBackupable_LiveReindexOnEveryShardRefusesTheWholePass(t *testing.T) {
	for _, shardCount := range []int{3, 12} {
		t.Run(fmt.Sprintf("%d shards", shardCount), func(t *testing.T) {
			const className = "FailClosedCls"
			shards := precheckShards(className, shardCount)
			db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
			counter := &countingActivityBuilder{snapshots: func() (ShardReindexActivityLookup, error) {
				return func(string, string) bool { return true }, nil
			}}
			counter.install(db)

			err := db.Backupable(testCtx(), []string{className})

			require.Error(t, err)
			require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
			require.False(t, namesAnyShard(err, shards), "the body names no shard")
			require.Equal(t, 1, refusalCount(err),
				"one reason covers every shard, so the body states it once")
			builds, probed := counter.stats()
			require.Equal(t, 1, builds)
			require.Len(t, probed, len(shards), "every shard must be judged")
		})
	}
}

// TestBackupable_UnwiredLookupAllowsPass pins the fail-open startup
// default: no installed builder means no query and no refusal.
func TestBackupable_UnwiredLookupAllowsPass(t *testing.T) {
	const className = "UnwiredCls"
	db := precheckDB(t, []precheckClass{{name: className, shards: precheckShards(className, 3)}})

	require.NoError(t, db.Backupable(testCtx(), []string{className}))
}

// TestBackupable_IndexWithoutDBRefusesThePass pins that an index without a
// DB back-reference can't consult the gate, so it refuses.
func TestBackupable_IndexWithoutDBRefusesThePass(t *testing.T) {
	const className = "NoBackRefCls"
	shards := precheckShards(className, 3)
	db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
	db.indices[indexID(schema.ClassName(className))].db = nil

	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
	counter.install(db)

	err := db.Backupable(testCtx(), []string{className})

	require.Error(t, err)
	require.Equal(t, 1, refusalCount(err),
		"every shard refuses for the same reason, which the body states once")
	require.Contains(t, err.Error(), "startup window")
	builds, _ := counter.stats()
	require.Equal(t, 0, builds, "an index that cannot consult the gate must not query DTM")
}

// TestBackupable_UnreachableLeaderRefusesOnceWithoutNamingShards pins that
// a failed leader query produces one constant-size refusal, not a naming
// of every shard (a 7 MB body on a 20,000-shard node).
func TestBackupable_UnreachableLeaderRefusesOnceWithoutNamingShards(t *testing.T) {
	leaderErr := errors.New("list DTM tasks: leader not found")

	var bodyLen []int
	for _, shardCount := range []int{3, 50, 301} {
		t.Run(fmt.Sprintf("%d shards", shardCount), func(t *testing.T) {
			const className = "UnknownStateCls"
			shards := precheckShards(className, shardCount)
			db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
			counter := &countingActivityBuilder{snapshots: func() (ShardReindexActivityLookup, error) {
				return nil, leaderErr
			}}
			counter.install(db)

			err := db.Backupable(testCtx(), []string{className})

			require.Error(t, err)
			require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
				"the refusal stays fail-closed and keeps the sentinel")
			require.Equal(t, 1, refusalCount(err),
				"one refusal for the whole node, not one per shard")
			require.False(t, namesAnyShard(err, shards),
				"no shard's state is known, so no shard may be named")
			require.NotContains(t, err.Error(), "active runtime-reindex task in DTM",
				"no reindex is known to exist; claiming one sends operators after a task that may not be there")
			require.NotContains(t, err.Error(), "cancel",
				"there is no task to cancel")
			require.Contains(t, err.Error(), "cluster leader could not be reached")
			require.NotContains(t, err.Error(), entitiesbackup.ErrBackupBlockedByInFlightReindex.Error(),
				"the sentinel's own text claims a reindex is in flight; it must not be rendered anywhere in this refusal")
			require.NotContains(t, err.Error(), leaderErr.Error(),
				"the RAFT-transport cause is log-only")
			require.True(t, errors.Is(err, leaderErr), "but it stays matchable")
			require.NotContains(t, err.Error(), "\n", "one refusal, not one per shard")

			builds, probed := counter.stats()
			require.Equal(t, 1, builds)
			require.Empty(t, probed, "a failed resolution has nothing to judge shards against")
			bodyLen = append(bodyLen, len(err.Error()))
		})
	}
	if len(bodyLen) == 3 {
		require.Equal(t, []int{bodyLen[0], bodyLen[0], bodyLen[0]}, bodyLen,
			"the refusal must not grow with shard count")
	}
}

// TestBackupableRefusalsAreBoundedForLogging runs the real refusals
// through the log bound: the unknown-state error is a type of its own, so
// the bound must be exercised against it and not only against the joined
// form.
//
// The body no longer grows with shard count — one line per reason covers
// them all — so the growth axis that remains is collections: each names
// itself, so each contributes its own line to a multi-collection pass.
func TestBackupableRefusalsAreBoundedForLogging(t *testing.T) {
	const logBoundBytes = 8 << 10

	// Enough collections to clear the bound by an order of magnitude, so the
	// fixture keeps proving the bound is load-bearing even if the wording
	// shortens.
	wideClasses := func() []precheckClass {
		classes := make([]precheckClass, 8000)
		for i := range classes {
			name := fmt.Sprintf("LogBoundCls%d", i)
			classes[i] = precheckClass{name: name, shards: precheckShards(name, 2)}
		}
		return classes
	}

	tests := []struct {
		name    string
		build   func(*testing.T) error
		wantBig bool
	}{
		{
			name: "genuine reindex on every collection",
			build: func(t *testing.T) error {
				classes := wideClasses()
				db := precheckDB(t, classes)
				counter := &countingActivityBuilder{snapshots: func() (ShardReindexActivityLookup, error) {
					return func(string, string) bool { return true }, nil
				}}
				counter.install(db)
				return db.Backupable(testCtx(), precheckClassNames(classes))
			},
			wantBig: true,
		},
		{
			name: "cluster leader unreachable",
			build: func(t *testing.T) error {
				classes := wideClasses()
				db := precheckDB(t, classes)
				counter := &countingActivityBuilder{snapshots: func() (ShardReindexActivityLookup, error) {
					return nil, errors.New("list DTM tasks: leader not found")
				}}
				counter.install(db)
				return db.Backupable(testCtx(), precheckClassNames(classes))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.build(t)
			require.Error(t, err)
			if tt.wantBig {
				require.Greater(t, len(err.Error()), 1<<20,
					"fixture must produce the body the bound exists for")
			}

			bounded := entitiesbackup.ErrorForLog(err)
			require.LessOrEqual(t, len(bounded.Error()), logBoundBytes,
				"what reaches the log must not grow with shard count")
			require.True(t, errors.Is(bounded, entitiesbackup.ErrBackupBlockedByInFlightReindex) ||
				len(bounded.Error()) < len(err.Error()),
				"a bounded refusal is either the original error or a strictly shorter rendering")
		})
	}
}
