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

// precheckClass is one collection of a Backupable pass and the shards
// the local node owns for it.
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

// precheckDB assembles the minimum DB a Backupable pass walks: one
// index per class, each reading a sharding state whose shards all
// belong to the local node.
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

// blockedShards reports which of candidates a precheck error refused,
// so a test can distinguish "refused everything" from "refused the
// first shard it looked at".
func blockedShards(err error, candidates []string) []string {
	if err == nil {
		return nil
	}
	var blocked []string
	for _, shardName := range candidates {
		if strings.Contains(err.Error(), fmt.Sprintf("shard %q", shardName)) {
			blocked = append(blocked, shardName)
		}
	}
	return blocked
}

// TestBackupable_BuildsReindexLookupOncePerPrecheck pins the cost of a
// precheck: one DTM snapshot for the whole pass, however many
// collections and shards it walks. Building one per shard turned a
// backup start into one cluster-wide query per shard.
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

			assert.Equal(t, tt.wantBuilds, counter.builds, "DTM snapshots built by one precheck")
			// Anti-vacuity: a fixture whose shards never reach the gate
			// reports zero builds whether or not the pass shares one.
			assert.Len(t, counter.probed, tt.wantShardsHit, "shards the pass actually judged")
		})
	}
}

// TestBackupable_OneGateStillJudgesEachShardByItsOwnName pins that
// sharing a snapshot does not merge the shards' verdicts: each shard is
// still asked about by its own name.
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
	require.Equal(t, []string{live}, blockedShards(err, shards),
		"only the shard with a live task may be refused")
	require.Equal(t, 1, counter.builds)

	wantProbed := make([][2]string, len(shards))
	for i, shardName := range shards {
		wantProbed[i] = [2]string{className, shardName}
	}
	require.ElementsMatch(t, wantProbed, counter.probed,
		"a snapshot asked about one name repeatedly cannot tell the shards apart")
}

// TestBackupable_AllShardsJudgedAgainstOneSnapshot pins the deliberate
// narrowing the hoist introduces: every shard of a pass is judged
// against the snapshot taken when the pass reached its first shard, so
// the verdict cannot change halfway through.
func TestBackupable_AllShardsJudgedAgainstOneSnapshot(t *testing.T) {
	const className = "CoherentCls"
	shards := precheckShards(className, 4)
	db := precheckDB(t, []precheckClass{{name: className, shards: shards}})

	// The first snapshot reports every shard busy, later ones report
	// none: one snapshot per pass refuses all four, one per shard
	// refuses only the shard that happened to be checked first.
	taken := 0
	counter := &countingActivityBuilder{snapshots: func() ShardReindexActivityLookup {
		taken++
		busy := taken == 1
		return func(string, string) bool { return busy }
	}}
	counter.install(db)

	err := db.Backupable(testCtx(), []string{className})

	require.Error(t, err)
	require.ElementsMatch(t, shards, blockedShards(err, shards))
	require.Equal(t, 1, counter.builds)
}

// TestBackupable_UnreachableTaskManagerRefusesEveryShard pins the
// fail-closed default: when the DTM query fails, its snapshot reports
// every shard busy, and sharing that snapshot must refuse the whole
// pass rather than only its first shard.
func TestBackupable_UnreachableTaskManagerRefusesEveryShard(t *testing.T) {
	for _, shardCount := range []int{3, 12} {
		t.Run(fmt.Sprintf("%d shards", shardCount), func(t *testing.T) {
			const className = "FailClosedCls"
			shards := precheckShards(className, shardCount)
			db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
			counter := &countingActivityBuilder{snapshots: func() ShardReindexActivityLookup {
				return func(string, string) bool { return true }
			}}
			counter.install(db)

			err := db.Backupable(testCtx(), []string{className})

			require.Error(t, err)
			require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex))
			require.ElementsMatch(t, shards, blockedShards(err, shards))
			require.Equal(t, 1, counter.builds)
		})
	}
}

// TestBackupable_UnwiredLookupAllowsPass pins that the shared gate
// keeps the fail-open startup default: no installed builder means no
// query and no refusal.
func TestBackupable_UnwiredLookupAllowsPass(t *testing.T) {
	const className = "UnwiredCls"
	db := precheckDB(t, []precheckClass{{name: className, shards: precheckShards(className, 3)}})

	require.NoError(t, db.Backupable(testCtx(), []string{className}))
}

// TestBackupable_IndexWithoutDBRefusesEveryShard pins that an index
// carrying no DB back-reference stays conservative under the shared
// gate: it cannot consult the snapshot, so it refuses.
func TestBackupable_IndexWithoutDBRefusesEveryShard(t *testing.T) {
	const className = "NoBackRefCls"
	shards := precheckShards(className, 3)
	db := precheckDB(t, []precheckClass{{name: className, shards: shards}})
	db.indices[indexID(schema.ClassName(className))].db = nil

	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
	counter.install(db)

	err := db.Backupable(testCtx(), []string{className})

	require.Error(t, err)
	require.ElementsMatch(t, shards, blockedShards(err, shards))
	require.Contains(t, err.Error(), "startup window")
	require.Equal(t, 0, counter.builds, "an index that cannot consult the gate must not query DTM")
}
