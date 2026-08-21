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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestIsSemanticMigration pins the semantic/format-only classification
// (weaviate/0-weaviate-issues#254 promoted change-algorithm to semantic).
func TestIsSemanticMigration(t *testing.T) {
	semantic := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeChangeAlgorithm,
	}
	formatOnly := []ReindexMigrationType{
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
		ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable,
	}
	for _, mt := range semantic {
		t.Run(string(mt)+" → semantic", func(t *testing.T) {
			require.True(t, IsSemanticMigration(mt))
		})
	}
	for _, mt := range formatOnly {
		t.Run(string(mt)+" → format-only", func(t *testing.T) {
			require.False(t, IsSemanticMigration(mt))
		})
	}
}

// TestSemanticMigrationIndexTypes pins the migration-type → index-type
// mapping. Format-only migrations (repair-*, enable-rangeable) MUST
// return nil here — they don't go through the swap barrier, so
// LocalCallbacksDone has nothing to check for them.
func TestSemanticMigrationIndexTypes(t *testing.T) {
	tests := []struct {
		name string
		mt   ReindexMigrationType
		want []string
	}{
		{
			name: "change-tokenization → searchable + filterable",
			mt:   ReindexTypeChangeTokenization,
			want: []string{"searchable", "filterable"},
		},
		{
			name: "change-tokenization-filterable → filterable only",
			mt:   ReindexTypeChangeTokenizationFilterable,
			want: []string{"filterable"},
		},
		{
			name: "enable-searchable → searchable",
			mt:   ReindexTypeEnableSearchable,
			want: []string{"searchable"},
		},
		{
			name: "enable-filterable → filterable",
			mt:   ReindexTypeEnableFilterable,
			want: []string{"filterable"},
		},
		{
			name: "change-algorithm → searchable (semantic, cluster-wide flag flip)",
			mt:   ReindexTypeChangeAlgorithm,
			want: []string{"searchable"},
		},
		{
			name: "repair-filterable → empty (format-only)",
			mt:   ReindexTypeRepairFilterable,
			want: nil,
		},
		{
			name: "enable-rangeable → empty (format-only)",
			mt:   ReindexTypeEnableRangeable,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := semanticMigrationIndexTypes(tc.mt)
			require.Equal(t, tc.want, got)
		})
	}
}

// Pins that LocalCallbacksDone, firing at bootstrap with every tenant cold,
// reads the tracker dir at a path it can join without loading the shard.
func TestLocalCallbacksDoneLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
		node   = "n1"
	)

	for _, tc := range []struct {
		name string
		// state plants a record on the cold tenant; empty plants nothing.
		state MigrationState
		// laterState plants a second record on the same property at a higher
		// generation, which is a follow-up migration on the same tuple.
		laterState MigrationState
		// otherProperty makes the planted record name a property this task
		// does not, which is what keeps two tasks on one shard apart.
		otherProperty bool
		// unreadableRecord plants a record file this build cannot place.
		unreadableRecord bool
		// hostedElsewhere maps the unit to a node that is not this one.
		hostedElsewhere bool
		// changeAlgorithm runs the migration whose tracker is class-level, so
		// the probe has to look somewhere the per-property scope never does.
		changeAlgorithm bool
		// absentFromShardMap leaves the shard out of this node's map while
		// the payload still assigns its unit here.
		absentFromShardMap bool
		want               bool
	}{
		{
			name:  "a cold tenant whose swap started and never committed",
			state: MigrationStateIterating,
			want:  false,
		},
		{
			name:  "a cold tenant whose data is committed",
			state: MigrationStateMerged,
			want:  true,
		},
		{
			name:  "a cold tenant whose swap is durable",
			state: MigrationStateSwapped,
			want:  true,
		},
		{
			name: "a cold tenant carrying nothing",
			want: true,
		},
		// A follow-up migration on the same property still owes its own
		// callbacks, whatever became of the one before it.
		{
			name:       "a committed migration beside a later one still rebuilding",
			state:      MigrationStateSwapped,
			laterState: MigrationStateIterating,
			want:       false,
		},
		{
			name:          "an uncommitted migration on a property this task does not name",
			state:         MigrationStateIterating,
			otherProperty: true,
			want:          true,
		},
		// The unreadable record could be the one still owing callbacks, so
		// its silence must not release the bootstrap gate.
		{
			name:             "a record this build cannot read",
			unreadableRecord: true,
			want:             false,
		},
		{
			name:            "an interrupted swap on another node's unit",
			state:           MigrationStateIterating,
			hostedElsewhere: true,
			want:            true,
		},
		{
			name:            "a cold tenant whose class-level blockmax swap started and never committed",
			state:           MigrationStateIterating,
			changeAlgorithm: true,
			want:            false,
		},
		{
			name:            "a cold tenant whose class-level blockmax swap is durable",
			state:           MigrationStateSwapped,
			changeAlgorithm: true,
			want:            true,
		},
		// The unit is assigned here, so the payload's node filter passes and
		// the empty-set early return does not fire. Membership in this node's
		// shard map is what has to reject it.
		{
			name:               "a unit assigned to this node whose shard the map does not hold",
			state:              MigrationStateIterating,
			absentFromShardMap: true,
			want:               true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "LocalCallbacksDone_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())

			migrationType := ReindexTypeChangeTokenization
			trackerDir := postMergeTrackerDir(t, prop)
			laterTrackerDir := migrationDirWithProps(
				migrationDirPrefixesForIndexType("searchable")[0], []string{prop}) + "_2"
			if tc.changeAlgorithm {
				migrationType = ReindexTypeChangeAlgorithm
				trackerDir = MigrationDirSearchableMapToBlockmax + "_1"
				laterTrackerDir = MigrationDirSearchableMapToBlockmax + "_2"
			}
			tenantLSM := shardPathLSM(idx.path(), tenant)
			recordProp := prop
			if tc.otherProperty {
				recordProp = "other"
			}
			if tc.state != "" {
				mkMigrationRecordFor(t, tenantLSM, trackerDir, "T_bootstrap", 1, "u1__n1",
					migrationType, tc.state, recordProp)
			}
			if tc.laterState != "" {
				mkMigrationRecordFor(t, tenantLSM, laterTrackerDir, "T_next", 2, "u1__n1",
					migrationType, tc.laterState, recordProp)
			}
			if tc.unreadableRecord {
				records := filepath.Join(tenantLSM, ".migrations", migrationRecordsDirName)
				require.NoError(t, os.MkdirAll(records, 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(records, "99_enable_searchable.json"), []byte("{"), 0o644))
			}
			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			if !tc.absentFromShardMap {
				idx.shards.Store(tenant, cold)
			}
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			owner := node
			if tc.hostedElsewhere {
				owner = "n2"
			}
			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:    className,
				MigrationType: migrationType,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": tenant},
				UnitToNode:    map[string]string{"u1": owner},
			})
			require.NoError(t, err)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, node, nil, ctx)

			got := p.LocalCallbacksDone(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bootstrap", Version: 1},
				Status:         distributedtask.TaskStatusSwapping,
				Payload:        payload,
			}, node)

			require.Equal(t, tc.want, got)
			require.False(t, cold.isLoaded(),
				"the bootstrap check reads a record at a path this node joins itself; "+
					"loading a tenant to ask it for that path is what startup cannot afford")
		})
	}
}
