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
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newPrecheckGateTestDB assembles a DB whose Backupable precheck walks
// collections × shardsPerCollection local shards. The shards exist only in
// the sharding state — the precheck never touches disk, so shard count is
// free and the invocation-count assertions below need no real cluster.
func newPrecheckGateTestDB(t *testing.T, collections, shardsPerCollection int) (*DB, []string) {
	t.Helper()

	logger, _ := test.NewNullLogger()
	db := &DB{
		logger:        logger,
		localNodeName: "node1",
		indices:       map[string]*Index{},
	}

	classNames := make([]string, 0, collections)
	for c := 0; c < collections; c++ {
		className := fmt.Sprintf("PrecheckGateClass%d", c)
		classNames = append(classNames, className)

		shardState := &sharding.State{Physical: map[string]sharding.Physical{}}
		for s := 0; s < shardsPerCollection; s++ {
			shardName := fmt.Sprintf("%s-shard%d", className, s)
			shardState.Physical[shardName] = sharding.Physical{
				Name: shardName,
				// readSchema keeps only shards local to getSchema.NodeName(),
				// which fakeSchemaGetter reports as "node1".
				BelongsToNodes: []string{"node1"},
			}
		}

		reader := schemaUC.NewMockSchemaReader(t)
		reader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(class string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
				return readFunc(&models.Class{Class: class}, shardState)
			}).Maybe()

		db.indices[indexID(schema.ClassName(className))] = &Index{
			db:           db,
			Config:       IndexConfig{ClassName: schema.ClassName(className)},
			getSchema:    &fakeSchemaGetter{shardState: shardState},
			schemaReader: reader,
		}
	}
	return db, classNames
}

// TestBackupable_BuildsReindexLookupOncePerPrecheck pins that one precheck
// builds each lookup exactly once, regardless of shard count.
//
// Build count is the assertable property here: each build costs a
// cluster-wide RAFT query, so counting builds needs no scale to be
// meaningful. Three shards pin the same invariant as three thousand.
func TestBackupable_BuildsReindexLookupOncePerPrecheck(t *testing.T) {
	tests := []struct {
		name                string
		collections         int
		shardsPerCollection int
		dtmUnreachable      bool
		wantBuilds          int64
		wantRefusedShards   int
	}{
		{
			name:        "single collection, single shard",
			collections: 1, shardsPerCollection: 1,
			wantBuilds: 1,
		},
		{
			name:        "single collection, three shards",
			collections: 1, shardsPerCollection: 3,
			wantBuilds: 1,
		},
		{
			name:        "three collections, four shards each",
			collections: 3, shardsPerCollection: 4,
			wantBuilds: 1,
		},
		{
			name:        "many shards, still one build",
			collections: 2, shardsPerCollection: 25,
			wantBuilds: 1,
		},
		{
			name:        "fail-closed: DTM unreachable, three shards",
			collections: 1, shardsPerCollection: 3, dtmUnreachable: true,
			wantBuilds: 1, wantRefusedShards: 3,
		},
		{
			name:        "fail-closed: DTM unreachable, three collections, four shards each",
			collections: 3, shardsPerCollection: 4, dtmUnreachable: true,
			wantBuilds: 1, wantRefusedShards: 12,
		},
		{
			name:        "no classes, no query",
			collections: 0, shardsPerCollection: 0,
			wantBuilds: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, classes := newPrecheckGateTestDB(t, tc.collections, tc.shardsPerCollection)
			totalShards := tc.collections * tc.shardsPerCollection

			var activityBuilds, cleanupBuilds atomic.Int64
			db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
				activityBuilds.Add(1)
				if tc.dtmUnreachable {
					// Mirrors the configure_api.go fail-closed fallback.
					return func(string, string) bool { return true }
				}
				return func(string, string) bool { return false }
			})
			db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
				cleanupBuilds.Add(1)
				return func(string, string) bool { return false }
			})

			err := db.Backupable(context.Background(), classes)

			require.Equalf(t, tc.wantBuilds, activityBuilds.Load(),
				"expected %d ListDistributedTasks lookup build(s) for %d shards, got %d",
				tc.wantBuilds, totalShards, activityBuilds.Load())
			require.Equalf(t, tc.wantBuilds, cleanupBuilds.Load(),
				"expected %d cleanup lookup build(s) for %d shards, got %d",
				tc.wantBuilds, totalShards, cleanupBuilds.Load())

			if tc.wantRefusedShards == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex),
				"fail-closed refusal must wrap the sentinel so the coordinator can classify it")
			require.Equalf(t, tc.wantRefusedShards,
				strings.Count(err.Error(), "active runtime-reindex task in DTM"),
				"a single build must refuse every shard, not just the first: expected %d refusals",
				tc.wantRefusedShards)
		})
	}
}

// TestBackupable_AllShardsJudgedAgainstOneSnapshot pins that every shard in
// one precheck is judged against the same DTM snapshot, not one taken fresh
// per shard.
func TestBackupable_AllShardsJudgedAgainstOneSnapshot(t *testing.T) {
	db, classes := newPrecheckGateTestDB(t, 1, 4)

	var builds atomic.Int64
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		// Alternates per build, standing in for a DTM snapshot that
		// changes between two queries issued moments apart.
		live := builds.Add(1)%2 == 1
		return func(string, string) bool { return live }
	})

	err := db.Backupable(context.Background(), classes)
	require.Error(t, err)
	require.Equal(t, 4, strings.Count(err.Error(), "active runtime-reindex task in DTM"),
		"all four shards must share the verdict of the single snapshot taken for this precheck")
}
