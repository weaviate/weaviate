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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestAssignShardsToNodes(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]string
		expected map[string][]string
	}{
		{
			name:     "empty input",
			input:    map[string][]string{},
			expected: map[string][]string{},
		},
		{
			name:  "single shard single node",
			input: map[string][]string{"s1": {"nodeA"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name:  "single shard multiple replicas picks alphabetically first",
			input: map[string][]string{"s1": {"nodeC", "nodeA", "nodeB"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name: "multiple shards same replica set distributes evenly",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "three shards two nodes",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
				"s3": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1", "s3"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "overlapping but different replica sets",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeB", "nodeC"},
				"s3": {"nodeA", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
		{
			name: "uneven replica counts",
			input: map[string][]string{
				"s1": {"nodeA"},
				"s2": {"nodeB"},
				"s3": {"nodeA", "nodeB", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignShardsToNodes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func newTestIndexForSnapshot(t *testing.T, className string) *Index {
	t.Helper()
	return &Index{
		Config: IndexConfig{
			RootPath:  t.TempDir(),
			ClassName: schema.ClassName(className),
		},
		getSchema: &fakeSchemaGetter{
			schema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{{Class: className}},
				},
			},
		},
		logger:           logrus.New(),
		shardCreateLocks: esync.NewKeyRWLocker(),
	}
}

// TestIsAsyncReplicationEnabledOrIrrelevant covers the export gate:
// exportable when RF ≤ 1 AND no shard has a non-terminal replication op
// (irrelevant), or RF > 1 and async replication is not globally disabled.
//
// The non-terminal-op cases pin a real bug class: at RF = 1 with a replica
// movement in flight, BelongsToNodes transiently contains both source and
// target. Returning true here would let the export selector route the
// snapshot to either replica via least-loaded scheduling, capturing whatever
// state happened to land on the picked node (with ASYNC_REPLICATION_DISABLED
// the per-shard hashbeat gate is off too, so there is nothing to repair
// divergence).
func TestIsAsyncReplicationEnabledOrIrrelevant(t *testing.T) {
	disabled := func(v bool) *replication.GlobalConfig {
		return &replication.GlobalConfig{
			AsyncReplicationDisabled: configRuntime.NewDynamicValue(v),
		}
	}

	// shardingState builds a sharding.State whose Physical map keys are the
	// supplied shard names. BelongsToNodes content is irrelevant to the new
	// gate (the FSM is the source of truth for "is a movement in flight"),
	// but we set a stub node so the structure is realistic.
	shardingState := func(shardNames []string) *sharding.State {
		st := &sharding.State{Physical: map[string]sharding.Physical{}}
		for _, name := range shardNames {
			st.Physical[name] = sharding.Physical{
				Name:           name,
				BelongsToNodes: []string{"nodeA"},
			}
		}
		return st
	}

	const className = "TestClass"

	tests := []struct {
		name              string
		replicationFactor int64
		globalConfig      *replication.GlobalConfig
		// shards is the set of physical shard names returned by the schema
		// reader. shardsWithOps is the subset for which the FSM reports a
		// non-terminal op. Both unused when replicationFactor > 1 (the gate
		// short-circuits before consulting either).
		shards        []string
		shardsWithOps map[string]bool
		// readErr, if non-nil, makes the schemaReader fail. Mutually
		// exclusive with shards/shardsWithOps.
		readErr error
		want    bool
	}{
		{
			name:              "RF=1, shards present, FSM reports no ops, globally disabled: exportable (irrelevant)",
			replicationFactor: 1,
			globalConfig:      disabled(true),
			shards:            []string{"s1", "s2"},
			shardsWithOps:     nil,
			want:              true,
		},
		{
			name:              "RF=0, no shards: exportable (irrelevant)",
			replicationFactor: 0,
			globalConfig:      nil,
			shards:            nil,
			want:              true,
		},
		{
			name:              "RF=1, single shard with non-terminal op, globally disabled: NOT exportable (bug repro)",
			replicationFactor: 1,
			globalConfig:      disabled(true),
			shards:            []string{"s1"},
			shardsWithOps:     map[string]bool{"s1": true},
			want:              false,
		},
		{
			name:              "RF=1, single shard with non-terminal op, NOT globally disabled: NOT exportable (no convergence guarantee)",
			replicationFactor: 1,
			globalConfig:      disabled(false),
			shards:            []string{"s1"},
			shardsWithOps:     map[string]bool{"s1": true},
			want:              false,
		},
		{
			name:              "RF=1, schema read fails: NOT exportable (conservative)",
			replicationFactor: 1,
			globalConfig:      nil,
			readErr:           errors.New("schema unavailable"),
			want:              false,
		},
		{
			name:              "RF>1 globally disabled: not exportable (unchanged)",
			replicationFactor: 3,
			globalConfig:      disabled(true),
			want:              false,
		},
		{
			name:              "RF>1 not globally disabled: exportable (unchanged)",
			replicationFactor: 3,
			globalConfig:      disabled(false),
			want:              true,
		},
		{
			name:              "RF>1 nil global config: enabled by default, exportable (unchanged)",
			replicationFactor: 3,
			globalConfig:      nil,
			want:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &Index{
				Config: IndexConfig{
					ReplicationFactor: tt.replicationFactor,
					ClassName:         schema.ClassName(className),
				},
				globalreplicationConfig: tt.globalConfig,
				logger:                  logrus.New(),
			}
			// Only wire schema/FSM readers when the gate will reach for them
			// (RF ≤ 1 path). For RF > 1 we deliberately leave them nil so a
			// regression that newly consults either for RF > 1 surfaces as a
			// nil-pointer panic in test rather than silently passing.
			if tt.replicationFactor <= 1 {
				sr := schemaUC.NewMockSchemaReader(t)
				call := sr.EXPECT().Read(className, true, mock.Anything)
				if tt.readErr != nil {
					call.Return(tt.readErr).Once()
				} else {
					call.RunAndReturn(func(_ string, _ bool, r func(*models.Class, *sharding.State) error) error {
						return r(nil, shardingState(tt.shards))
					}).Once()
				}
				idx.schemaReader = sr

				fsm := replicationTypes.NewMockReplicationFSMReader(t)
				// Set up a permissive expectation: the gate may call
				// HasOngoingReplication 0..N times (short-circuits on
				// the first true). Maybe() keeps the test honest without
				// forcing a specific iteration count.
				fsm.EXPECT().
					HasOngoingReplication(className, mock.Anything).
					RunAndReturn(func(_, shard string) bool {
						return tt.shardsWithOps[shard]
					}).
					Maybe()
				idx.replicationFSMReader = fsm
			}
			assert.Equal(t, tt.want, idx.IsAsyncReplicationEnabledOrIrrelevant())
		})
	}
}

// TestDBIsAsyncReplicationEnabled verifies the DB-level export gate: a missing
// index is not exportable, and an existing index delegates to its own
// IsAsyncReplicationEnabledOrIrrelevant predicate.
func TestDBIsAsyncReplicationEnabled(t *testing.T) {
	const className = "TestClass"

	newDB := func(idx *Index) *DB {
		indices := map[string]*Index{}
		if idx != nil {
			indices[indexID(schema.ClassName(className))] = idx
		}
		return &DB{indices: indices}
	}

	t.Run("index not found: not exportable", func(t *testing.T) {
		db := newDB(nil)
		assert.False(t, db.IsAsyncReplicationEnabled(context.Background(), className))
	})

	// newIndexAtRF1 wires the schema/FSM mocks the new RF≤1 gate consults.
	// shardsWithOps drives the FSM mock: shards in the map (with value true)
	// report a non-terminal replication op.
	newIndexAtRF1 := func(t *testing.T, shardsWithOps map[string]bool) *Index {
		shardNames := []string{"s1"}
		if len(shardsWithOps) > 0 {
			shardNames = make([]string, 0, len(shardsWithOps))
			for name := range shardsWithOps {
				shardNames = append(shardNames, name)
			}
		}
		physical := map[string]sharding.Physical{}
		for _, name := range shardNames {
			physical[name] = sharding.Physical{Name: name, BelongsToNodes: []string{"nodeA"}}
		}

		sr := schemaUC.NewMockSchemaReader(t)
		sr.EXPECT().Read(className, true, mock.Anything).
			RunAndReturn(func(_ string, _ bool, r func(*models.Class, *sharding.State) error) error {
				return r(nil, &sharding.State{Physical: physical})
			}).Once()

		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().
			HasOngoingReplication(className, mock.Anything).
			RunAndReturn(func(_, shard string) bool { return shardsWithOps[shard] }).
			Maybe()

		return &Index{
			Config: IndexConfig{
				ReplicationFactor: 1,
				ClassName:         schema.ClassName(className),
			},
			logger:               logrus.New(),
			schemaReader:         sr,
			replicationFSMReader: fsm,
		}
	}

	t.Run("RF=1 index, no shard has non-terminal ops: exportable (irrelevant)", func(t *testing.T) {
		db := newDB(newIndexAtRF1(t, nil))
		assert.True(t, db.IsAsyncReplicationEnabled(context.Background(), className))
	})

	t.Run("RF=1 index, shard mid-movement: NOT exportable", func(t *testing.T) {
		db := newDB(newIndexAtRF1(t, map[string]bool{"s1": true}))
		assert.False(t, db.IsAsyncReplicationEnabled(context.Background(), className))
	})

	t.Run("RF>1 not globally disabled: exportable", func(t *testing.T) {
		db := newDB(&Index{
			Config: IndexConfig{ReplicationFactor: 3},
			globalreplicationConfig: &replication.GlobalConfig{
				AsyncReplicationDisabled: configRuntime.NewDynamicValue(false),
			},
		})
		assert.True(t, db.IsAsyncReplicationEnabled(context.Background(), className))
	})

	t.Run("RF>1 with async replication globally disabled: not exportable", func(t *testing.T) {
		db := newDB(&Index{
			Config: IndexConfig{ReplicationFactor: 3},
			globalreplicationConfig: &replication.GlobalConfig{
				AsyncReplicationDisabled: configRuntime.NewDynamicValue(true),
			},
		})
		assert.False(t, db.IsAsyncReplicationEnabled(context.Background(), className))
	})
}

// TestSnapshotShardsForExport_EmptyShardNames asserts that passing an empty
// shardNames slice returns immediately with no results and no error. Without
// the early return, the dispatch loop blocks forever on a select with an empty
// retry channel and a done channel that nobody closes.
func TestSnapshotShardsForExport_EmptyShardNames(t *testing.T) {
	idx := newTestIndexForSnapshot(t, "TestClass")

	done := make(chan error, 1)
	go func() {
		res, err := idx.snapshotShardsForExport(context.Background(), nil, "export-id")
		assert.Empty(t, res)
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("snapshotShardsForExport hung on empty shardNames — dispatch loop deadlock")
	}
}

// TestSnapshotShardsForExport_CancelUnblocksLockedShard asserts that a
// write-locked shard doesn't prevent context cancellation from unblocking
// snapshotShardsForExport. The worker polls TryRLock with a select on
// egCtx.Done(), so when the parent ctx is cancelled the function must
// return within a short interval regardless of lock state.
func TestSnapshotShardsForExport_CancelUnblocksLockedShard(t *testing.T) {
	idx := newTestIndexForSnapshot(t, "TestClass")

	// Write-lock the shard so TryRLock always fails.
	shardName := "shard-0"
	idx.shardCreateLocks.Lock(shardName)
	defer idx.shardCreateLocks.Unlock(shardName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := idx.snapshotShardsForExport(ctx, []string{shardName}, "export-id")
		done <- err
	}()
	<-started

	// Confirm the function is actually blocking (worker is in the polling
	// loop) before cancelling — otherwise we could end up testing the
	// dispatcher-exit path instead of the polling-loop-exit path. A few
	// poll intervals are plenty to enter the select on the ticker; if the
	// function returns during this window the test setup is broken.
	select {
	case err := <-done:
		t.Fatalf("snapshotShardsForExport returned before cancellation (lock not actually contended): %v", err)
	case <-time.After(4 * lockPollInterval):
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("snapshotShardsForExport did not respect context cancellation while holding a contended lock")
	}
}
