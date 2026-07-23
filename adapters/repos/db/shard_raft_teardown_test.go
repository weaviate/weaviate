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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	shardraft "github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type raftTeardownAddrResolver struct{}

func (raftTeardownAddrResolver) NodeAddress(string) string { return "127.0.0.1" }

// newTestIndexWithShardRaft builds a raft-enabled Index wired to a real,
// started shard-raft Registry (single node, ephemeral port). Returns the
// index, the registry, and the data dir holding the shared raft log and
// snapshot tree.
func newTestIndexWithShardRaft(
	t *testing.T,
	class *models.Class,
	state *sharding.State,
) (*Index, *shardraft.Registry, string) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	dataDir := t.TempDir()

	reg := shardraft.NewRegistry(shardraft.RegistryConfig{
		NodeID:                 "node1",
		Logger:                 logger,
		AddressResolver:        raftTeardownAddrResolver{},
		DataPath:               dataDir,
		RaftPort:               0,
		HeartbeatTimeout:       50 * time.Millisecond,
		ElectionTimeout:        200 * time.Millisecond,
		MaxConcurrentSnapshots: 1,
	})
	require.NoError(t, reg.Start())
	t.Cleanup(func() { _ = reg.Shutdown() })

	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().NodeName().Return("node1").Maybe()
	for name := range state.Physical {
		mockSchemaGetter.EXPECT().ShardReplicas(class.Class, name).
			Return([]string{"node1"}, nil).Maybe()
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().
		Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(className string, retryIfClassNotFound bool, readFn func(*models.Class, *sharding.State) error) error {
			return readFn(class, state)
		}).
		Maybe()

	shardResolver := resolver.NewShardResolver(class.Class, false, mockSchemaGetter)
	idx, err := NewIndex(
		context.Background(),
		IndexConfig{
			ClassName:              schema.ClassName(class.Class),
			RootPath:               dataDir,
			ReplicationFactor:      1,
			RaftReplicationEnabled: true,
			ShardRegistry:          reg,
			ShardLoadLimiter:       loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		},
		inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(),
		nil,
		nil,
		shardResolver,
		mockSchemaGetter,
		mockSchemaReader,
		nil,
		logger,
		nil,
		nil,
		nil,
		nil,
		nil,
		class,
		nil,
		scheduler,
		nil,
		nil,
		NewShardReindexerV3Noop(),
		roaringset.NewBitmapBufPoolNoop(),
		false,
		nil,
	)
	require.NoError(t, err)

	// Post-drop this errors (index already closed) — ignored on purpose; it
	// only backstops early test failures.
	t.Cleanup(func() { _ = idx.Shutdown(context.Background()) })

	return idx, reg, dataDir
}

// TestIndexDrop_TearsDownRaftGroups pins the full raft teardown contract for
// the two drop journeys: after a drop the store must be unreachable, inbound
// messages for its group must not resurrect anything, and the group's
// persisted state (shared log + snapshot dir) must be purged so a restart or
// same-name re-creation cannot revive a dead group.
func TestIndexDrop_TearsDownRaftGroups(t *testing.T) {
	tests := []struct {
		name string
		drop func(t *testing.T, idx *Index)
		// wantManagerGone: the class-level Raft manager itself is removed
		// (whole-index drop) vs kept (shard-scoped drop, class still exists).
		wantManagerGone bool
	}{
		{
			name:            "class drop removes manager and purges group state",
			drop:            func(t *testing.T, idx *Index) { require.NoError(t, idx.drop()) },
			wantManagerGone: true,
		},
		{
			name: "shard drop purges group state, keeps class manager",
			drop: func(t *testing.T, idx *Index) {
				require.NoError(t, idx.dropShards([]string{"shard1"}))
			},
			wantManagerGone: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class:               "RaftTeardownClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			}
			state := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         models.TenantActivityStatusHOT,
					},
				},
			}
			state.SetLocalName("node1")

			idx, reg, dataDir := newTestIndexWithShardRaft(t, class, state)

			store := reg.GetStore(class.Class, "shard1")
			require.NotNil(t, store, "raft store must exist before drop")
			gid := store.GroupID()

			// Leadership implies the group's HardState reached the shared
			// log: processReady persists before surfacing SoftState changes,
			// so the purge assertions below cannot pass vacuously.
			require.Eventually(t, store.IsLeader, 5*time.Second, 10*time.Millisecond,
				"single-voter group must elect itself")

			tt.drop(t, idx)

			assert.Nil(t, reg.GetStore(class.Class, "shard1"),
				"store must be unreachable after drop")
			if tt.wantManagerGone {
				assert.Nil(t, reg.GetRaft(class.Class),
					"class raft manager must be removed on index drop")
			} else {
				assert.NotNil(t, reg.GetRaft(class.Class),
					"class raft manager must survive a shard-scoped drop")
			}

			// A routed message for the dropped group must be dropped silently,
			// never resurrect a store.
			require.NoError(t, reg.RouteMessage(gid, raftpb.Message{Type: raftpb.MsgHeartbeat}))
			assert.Nil(t, reg.GetStore(class.Class, "shard1"),
				"routed message must not resurrect the dropped group")

			// Release the registry's bbolt lock, then inspect persisted state.
			require.NoError(t, reg.Shutdown())

			quiet := logrus.New()
			quiet.SetLevel(logrus.WarnLevel)
			sl, err := sharedlog.Open(sharedlog.Options{
				Path:   filepath.Join(dataDir, "shard-raft-log.db"),
				Logger: quiet,
			})
			require.NoError(t, err)
			defer sl.Close()

			has, err := sl.HasGroup(gid)
			require.NoError(t, err)
			assert.False(t, has,
				"persisted raft state for group %d must be purged on drop", gid)

			_, statErr := os.Stat(filepath.Join(dataDir, "raft-snapshots", class.Class, "shard1"))
			assert.True(t, os.IsNotExist(statErr),
				"snapshot dir must be removed on drop")
		})
	}
}
