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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	shardraft "github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func newTestIndexForRaftReadiness(
	t *testing.T,
	class *models.Class,
	state *sharding.State,
	enableLazyLoad bool,
	raftEnabled bool,
) (*Index, *queue.Scheduler, *schemaUC.MockSchemaGetter) {
	t.Helper()

	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().NodeName().Return("node1").Maybe()

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
			RootPath:               t.TempDir(),
			ReplicationFactor:      1,
			EnableLazyLoadShards:   enableLazyLoad,
			RaftReplicationEnabled: raftEnabled,
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

	return idx, scheduler, mockSchemaGetter
}

func TestNewIndex_RaftEnabledBypassesLazyShardLoading(t *testing.T) {
	class := &models.Class{
		Class:               "RaftLazyBypassClass",
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

	idx, _, _ := newTestIndexForRaftReadiness(t, class, state, true, true)

	loaded := idx.shards.Load("shard1")
	require.NotNil(t, loaded)
	_, isLazy := loaded.(*LazyLoadShard)
	require.False(t, isLazy)
}

func TestNewShard_FailsWhenRaftReplicasLookupFails(t *testing.T) {
	class := &models.Class{
		Class:               "RaftRegistrationClassErr",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}

	state := &sharding.State{Physical: map[string]sharding.Physical{}}
	state.SetLocalName("node1")

	idx, scheduler, mockSchemaGetter := newTestIndexForRaftReadiness(t, class, state, false, false)
	idx.Config.RaftReplicationEnabled = true
	idx.Config.ShardRegistry = &shardraft.Registry{}
	idx.raft = shardraft.NewRaft(shardraft.RaftConfig{
		ClassName: class.Class,
		NodeID:    "node1",
		Logger:    logrus.New(),
	})

	lookupErr := errors.New("replica lookup failed")
	mockSchemaGetter.EXPECT().ShardReplicas(class.Class, "shard1").Return(nil, lookupErr).Once()

	_, err := NewShard(context.Background(), nil, "shard1", idx, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.Error(t, err)
	require.ErrorContains(t, err, "raft shard registration: get replicas")
}

func TestNewShard_FailsWhenRaftReplicasEmpty(t *testing.T) {
	class := &models.Class{
		Class:               "RaftRegistrationClassEmpty",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}

	state := &sharding.State{Physical: map[string]sharding.Physical{}}
	state.SetLocalName("node1")

	idx, scheduler, mockSchemaGetter := newTestIndexForRaftReadiness(t, class, state, false, false)
	idx.Config.RaftReplicationEnabled = true
	idx.Config.ShardRegistry = &shardraft.Registry{}
	idx.raft = shardraft.NewRaft(shardraft.RaftConfig{
		ClassName: class.Class,
		NodeID:    "node1",
		Logger:    logrus.New(),
	})

	mockSchemaGetter.EXPECT().ShardReplicas(class.Class, "shard1").Return([]string{}, nil).Once()

	_, err := NewShard(context.Background(), nil, "shard1", idx, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.Error(t, err)
	require.ErrorContains(t, err, "raft shard registration: no replicas")
}

func TestNewShard_FailsWhenRaftOnShardCreatedFails(t *testing.T) {
	class := &models.Class{
		Class:               "RaftRegistrationClassCreate",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}

	state := &sharding.State{Physical: map[string]sharding.Physical{}}
	state.SetLocalName("node1")

	idx, scheduler, mockSchemaGetter := newTestIndexForRaftReadiness(t, class, state, false, false)
	idx.Config.RaftReplicationEnabled = true
	idx.Config.ShardRegistry = &shardraft.Registry{}
	// Keep raft manager not started so OnShardCreated fails deterministically.
	idx.raft = shardraft.NewRaft(shardraft.RaftConfig{
		ClassName: class.Class,
		NodeID:    "node1",
		Logger:    logrus.New(),
	})

	mockSchemaGetter.EXPECT().ShardReplicas(class.Class, "shard1").Return([]string{"node1"}, nil).Once()

	_, err := NewShard(context.Background(), nil, "shard1", idx, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.Error(t, err)
	require.ErrorContains(t, err, "raft shard registration")
}
