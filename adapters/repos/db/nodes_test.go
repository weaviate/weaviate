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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestGetShardReplicationDetails(t *testing.T) {
	const className = "Repl"

	stateWith := func(physicals ...sharding.Physical) *sharding.State {
		m := make(map[string]sharding.Physical, len(physicals))
		for _, p := range physicals {
			m[p.Name] = p
		}
		return &sharding.State{Physical: m, ReplicationFactor: 3}
	}

	tests := []struct {
		name                  string
		state                 *sharding.State
		shard                 string
		wantNumberOfReplicas  int64
		wantReplicationFactor int64
	}{
		{
			name:                  "shard present in the sharding state",
			state:                 stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{"node1", "node2"}}),
			shard:                 "s1",
			wantNumberOfReplicas:  2,
			wantReplicationFactor: 3,
		},
		{
			name:                  "shard absent from the sharding state",
			state:                 stateWith(sharding.Physical{Name: "other", BelongsToNodes: []string{"node1"}}),
			shard:                 "s1",
			wantNumberOfReplicas:  0,
			wantReplicationFactor: 3,
		},
		{
			// a nil state stands for a class that has left the schema
			name:                  "class already gone from the schema",
			state:                 nil,
			shard:                 "s1",
			wantNumberOfReplicas:  0,
			wantReplicationFactor: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			reader := &retryingSchemaReader{class: &models.Class{Class: className}, state: tt.state}
			idx := newTestIndex(logger, className, reader, nil)

			numberOfReplicas, replicationFactor := getShardReplicationDetails(idx, tt.shard)

			assert.Equal(t, tt.wantNumberOfReplicas, numberOfReplicas, "number of replicas")
			assert.Equal(t, tt.wantReplicationFactor, replicationFactor, "replication factor")
			// a class or shard that has left the schema never comes back by
			// waiting, so neither lookup may spend the retry budget per shard
			assert.Equal(t, 1, reader.reads, "number of schema reads")
		})
	}
}

// TestLocalNodeShardStats pins that a verbose scan leaves indexLock free while
// still holding the scanned index against a drop or a shutdown.
func TestLocalNodeShardStats(t *testing.T) {
	const className = "Slow"

	tests := []struct {
		name           string
		class          string
		shard          string
		extraIndices   int
		withNilIndex   bool
		closeIndex     bool
		wantShards     int
		wantShardCount int64
	}{
		{name: "all classes", class: "", wantShards: 1, wantShardCount: 1},
		{name: "single class", class: className, wantShards: 1, wantShardCount: 1},
		{
			name: "all classes, one index entry missing", class: "",
			withNilIndex: true, wantShards: 1, wantShardCount: 1,
		},
		{
			name: "all classes, counts summed across indices", class: "",
			extraIndices: 2, wantShards: 3, wantShardCount: 3,
		},
		{
			name: "shard filter matches one of many", class: "", shard: "s1",
			extraIndices: 2, wantShards: 1, wantShardCount: 1,
		},
		{
			name: "shard filter matches nothing", class: "", shard: "nosuchshard",
			wantShards: 0, wantShardCount: 0,
		},
		{
			name: "index already shut down", class: className,
			closeIndex: true, wantShards: 0, wantShardCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the scan only reaches the blocking shard when that shard is scanned
			blocking := tt.wantShardCount > 0 && !tt.closeIndex

			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseScan()

			logger, _ := test.NewNullLogger()
			idx := shardedIndex(t, className, "s1", entered, release, blocking)
			if tt.closeIndex {
				idx.closed = true
			}
			db := &DB{logger: logger, indices: map[string]*Index{idx.ID(): idx}}
			if tt.withNilIndex {
				db.indices["gone"] = nil
			}
			for i := 0; i < tt.extraIndices; i++ {
				extra := shardedIndex(t, fmt.Sprintf("Other%d", i), fmt.Sprintf("extra%d", i), nil, nil, false)
				db.indices[extra.ID()] = extra
			}

			var shards []*models.NodeShardStatus
			var stats *models.NodeStats
			done := make(chan struct{})
			go func() {
				defer close(done)
				stats = db.localNodeShardStats(context.Background(), &shards, tt.class, tt.shard)
			}()

			if blocking {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("shard scan never started")
				}

				if db.indexLock.TryLock() {
					db.indexLock.Unlock()
				} else {
					assert.Fail(t, "indexLock must be free while shards are scanned")
				}
				if idx.dropIndex.TryLock() {
					idx.dropIndex.Unlock()
					assert.Fail(t, "the scanned index must be held against a drop")
				}
				releaseScan()
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("shard scan never finished")
			}

			require.NotNil(t, stats)
			require.Len(t, shards, tt.wantShards)
			assert.Equal(t, tt.wantShardCount, stats.ShardCount, "shard count")
			assert.Equal(t, tt.wantShardCount, stats.ObjectCount, "object count")
			for _, shard := range shards {
				assert.Equal(t, int64(1), shard.NumberOfReplicas, "number of replicas")
			}
		})
	}
}

// shardedIndex builds an index holding a single shard reporting one object. When
// blocking, the shard's ObjectCountAsync closes entered and waits for release.
func shardedIndex(t *testing.T, className, shardName string,
	entered, release chan struct{}, blocking bool,
) *Index {
	t.Helper()

	logger, _ := test.NewNullLogger()
	state := &sharding.State{
		Physical:          map[string]sharding.Physical{shardName: {Name: shardName, BelongsToNodes: []string{"node1"}}},
		ReplicationFactor: 1,
	}

	reader := schemaUC.NewMockSchemaReader(t)
	// the false matcher pins that the scan never asks for a retry
	reader.EXPECT().Read(className, false, mock.Anything).
		RunAndReturn(func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(&models.Class{Class: className}, state)
		}).Maybe()

	shard := NewMockShardLike(t)
	shard.EXPECT().Name().Return(shardName).Maybe()
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady).Maybe()
	shard.EXPECT().ForEachVectorQueue(mock.Anything).Return(nil).Maybe()
	shard.EXPECT().ForEachGeoQueue(mock.Anything).Return(nil).Maybe()
	shard.EXPECT().ForEachVectorIndex(mock.Anything).Return(nil).Maybe()
	shard.EXPECT().getAsyncReplicationStats(mock.Anything).Return(nil).Maybe()
	shard.EXPECT().ObjectCountAsync(mock.Anything).RunAndReturn(func(context.Context) (int64, error) {
		if blocking {
			close(entered)
			<-release
		}
		return 1, nil
	}).Maybe()

	return newTestIndex(logger, className, reader, map[string]ShardLike{shardName: shard})
}

// retryingSchemaReader stands in for the real schema reader, reproducing how it
// resolves a class and retries every error the read returns that is not
// permanent. reads counts how often the read actually ran.
type retryingSchemaReader struct {
	schemaUC.SchemaReader
	class *models.Class
	state *sharding.State
	reads int
}

func (r *retryingSchemaReader) Read(_ string, retryIfClassNotFound bool,
	read func(*models.Class, *sharding.State) error,
) error {
	return backoff.Retry(func() error {
		r.reads++
		if r.state == nil {
			if retryIfClassNotFound {
				return clusterSchema.ErrClassNotFound
			}
			return backoff.Permanent(clusterSchema.ErrClassNotFound)
		}
		return read(r.class, r.state)
	}, utils.NewBackoff())
}
