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
	"slices"
	"sync"
	"sync/atomic"
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
// still holding the scanned index against a drop, and that a drop requested
// mid-scan aborts it and leaves that index out of the response.
func TestLocalNodeShardStats(t *testing.T) {
	const className = "Slow"

	tests := []struct {
		name           string
		class          string
		shards         []string
		shard          string
		extraIndices   int
		withNilIndex   bool
		closeIndex     bool
		signalDrop     bool
		cancelCaller   bool
		wantShards     int
		wantShardCount int64
		wantScanned    int32
	}{
		{name: "all classes", class: "", wantShards: 1, wantShardCount: 1, wantScanned: 1},
		{name: "single class", class: className, wantShards: 1, wantShardCount: 1, wantScanned: 1},
		{
			name: "all classes, one index entry missing", class: "",
			withNilIndex: true, wantShards: 1, wantShardCount: 1, wantScanned: 1,
		},
		{
			name: "all classes, counts summed across indices", class: "",
			extraIndices: 2, wantShards: 3, wantShardCount: 3, wantScanned: 1,
		},
		{
			name: "shard filter matches one of many", class: "", shard: "s1",
			extraIndices: 2, wantShards: 1, wantShardCount: 1, wantScanned: 1,
		},
		{
			name: "shard filter matches nothing", class: "", shard: "nosuchshard",
			wantShards: 0, wantShardCount: 0, wantScanned: 0,
		},
		{
			name: "index already shut down", class: className,
			closeIndex: true, wantShards: 0, wantShardCount: 0, wantScanned: 0,
		},
		{
			name: "drop requested mid-scan", class: "", shards: []string{"s1", "s2"},
			signalDrop: true, wantShards: 0, wantShardCount: 0, wantScanned: 1,
		},
		{
			name: "drop requested mid-scan keeps the other indices", class: "",
			shards: []string{"s1", "s2"}, signalDrop: true, extraIndices: 2,
			wantShards: 2, wantShardCount: 2, wantScanned: 1,
		},
		{
			name: "drop requested mid-scan with a cancelled caller", class: "",
			shards: []string{"s1", "s2"}, signalDrop: true, cancelCaller: true,
			wantShards: 0, wantShardCount: 0, wantScanned: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardNames := tt.shards
			if shardNames == nil {
				shardNames = []string{"s1"}
			}
			// the scan only blocks once it reaches a shard of the index under test
			blocking := !tt.closeIndex && (tt.shard == "" || slices.Contains(shardNames, tt.shard))

			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseScan()

			logger, _ := test.NewNullLogger()
			idx, scanned := shardedIndex(t, className, shardNames, entered, release, blocking)
			if tt.closeIndex {
				idx.closed = true
			}
			db := &DB{logger: logger, indices: map[string]*Index{idx.ID(): idx}}
			if tt.withNilIndex {
				db.indices["gone"] = nil
			}
			for i := 0; i < tt.extraIndices; i++ {
				extra, _ := shardedIndex(t, fmt.Sprintf("Other%d", i),
					[]string{fmt.Sprintf("extra%d", i)}, nil, nil, false)
				db.indices[extra.ID()] = extra
			}

			callerCtx, cancelCaller := context.WithCancel(context.Background())
			defer cancelCaller()

			var shards []*models.NodeShardStatus
			var stats *models.NodeStats
			done := make(chan struct{})
			go func() {
				defer close(done)
				stats = db.localNodeShardStats(callerCtx, &shards, tt.class, tt.shard)
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
				if tt.cancelCaller {
					cancelCaller()
				}
				// a requested drop must unblock the scan on its own; every other
				// case needs the test to release it
				if tt.signalDrop {
					idx.signalDropRequested()
				} else {
					releaseScan()
				}
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
			assert.Equal(t, tt.wantScanned, scanned.Load(), "shards scanned")
			for _, shard := range shards {
				assert.Equal(t, int64(1), shard.NumberOfReplicas, "number of replicas")
				if tt.signalDrop {
					assert.NotEqual(t, className, shard.Class, "an aborted index must not report shards")
				}
			}
		})
	}
}

// shardedIndex builds an index holding one shard per name, each reporting a
// single object. When blocking, the first shard the scan reaches closes entered
// and waits for release. The counter records how many shards were scanned.
func shardedIndex(t *testing.T, className string, shardNames []string,
	entered, release chan struct{}, blocking bool,
) (*Index, *atomic.Int32) {
	t.Helper()

	logger, _ := test.NewNullLogger()
	physical := make(map[string]sharding.Physical, len(shardNames))
	for _, name := range shardNames {
		physical[name] = sharding.Physical{Name: name, BelongsToNodes: []string{"node1"}}
	}
	state := &sharding.State{Physical: physical, ReplicationFactor: 1}

	reader := schemaUC.NewMockSchemaReader(t)
	// the false matcher pins that the scan never asks for a retry
	reader.EXPECT().Read(className, false, mock.Anything).
		RunAndReturn(func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(&models.Class{Class: className}, state)
		}).Maybe()

	var scanned atomic.Int32
	shards := make(map[string]ShardLike, len(shardNames))
	for _, name := range shardNames {
		shard := NewMockShardLike(t)
		shard.EXPECT().Name().Return(name).Maybe()
		shard.EXPECT().GetStatus().Return(storagestate.StatusReady).Maybe()
		shard.EXPECT().ForEachVectorQueue(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ForEachGeoQueue(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ForEachVectorIndex(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().getAsyncReplicationStats(mock.Anything).Return(nil).Maybe()
		shard.EXPECT().ObjectCountAsync(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
			first := scanned.Add(1) == 1
			if blocking && first {
				close(entered)
				// an aborted scan releases the shard through its context, so a
				// test that never closes release still finishes
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			return 1, nil
		}).Maybe()
		shards[name] = shard
	}

	return newTestIndex(logger, className, reader, shards), &scanned
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
