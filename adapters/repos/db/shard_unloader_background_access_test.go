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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestInternalShardWorkDoesNotCountAsAccess(t *testing.T) {
	override := func(f *unloadFixture) additional.AsyncReplicationTargetNodeOverride {
		return additional.AsyncReplicationTargetNodeOverride{
			CollectionID:   f.index.Config.ClassName.String(),
			ShardID:        f.name,
			SourceNode:     f.index.Config.NodeName,
			TargetNode:     "other-node",
			UpperTimeBound: time.Now().Add(time.Hour).UnixMilli(),
		}
	}
	tests := []struct {
		name        string
		read        func(t *testing.T, f *unloadFixture) error
		wantStamped bool
	}{
		{
			name: "request pin",
			read: func(t *testing.T, f *unloadFixture) error {
				_, release, err := f.index.GetShard(testCtx(), f.name)
				release()
				return err
			},
			wantStamped: true,
		},
		{
			name: "shards storage status",
			read: func(t *testing.T, f *unloadFixture) error {
				_, _, err := f.index.getShardsStorageStatus(testCtx(), "")
				return err
			},
		},
		{
			name: "shards queue size",
			read: func(t *testing.T, f *unloadFixture) error {
				_, err := f.index.getShardsQueueSize(testCtx(), "")
				return err
			},
		},
		{
			name: "incoming shard status",
			read: func(t *testing.T, f *unloadFixture) error {
				_, err := f.index.IncomingGetShardStatus(testCtx(), f.name)
				return err
			},
		},
		{
			name: "incoming shard queue size",
			read: func(t *testing.T, f *unloadFixture) error {
				_, err := f.index.IncomingGetShardQueueSize(testCtx(), f.name)
				return err
			},
		},
		{
			name: "movement enables async replication",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.InitAsyncReplicationOnShard(testCtx(), f.name)
			},
		},
		{
			name: "movement reverts async replication",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.RevertAsyncReplicationOnShard(testCtx(), f.name)
			},
		},
		{
			name: "movement adds a target node override",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.IncomingAddAsyncReplicationTargetNode(testCtx(), f.name, override(f))
			},
		},
		{
			name: "movement removes a target node override",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.IncomingRemoveAsyncReplicationTargetNode(testCtx(), f.name, override(f))
			},
		},
		{
			name: "movement removes every target node override",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.IncomingRemoveAllAsyncReplicationTargetNodes(testCtx(), f.name)
			},
		},
		{
			name: "copy puts a file",
			read: func(t *testing.T, f *unloadFixture) error {
				w, err := f.index.IncomingFilePutter(testCtx(), f.name, "copied.tmp")
				if err != nil {
					return err
				}
				return w.Close()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			require.True(t, f.lazy.isLoaded())
			f.lazy.lastAccess.Store(0)

			require.NoError(t, tt.read(t, f))

			require.True(t, f.lazy.isLoaded())
			if tt.wantStamped {
				require.NotZero(t, f.lazy.LastAccess())
			} else {
				require.Zero(t, f.lazy.LastAccess())
			}
		})
	}
}

func idFilter(f *unloadFixture, id string) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On:       &filters.Path{Class: schema.ClassName(f.index.Config.ClassName.String()), Property: filters.InternalPropID},
		Value:    &filters.Value{Value: id, Type: schema.DataTypeText},
	}}
}

type unloadAtLookup struct {
	lookups atomic.Int32
	at      int32
	unload  func()
}

func (e *unloadAtLookup) GetNamespace(name string) (api.Namespace, bool) {
	if e.lookups.Add(1) == e.at {
		e.unload()
	}
	return api.Namespace{Name: name, State: api.NamespaceStateActive}, true
}

func TestBackgroundReadsNeverReloadAShardUnloadedBeforeTheirPin(t *testing.T) {
	tests := []struct {
		name string
		at   int32
		read func(t *testing.T, f *unloadFixture, id string)
	}{
		{
			name: "shards storage status",
			at:   2,
			read: func(t *testing.T, f *unloadFixture, _ string) {
				statuses, _, err := f.index.getShardsStorageStatus(testCtx(), "")
				require.NoError(t, err)
				require.NotEmpty(t, statuses[f.name])
			},
		},
		{
			name: "shards queue size",
			at:   2,
			read: func(t *testing.T, f *unloadFixture, _ string) {
				sizes, err := f.index.getShardsQueueSize(testCtx(), "")
				require.NoError(t, err)
				require.Zero(t, sizes[f.name])
			},
		},
		{
			name: "incoming shard status",
			at:   2,
			read: func(t *testing.T, f *unloadFixture, _ string) {
				status, err := f.index.IncomingGetShardStatus(testCtx(), f.name)
				require.NoError(t, err)
				require.NotEmpty(t, status)
			},
		},
		{
			name: "incoming shard queue size",
			at:   2,
			read: func(t *testing.T, f *unloadFixture, _ string) {
				size, err := f.index.IncomingGetShardQueueSize(testCtx(), f.name)
				require.NoError(t, err)
				require.Zero(t, size)
			},
		},
		{
			name: "find uuids",
			at:   1,
			read: func(t *testing.T, f *unloadFixture, id string) {
				found, err := f.index.findUUIDs(withBackgroundShardAccess(testCtx()), idFilter(f, id), "", nil, 10)
				require.NoError(t, err)
				require.Empty(t, found[f.name])
			},
		},
		{
			name: "incoming find uuids",
			at:   1,
			read: func(t *testing.T, f *unloadFixture, id string) {
				found, err := f.index.IncomingFindUUIDs(withBackgroundShardAccess(testCtx()), f.name, idFilter(f, id), 10)
				require.NoError(t, err)
				require.Empty(t, found)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			require.True(t, f.lazy.isLoaded())
			var outcome bool
			e := &unloadAtLookup{at: tt.at, unload: func() {
				outcome = f.lazy.TryUnload(context.Background(), UnloadRequest{})
			}}
			f.index.namespace, f.index.namespacesExister = "alpha", e
			loadsBefore := f.unloader.beforeLoads.Load()

			tt.read(t, f, id.String())

			require.True(t, outcome)
			f.requireUnloaded(t)
			require.Equal(t, loadsBefore, f.unloader.beforeLoads.Load())
		})
	}
}

func TestLoadedOnlyPinNeverLoads(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	ctx := withLoadedShardsOnly(testCtx())

	shard, release, err := f.index.GetShard(ctx, f.name)
	require.NoError(t, err)
	require.NotNil(t, shard)
	release()

	require.True(t, f.lazy.TryUnload(context.Background(), UnloadRequest{}))
	loadsBefore := f.unloader.beforeLoads.Load()

	_, release, err = f.index.GetShard(ctx, f.name)
	release()
	require.ErrorIs(t, err, errLoadedOnlyShardUnloaded)
	f.requireUnloaded(t)
	require.Equal(t, loadsBefore, f.unloader.beforeLoads.Load())
}
