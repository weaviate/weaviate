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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func TestInternalShardWorkDoesNotCountAsAccess(t *testing.T) {
	tests := []struct {
		name string
		read func(t *testing.T, f *unloadFixture) error
	}{
		{name: "hashbeat-style loaded pin", read: func(t *testing.T, f *unloadFixture) error {
			_, release, err := f.index.getLoadedShard(f.name)
			release()
			return err
		}},
		{name: "nodes verbose", read: func(t *testing.T, f *unloadFixture) error {
			var status []*models.NodeShardStatus
			_, n, err := f.index.getShardsNodeStatus(testCtx(), &status, "")
			require.EqualValues(t, 1, n)
			return err
		}},
		{name: "loaded shard walk", read: func(t *testing.T, f *unloadFixture) error {
			walked := 0
			require.NoError(t, f.index.withEachLoadedShard(func(string, ShardLike) error { walked++; return nil }))
			require.Equal(t, 1, walked)
			return nil
		}},
		{name: "add property", read: func(t *testing.T, f *unloadFixture) error {
			return f.index.addProperty(testCtx(), textProp("added", true))
		}},
		{name: "status reads", read: func(t *testing.T, f *unloadFixture) error {
			_, _, err := f.index.getShardsStorageStatus(testCtx(), "")
			require.NoError(t, err)
			_, err = f.index.getShardsQueueSize(testCtx(), "")
			require.NoError(t, err)
			_, err = f.index.IncomingGetShardStatus(testCtx(), f.name)
			require.NoError(t, err)
			_, err = f.index.IncomingGetShardQueueSize(testCtx(), f.name)
			return err
		}},
		{name: "backup", read: func(t *testing.T, f *unloadFixture) error {
			backupShard(t, f)
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			require.NotZero(t, AccessClockNow())
			f.lazy.lastAccess.Store(0)

			require.NoError(t, tt.read(t, f))

			require.True(t, f.lazy.isLoaded())
			require.Zero(t, f.lazy.LastAccess())
		})
	}
}

type unloadAtFirstLookup struct {
	fired  atomic.Bool
	unload func()
}

func (e *unloadAtFirstLookup) GetNamespace(name string) (api.Namespace, bool) {
	if e.fired.CompareAndSwap(false, true) {
		e.unload()
	}
	return api.Namespace{Name: name, State: api.NamespaceStateActive}, true
}

func TestBackgroundFindUUIDsNeverReloadAShardUnloadedBeforeItsPin(t *testing.T) {
	tests := []struct {
		name string
		find func(f *unloadFixture, filter *filters.LocalFilter) ([]strfmt.UUID, error)
	}{
		{name: "find uuids", find: func(f *unloadFixture, filter *filters.LocalFilter) ([]strfmt.UUID, error) {
			found, err := f.index.findUUIDs(withBackgroundShardAccess(testCtx()), filter, "", nil, 10)
			return found[f.name], err
		}},
		{name: "incoming find uuids", find: func(f *unloadFixture, filter *filters.LocalFilter) ([]strfmt.UUID, error) {
			return f.index.IncomingFindUUIDs(withBackgroundShardAccess(testCtx()), f.name, filter, 10)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			var outcome bool
			e := &unloadAtFirstLookup{unload: func() { outcome = f.lazy.TryUnload(context.Background(), UnloadRequest{}) }}
			f.index.namespace, f.index.namespacesExister = "alpha", e
			loadsBefore := f.unloader.beforeLoads.Load()

			found, err := tt.find(f, &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On:       &filters.Path{Class: schema.ClassName(f.index.Config.ClassName.String()), Property: filters.InternalPropID},
				Value:    &filters.Value{Value: id.String(), Type: schema.DataTypeText},
			}})
			require.NoError(t, err)
			require.Empty(t, found)

			require.True(t, outcome)
			f.requireUnloaded(t)
			require.Equal(t, loadsBefore, f.unloader.beforeLoads.Load())
		})
	}
}

type schemaReadHook struct {
	schemaUC.SchemaGetter
	fired  atomic.Bool
	onRead func()
}

func (h *schemaReadHook) ReadOnlyClass(name string) *models.Class {
	if h.fired.CompareAndSwap(false, true) {
		h.onRead()
	}
	return h.SchemaGetter.ReadOnlyClass(name)
}

func backupShard(t *testing.T, f *unloadFixture) {
	t.Helper()
	var desc backup.ClassDescriptor
	require.NoError(t, f.index.descriptor(testCtx(), "bak", &desc, nil, nil, newReindexGate(f.index.db, reindexGateMaxAge)))
	require.Len(t, desc.Shards, 1)
	require.NotEmpty(t, desc.Shards[0].Files)
	require.NoError(t, f.index.ReleaseBackup(testCtx(), "bak"))
}

func TestBackgroundLoadsDoNotCountAsAccess(t *testing.T) {
	bg := func() context.Context { return withBackgroundShardAccess(testCtx()) }
	pin := func(t *testing.T, f *unloadFixture, ctx context.Context) {
		_, release, err := f.index.GetShard(ctx, f.name)
		require.NoError(t, err)
		release()
	}
	writePin := func(t *testing.T, f *unloadFixture, ctx context.Context) {
		_, release, err := f.index.getOrInitShard(ctx, f.name)
		require.NoError(t, err)
		release()
	}
	evict := func(t *testing.T, f *unloadFixture) {
		_, ok := f.index.shards.LoadAndDelete(f.name)
		require.True(t, ok)
	}
	republishBeforeInit := func(t *testing.T, f *unloadFixture) {
		evict(t, f)
		prev := f.index.getSchema
		f.index.getSchema = &schemaReadHook{SchemaGetter: prev, onRead: func() { f.index.shards.Store(f.name, f.lazy) }}
		t.Cleanup(func() { f.index.getSchema = prev })
	}
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
		name          string
		collecting    bool
		staysUnloaded bool
		load          func(t *testing.T, f *unloadFixture)
		wantStamped   bool
	}{
		{name: "backup", staysUnloaded: true, load: backupShard},
		{name: "background write pin initializing an absent shard", load: func(t *testing.T, f *unloadFixture) {
			evict(t, f)
			writePin(t, f, bg())
		}},
		{name: "background write pin loading a shard published after its lookup", load: func(t *testing.T, f *unloadFixture) {
			republishBeforeInit(t, f)
			writePin(t, f, bg())
		}},
		{name: "movement enables async replication", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.InitAsyncReplicationOnShard(testCtx(), f.name))
		}},
		{name: "movement reverts async replication", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.RevertAsyncReplicationOnShard(testCtx(), f.name))
		}},
		{name: "movement adds a target node override", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.IncomingAddAsyncReplicationTargetNode(testCtx(), f.name, override(f)))
		}},
		{name: "movement removes a target node override", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.IncomingRemoveAsyncReplicationTargetNode(testCtx(), f.name, override(f)))
		}},
		{name: "movement removes every target node override", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.IncomingRemoveAllAsyncReplicationTargetNodes(testCtx(), f.name))
		}},
		{name: "copy puts a file", collecting: true, load: func(t *testing.T, f *unloadFixture) {
			w, err := f.index.IncomingFilePutter(testCtx(), f.name, "copied.tmp")
			require.NoError(t, err)
			require.NoError(t, w.Close())
		}},
		{name: "write pin initializing an absent shard", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			evict(t, f)
			writePin(t, f, testCtx())
		}},
		{name: "request pin after a background load", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			pin(t, f, bg())
			require.Zero(t, f.lazy.LastAccess())
			pin(t, f, testCtx())
		}},
		{name: "write pin after a background write pin load", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			writePin(t, f, bg())
			require.Zero(t, f.lazy.LastAccess())
			writePin(t, f, testCtx())
		}},
		{name: "request pin waiting on a background load", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			var fired atomic.Bool
			done := make(chan error, 1)
			hook := func(*LazyLoadShard) {
				if fired.CompareAndSwap(false, true) {
					go func() {
						_, release, err := f.index.GetShard(testCtx(), f.name)
						release()
						done <- err
					}()
					time.Sleep(20 * time.Millisecond)
				}
			}
			f.unloader.onBeforeLoad.Store(&hook)
			defer f.unloader.onBeforeLoad.Store(nil)
			pin(t, f, bg())
			require.NoError(t, <-done)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			epoch, prev := accessClockEpoch, accessClock.Load()
			t.Cleanup(func() { accessClockEpoch = epoch; accessClock.Store(prev) })
			accessClockEpoch = epoch.Add(-2000 * time.Second)
			advanceAccessClock()
			f := newUnloadFixture(t)
			f.put(t)
			require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
			f.unloader.idle.Store(!tt.collecting)
			f.lazy.lastAccess.Store(0)
			loadsBefore := f.unloader.beforeLoads.Load()

			tt.load(t, f)

			if tt.staysUnloaded {
				f.requireUnloaded(t)
				require.Equal(t, loadsBefore, f.unloader.beforeLoads.Load())
				require.Zero(t, f.lazy.LastAccess())
				return
			}
			lazy, ok := f.index.shards.Load(f.name).(*LazyLoadShard)
			require.True(t, ok)
			require.True(t, lazy.isLoaded())
			require.Equal(t, tt.wantStamped, lazy.LastAccess() != 0)
			f.unloader.idle.Store(false)
			require.Equal(t, !tt.wantStamped, lazy.TryUnload(testCtx(), UnloadRequest{IdleTTL: 1000 * time.Second}))
		})
	}
}
