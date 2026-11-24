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
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
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
			name: "write pin",
			read: func(t *testing.T, f *unloadFixture) error {
				_, release, err := f.index.getOrInitShard(testCtx(), f.name)
				release()
				return err
			},
			wantStamped: true,
		},
		{
			name: "hashbeat-style loaded pin",
			read: func(t *testing.T, f *unloadFixture) error {
				_, release, err := f.index.getLoadedShard(f.name)
				release()
				return err
			},
		},
		{
			name: "nodes verbose",
			read: func(t *testing.T, f *unloadFixture) error {
				var status []*models.NodeShardStatus
				_, n, err := f.index.getShardsNodeStatus(testCtx(), &status, "")
				require.NoError(t, err)
				require.EqualValues(t, 1, n)
				require.True(t, status[0].Loaded)
				return nil
			},
		},
		{
			name: "loaded shard walk",
			read: func(t *testing.T, f *unloadFixture) error {
				walked := 0
				require.NoError(t, f.index.withEachLoadedShard(func(string, ShardLike) error {
					walked++
					return nil
				}))
				require.Equal(t, 1, walked)
				return nil
			},
		},
		{
			name: "add property",
			read: func(t *testing.T, f *unloadFixture) error {
				return f.index.addProperty(testCtx(), textProp("added", true))
			},
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
		{
			name: "backup",
			read: func(t *testing.T, f *unloadFixture) error {
				backupShard(t, f)
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			require.True(t, f.lazy.isLoaded())
			require.NotZero(t, AccessClockNow())
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
			at:   1,
			read: func(t *testing.T, f *unloadFixture, _ string) {
				status, err := f.index.IncomingGetShardStatus(testCtx(), f.name)
				require.NoError(t, err)
				require.NotEmpty(t, status)
			},
		},
		{
			name: "incoming shard queue size",
			at:   1,
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
		{name: "background pin", load: func(t *testing.T, f *unloadFixture) { pin(t, f, bg()) }},
		{name: "background write pin", load: func(t *testing.T, f *unloadFixture) { writePin(t, f, bg()) }},
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
		{name: "request pin", wantStamped: true, load: func(t *testing.T, f *unloadFixture) { pin(t, f, testCtx()) }},
		{name: "write pin initializing an absent shard", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			evict(t, f)
			writePin(t, f, testCtx())
		}},
		{name: "write pin loading a shard published after its lookup", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			republishBeforeInit(t, f)
			writePin(t, f, testCtx())
		}},
		{name: "request pin after a background load", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			pin(t, f, bg())
			require.Zero(t, f.lazy.LastAccess())
			pin(t, f, testCtx())
		}},
		{name: "write pin after a background load", wantStamped: true, load: func(t *testing.T, f *unloadFixture) {
			pin(t, f, bg())
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
