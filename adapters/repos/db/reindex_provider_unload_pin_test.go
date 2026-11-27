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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// progressProbeRecorder runs onProgress on the first progress report of a unit.
type progressProbeRecorder struct {
	*fakeRecorder
	once       sync.Once
	onProgress func()
}

func (r *progressProbeRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	r.once.Do(r.onProgress)
	return r.fakeRecorder.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress)
}

type reindexPinFixture struct {
	idx     *Index
	lazy    *LazyLoadShard
	p       *ReindexProvider
	payload *ReindexTaskPayload
}

func newReindexPinFixture(t *testing.T, collector bool) *reindexPinFixture {
	t.Helper()
	ctx := testCtx()
	className := "ReindexPin" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	t.Cleanup(func() { require.NoError(t, hot.Shutdown(context.Background())) })
	if collector {
		idx.Config.ShardUnloader = newFakeShardUnloader()
	}
	const name = "pinned-shard"
	lazy := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
		idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer, false, idx.bitmapBufPool)
	idx.shards.Store(name, lazy)
	t.Cleanup(func() {
		if lazy.isLoaded() {
			require.NoError(t, lazy.Shutdown(context.Background()))
		}
	})
	registerIndex(idx, className)
	logger, _ := logrustest.NewNullLogger()
	return &reindexPinFixture{
		idx:  idx,
		lazy: lazy,
		p:    NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx),
		payload: &ReindexTaskPayload{
			Collection: className, MigrationType: ReindexTypeChangeTokenization,
			Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
			UnitToShard: map[string]string{"u1": name},
			UnitToNode:  map[string]string{"u1": "node1"},
		},
	}
}

func (f *reindexPinFixture) observe() (pinned, refused bool) {
	s := f.lazy.currentShard()
	pinned = s != nil && s.inUseCounter.Load() > 0
	refused = pinned && !f.lazy.TryUnload(testCtx(), UnloadRequest{})
	return pinned, refused
}

func TestReindexUnitsAndPhasesPinTheirShardWhileTheCollectorRuns(t *testing.T) {
	runUnit := func(t *testing.T, f *reindexPinFixture, probe func()) {
		rec := &progressProbeRecorder{fakeRecorder: newFakeRecorder(), onProgress: probe}
		f.p.processOneUnit(testCtx(), &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_unit", Version: 1},
			Status: distributedtask.TaskStatusStarted,
		}, f.payload, f.idx, "u1", rec)
	}
	runPhase := func(t *testing.T, f *reindexPinFixture, probe func()) {
		ctx := testCtx()
		desc := distributedtask.TaskDescriptor{ID: "T_phase", Version: 1}
		tasks, err := f.p.createReindexTasks(desc, "u1", f.payload)
		require.NoError(t, err)
		s, _, err := f.lazy.loadIfCold(ctx)
		require.NoError(t, err)
		for _, task := range tasks {
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, s))
		}
		f.p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{desc: {"u1": tasks}})
		raw, err := json.Marshal(f.payload)
		require.NoError(t, err)
		logger, _ := logrustest.NewNullLogger()
		require.NoError(t, f.p.runPerUnitPhase(&distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusSwapping, Payload: raw,
		}, f.payload, []string{"u1"}, f.idx, logger, "probe", "starting", false,
			func(string, ShardLike, []*ShardReindexTaskGeneric, bool) phaseResult {
				probe()
				return phaseResult{}
			}))
	}
	tests := []struct {
		name      string
		run       func(t *testing.T, f *reindexPinFixture, probe func())
		collector bool
	}{
		{name: "unit with the collector", run: runUnit, collector: true},
		{name: "unit without the collector", run: runUnit},
		{name: "phase with the collector", run: runPhase, collector: true},
		{name: "phase without the collector", run: runPhase},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReindexPinFixture(t, tt.collector)
			probed, pinned, refused := false, false, false
			tt.run(t, f, func() {
				probed = true
				pinned, refused = f.observe()
			})

			require.True(t, probed, "the probe has to run inside the unit or phase")
			require.Equal(t, tt.collector, pinned, "the shard is pinned for the scan only while the collector runs")
			require.Equal(t, tt.collector, refused, "a pinned shard refuses an idle unload")
			if tt.collector {
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}), "the pin is released when the scan ends")
			}
		})
	}
}

func TestAReindexUnitOnAShardTearingDownIsRetriedNotFailed(t *testing.T) {
	tests := []struct {
		name string
		flag func(s *Shard) *atomic.Bool
	}{
		{name: "shutting down", flag: func(s *Shard) *atomic.Bool { return &s.shutdownRequested }},
		{name: "being dropped", flag: func(s *Shard) *atomic.Bool { return &s.dropRequested }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReindexPinFixture(t, true)
			s, _, err := f.lazy.loadIfCold(testCtx())
			require.NoError(t, err)
			flag := tt.flag(s)
			flag.Store(true)
			t.Cleanup(func() { flag.Store(false) })

			rec := newFakeRecorder()
			f.p.processOneUnit(testCtx(), &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_teardown", Version: 1},
				Status: distributedtask.TaskStatusStarted,
			}, f.payload, f.idx, "u1", rec)

			require.Empty(t, rec.failed, "a teardown is not a unit failure")
			require.Empty(t, rec.progress, "the unit does not start")
			require.Empty(t, rec.completed)
		})
	}
}
