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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestDeleteIndexWaitsForReindexUnits(t *testing.T) {
	ctx := testCtx()
	className := "DropDrain" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	_, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	registerIndex(idx, className)

	exit, err := enterIndex(idx)
	require.NoError(t, err)
	deleted := make(chan error, 1)
	go func() { deleted <- idx.db.DeleteIndex(entschema.ClassName(className)) }()

	select {
	case err := <-deleted:
		t.Fatalf("the drop ran while a reindex unit still held the index: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.DirExists(t, idx.path(), "the files stay until the unit releases")
	_, err = enterIndex(idx)
	require.Error(t, err, "no unit may start on an index being dropped")

	exit()
	requireDropWithin(t, deleted, 10*time.Second)
	require.NoDirExists(t, idx.path())
}

func TestDropRequestCancelsRunningPhase(t *testing.T) {
	ctx := testCtx()
	className := "DropPhase" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	t.Cleanup(func() { _ = hot.Shutdown(context.Background()) })
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)
	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{"u1": hot.Name()},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_drop", Version: 1}
	tasks, err := p.createReindexTasks(desc, "u1", payload)
	require.NoError(t, err)
	for _, task := range tasks {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, hot))
	}
	p.cacheReindexTasks(desc, "u1", tasks)

	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	finishPhase := func() { finishOnce.Do(func() { close(finish) }) }
	t.Cleanup(finishPhase)
	cause := make(chan error, 1)
	go func() {
		_ = p.runPerUnitPhase(&distributedtask.Task{TaskDescriptor: desc}, payload, []string{"u1"}, idx, logger,
			"swap-requested", "starting", false,
			func(ctx context.Context, _ string, _ ShardLike, _ []*ShardReindexTaskGeneric, _ bool) phaseResult {
				close(entered)
				select {
				case <-ctx.Done():
					cause <- context.Cause(ctx)
				case <-finish:
				}
				// The tail stands in for disk work that ignores the ctx.
				<-finish
				return phaseResult{}
			})
	}()
	<-entered

	deleted := make(chan error, 1)
	go func() { deleted <- idx.db.DeleteIndex(entschema.ClassName(className)) }()
	select {
	case err := <-cause:
		require.ErrorIs(t, err, errIndexDropped)
	case <-time.After(5 * time.Second):
		t.Fatal("the drop request did not cancel the phase")
	}
	select {
	case err := <-deleted:
		t.Fatalf("the drop ran while the phase was still on the index: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.DirExists(t, idx.path())

	finishPhase()
	requireDropWithin(t, deleted, 5*time.Second)
}

func TestStoppedUnitLoadsNoShard(t *testing.T) {
	const (
		prop   = "title"
		node   = "node1"
		tenant = "cold-shard"
	)
	unitID := MigrationUnitID(tenant, node)
	desc := distributedtask.TaskDescriptor{ID: "T_closing", Version: 1}
	started := &distributedtask.Task{Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusStarted}
	cancelled, cancel := context.WithCancel(testCtx())
	cancel()

	for _, tc := range []struct {
		name    string
		closing bool
		run     func(p *ReindexProvider, idx *Index, payload *ReindexTaskPayload, rec *fakeRecorder) error
		wantErr error
	}{
		{
			name: "swap on a dropped collection", closing: true, wantErr: context.Canceled,
			run: func(p *ReindexProvider, _ *Index, payload *ReindexTaskPayload, _ *fakeRecorder) error {
				raw, err := json.Marshal(payload)
				require.NoError(t, err)
				return p.OnSwapRequested(&distributedtask.Task{
					Namespace: ReindexNamespace, TaskDescriptor: desc,
					Status: distributedtask.TaskStatusSwapping, NeedsPreparationBarrier: true, Payload: raw,
					SwapDecidedGroups: map[string]time.Time{"g1": {}},
				}, "g1", []string{unitID})
			},
		},
		{
			name: "scheduler unit on a dropped collection", closing: true,
			run: func(p *ReindexProvider, idx *Index, payload *ReindexTaskPayload, rec *fakeRecorder) error {
				p.processUnits(testCtx(), started, payload, idx, []string{unitID}, rec)
				return nil
			},
		},
		{
			name: "scheduler unit cancelled before its load",
			run: func(p *ReindexProvider, idx *Index, payload *ReindexTaskPayload, rec *fakeRecorder) error {
				p.processOneUnit(cancelled, started, payload, idx, unitID, rec)
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ClosingPhase" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			t.Cleanup(func() { _ = hot.Shutdown(context.Background()) })

			tenantDir := shardPath(idx.path(), tenant)
			mkMigrationRecordFor(t, shardPathLSM(idx.path(), tenant), StrategyCodeSearchableRetokenize, desc.ID, desc.Version, unitID,
				ReindexTypeChangeTokenization, MigrationStateSwapped, prop)
			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(tenant, cold)
			t.Cleanup(func() {
				if cold.isLoaded() {
					_ = cold.Shutdown(context.Background())
				}
			})
			registerIndex(idx, className)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, node, func() int { return 1 }, ctx)
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization,
				Properties: []string{prop}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
				UnitToShard: map[string]string{unitID: tenant},
				UnitToNode:  map[string]string{unitID: node},
			}
			tasks, err := p.createReindexTasks(desc, unitID, payload)
			require.NoError(t, err)
			p.cacheReindexTasks(desc, unitID, tasks[:1])

			if tc.closing {
				idx.signalCloseRequested(errIndexDropped)
			}
			require.NoError(t, os.RemoveAll(tenantDir))
			rec := newFakeRecorder()

			err = tc.run(p, idx, payload, rec)

			require.NoDirExists(t, tenantDir, "a load would recreate the removed tenant's directory")
			require.False(t, cold.isLoaded())
			require.Empty(t, rec.failed, "a stopped unit is not failed")
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestRecoveryClaimSkipsAClosingIndex(t *testing.T) {
	for _, tc := range []struct {
		name        string
		closing     bool
		wantRefusal error
	}{
		{name: "open index", closing: false},
		{name: "index asked to close", closing: true, wantRefusal: errIndexDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			p := structuralInvariantNewBareProvider()
			r := NewMigrationRecoveryReindexer(logger, nil, p)
			idx := newTestIndex(t, logger, "C", nil, nil)
			if tc.closing {
				idx.signalCloseRequested(errIndexDropped)
			}
			task := &ShardReindexTaskGeneric{taskID: "T1", taskVersion: 1, unitID: "u1"}

			claimed, skipped, release, refusal := r.ClaimUnits(&Shard{index: idx}, []*ShardReindexTaskGeneric{task})
			defer release()

			require.ErrorIs(t, refusal, tc.wantRefusal)
			require.Equal(t, tc.closing, len(skipped) == 1)
			require.Equal(t, !tc.closing, len(claimed) == 1)
		})
	}
}

// blockingProgressRecorder holds every non-zero progress write until release closes.
type blockingProgressRecorder struct {
	*fakeRecorder
	unitCtx     context.Context
	blockedOnce sync.Once
	blocked     chan struct{}
	release     chan struct{}
}

func (r *blockingProgressRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string,
	version uint64, nodeID, unitID string, progress float32,
) error {
	if progress == 0 {
		r.unitCtx = ctx
	}
	if progress > 0 {
		r.blockedOnce.Do(func() { close(r.blocked) })
		<-r.release
	}
	return r.fakeRecorder.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress)
}

func TestDropNotHeldByAUnitProgressWrite(t *testing.T) {
	ctx := testCtx()
	className := "DropProgress" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	t.Cleanup(func() { _ = hot.Shutdown(context.Background()) })
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", func() int { return 1 }, ctx)
	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{"u1": hot.Name()},
		UnitToNode:  map[string]string{"u1": "node1"},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_progress", Version: 1}
	tasks, err := p.createReindexTasks(desc, "u1", payload)
	require.NoError(t, err)
	// The loop reports progress once per this many objects, against the flushed object count.
	for range tasks[0].config.checkProcessingEveryNoObjects {
		require.NoError(t, hot.PutObject(ctx, createTestObjectWithText(className, "word")))
	}
	require.NoError(t, hot.Store().FlushMemtables(ctx))

	rec := &blockingProgressRecorder{fakeRecorder: newFakeRecorder(), blocked: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseWrites := func() { releaseOnce.Do(func() { close(rec.release) }) }
	t.Cleanup(releaseWrites)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.processUnits(ctx, &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusStarted,
		}, payload, idx, []string{"u1"}, rec)
	}()
	select {
	case <-rec.blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("the unit never reported progress")
	}

	deleted := make(chan error, 1)
	go func() { deleted <- idx.db.DeleteIndex(entschema.ClassName(className)) }()
	requireDropWithin(t, deleted, 5*time.Second)
	require.ErrorIs(t, context.Cause(rec.unitCtx), errIndexDropped)
	releaseWrites()
	<-done
}

func requireDropWithin(t *testing.T, deleted <-chan error, limit time.Duration) {
	t.Helper()
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(limit):
		t.Fatalf("the drop did not finish within %s", limit)
	}
}
