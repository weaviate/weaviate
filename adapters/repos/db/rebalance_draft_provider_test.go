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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication/types"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestRebalanceDraftProvider(t *testing.T) {
	const localNode = "node-1"

	newTask := func(localStatus distributedtask.UnitStatus, localNodeID string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace:      types.RebalanceDraftNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "plan-1", Version: 3},
			Status:         distributedtask.TaskStatusStarted,
			Units: map[string]*distributedtask.Unit{
				"node-1": {ID: "node-1", NodeID: localNodeID, Status: localStatus},
				"node-2": {ID: "node-2", Status: distributedtask.UnitStatusPending},
			},
		}
	}
	newProvider := func(t *testing.T, sizer *draftTestSizer, classes ...string) (*RebalanceDraftProvider, *draftTestRecorder) {
		logger, _ := test.NewNullLogger()
		p := NewRebalanceDraftProvider(sizer, func() []*models.Class {
			out := make([]*models.Class, 0, len(classes))
			for _, c := range classes {
				out = append(out, &models.Class{Class: c})
			}
			return out
		}, t.TempDir(), localNode, logger, context.Background())
		rec := &draftTestRecorder{}
		p.SetCompletionRecorder(rec)
		return p, rec
	}
	waitDone := func(t *testing.T, h distributedtask.TaskHandle) {
		t.Helper()
		select {
		case <-h.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("task handle never closed Done")
		}
	}
	draftPath := func(p *RebalanceDraftProvider) string {
		return filepath.Join(p.dir, "plan-1-3.json")
	}
	readDraft := func(t *testing.T, path string) rebalanceDraft {
		t.Helper()
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		var d rebalanceDraft
		require.NoError(t, json.Unmarshal(content, &d))
		return d
	}

	t.Run("drafts the local unit only", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			task *distributedtask.Task
		}{
			{name: "node not listed", task: func() *distributedtask.Task {
				task := newTask(distributedtask.UnitStatusPending, "")
				delete(task.Units, localNode)
				return task
			}()},
			{name: "local unit completed", task: newTask(distributedtask.UnitStatusCompleted, localNode)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sizer := &draftTestSizer{}
				p, rec := newProvider(t, sizer, "A")

				h, err := p.StartTask(tc.task)
				require.NoError(t, err)
				select {
				case <-h.Done():
				default:
					t.Fatal("handle is not already done")
				}
				h.Terminate()

				assert.Empty(t, rec.snapshot())
				assert.Zero(t, sizer.calls.Load())
				assert.NoFileExists(t, draftPath(p))
			})
		}
	})

	t.Run("persists then reports", func(t *testing.T) {
		sizer := &draftTestSizer{usage: map[string]*usagetypes.CollectionUsage{
			"A": {Shards: usagetypes.ShardsUsage{
				{Name: "a1", FullShardStorageBytes: 100},
				{Name: "a2", FullShardStorageBytes: 200},
			}},
			"B": {Shards: usagetypes.ShardsUsage{{Name: "b1", FullShardStorageBytes: 300}}},
		}, errs: map[string]error{
			"Deleted": fmt.Errorf("schemareader: %w", clusterSchema.ErrClassNotFound),
		}}
		// "NoIndex" has no canned usage, so the sizer returns nil. UsageForIndex
		// returns nil for a collection with no local index.
		p, rec := newProvider(t, sizer, "A", "B", "Deleted", "NoIndex")
		var (
			contentAtCompletion []byte
			readErr             error
		)
		rec.onCompletion = func() { contentAtCompletion, readErr = os.ReadFile(draftPath(p)) }

		h, err := p.StartTask(newTask(distributedtask.UnitStatusPending, ""))
		require.NoError(t, err)
		waitDone(t, h)

		assert.Equal(t, []draftTestCall{
			{kind: "progress", unit: localNode, progress: 0},
			{kind: "completion", unit: localNode},
		}, rec.kinds())
		require.NoError(t, readErr, "the draft file must exist when completion is recorded")
		var draftAtCompletion rebalanceDraft
		require.NoError(t, json.Unmarshal(contentAtCompletion, &draftAtCompletion))
		assert.Equal(t, "plan-1", draftAtCompletion.TaskID)
		assert.Equal(t, uint64(3), draftAtCompletion.Version)
		assert.ElementsMatch(t, []types.RebalanceShardSize{
			{Collection: "A", Shard: "a1", Bytes: 100},
			{Collection: "A", Shard: "a2", Bytes: 200},
			{Collection: "B", Shard: "b1", Bytes: 300},
		}, draftAtCompletion.Shards)

		// One semaphore bounds shard reads across all collections of a draft.
		require.Len(t, sizer.sems, 4)
		require.NotNil(t, sizer.sems[0])
		for _, sem := range sizer.sems[1:] {
			assert.Same(t, sizer.sems[0], sem)
		}
	})

	t.Run("sizing error records a retryable failure", func(t *testing.T) {
		sizer := &draftTestSizer{errs: map[string]error{"A": errors.New("read shard dir: permission denied")}}
		p, rec := newProvider(t, sizer, "A")

		h, err := p.StartTask(newTask(distributedtask.UnitStatusPending, ""))
		require.NoError(t, err)
		waitDone(t, h)

		assert.Equal(t, []draftTestCall{
			{kind: "progress", unit: localNode, progress: 0},
			{kind: "retryable failure", unit: localNode},
		}, rec.kinds())
		assert.NoFileExists(t, draftPath(p))
	})

	t.Run("heartbeat moves the unit while sizing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			sizer := &draftTestSizer{
				usage: map[string]*usagetypes.CollectionUsage{"A": {}, "B": {}},
				block: map[string]chan struct{}{"B": release},
			}
			p, rec := newProvider(t, sizer, "A", "B")
			start := time.Now()

			h, err := p.StartTask(newTask(distributedtask.UnitStatusPending, ""))
			require.NoError(t, err)
			time.Sleep(3*draftHeartbeatInterval + time.Second)
			synctest.Wait()

			calls := rec.snapshot()
			require.Len(t, calls, 4)
			assert.Equal(t, draftTestCall{kind: "progress", unit: localNode, progress: 0}, calls[0].draftTestCall)
			for i, c := range calls[1:] {
				assert.Equal(t, draftTestCall{kind: "progress", unit: localNode, progress: 0.5}, c.draftTestCall)
				assert.Equal(t, time.Duration(i+1)*draftHeartbeatInterval, c.at.Sub(start))
			}

			close(release)
			<-h.Done()
			assert.Equal(t, "completion", rec.snapshot()[4].kind)
		})
	})

	for _, tc := range []struct {
		name       string
		draft      *rebalanceDraft
		wantSizing int64
		wantShards []types.RebalanceShardSize
		wantCalls  []draftTestCall
	}{
		{
			name: "restart with draft reports without re-sizing",
			draft: &rebalanceDraft{TaskID: "plan-1", Version: 3, Shards: []types.RebalanceShardSize{
				{Collection: "A", Shard: "a1", Bytes: 50},
			}},
			wantSizing: 0,
			wantShards: []types.RebalanceShardSize{{Collection: "A", Shard: "a1", Bytes: 50}},
			wantCalls:  []draftTestCall{{kind: "completion", unit: localNode}},
		},
		{
			name:       "restart without draft re-sizes",
			wantSizing: 1,
			wantShards: []types.RebalanceShardSize{{Collection: "A", Shard: "a1", Bytes: 100}},
			wantCalls: []draftTestCall{
				{kind: "progress", unit: localNode, progress: 0},
				{kind: "completion", unit: localNode},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sizer := &draftTestSizer{usage: map[string]*usagetypes.CollectionUsage{
				"A": {Shards: usagetypes.ShardsUsage{{Name: "a1", FullShardStorageBytes: 100}}},
			}}
			p, rec := newProvider(t, sizer, "A")
			if tc.draft != nil {
				require.NoError(t, p.writeDraft("plan-1-3.json", *tc.draft))
			}

			h, err := p.StartTask(newTask(distributedtask.UnitStatusInProgress, localNode))
			require.NoError(t, err)
			waitDone(t, h)

			assert.Equal(t, tc.wantSizing, sizer.calls.Load())
			assert.Equal(t, tc.wantShards, readDraft(t, draftPath(p)).Shards)
			assert.Equal(t, tc.wantCalls, rec.kinds())
		})
	}

	t.Run("draft dirs are synced before the unit completes", func(t *testing.T) {
		// Synced paths are relative to the data path.
		draftRel := filepath.Join(".dtm", types.RebalanceDraftNamespace)
		writeExisting := func(t *testing.T, p *RebalanceDraftProvider) {
			require.NoError(t, p.writeDraft("plan-1-3.json", rebalanceDraft{TaskID: "plan-1", Version: 3}))
		}
		for _, tc := range []struct {
			name       string
			setup      func(t *testing.T, p *RebalanceDraftProvider)
			failOn     string
			wantSynced []string
			wantCalls  []draftTestCall
			wantSizing int64
			wantDraft  bool
		}{
			{
				name:       "write path creates the dirs",
				wantSynced: []string{".", ".dtm"},
				wantCalls: []draftTestCall{
					{kind: "progress", unit: localNode, progress: 0},
					{kind: "completion", unit: localNode},
				},
				wantSizing: 1,
				wantDraft:  true,
			},
			{
				name: "write path finds the dirs present",
				setup: func(t *testing.T, p *RebalanceDraftProvider) {
					require.NoError(t, os.MkdirAll(p.dir, 0o755))
				},
				wantSynced: []string{".", ".dtm"},
				wantCalls: []draftTestCall{
					{kind: "progress", unit: localNode, progress: 0},
					{kind: "completion", unit: localNode},
				},
				wantSizing: 1,
				wantDraft:  true,
			},
			{
				name:       "write path sync fails",
				failOn:     ".dtm",
				wantSynced: []string{".", ".dtm"},
				wantCalls: []draftTestCall{
					{kind: "progress", unit: localNode, progress: 0},
					{kind: "retryable failure", unit: localNode},
				},
				wantSizing: 1,
				wantDraft:  false,
			},
			{
				name:       "draft file already present",
				setup:      writeExisting,
				wantSynced: []string{".", ".dtm", draftRel},
				wantCalls:  []draftTestCall{{kind: "completion", unit: localNode}},
				wantSizing: 0,
				wantDraft:  true,
			},
			{
				name:       "draft file already present and sync fails",
				setup:      writeExisting,
				failOn:     draftRel,
				wantSynced: []string{".", ".dtm", draftRel},
				wantCalls:  []draftTestCall{{kind: "retryable failure", unit: localNode}},
				wantSizing: 0,
				wantDraft:  true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sizer := &draftTestSizer{usage: map[string]*usagetypes.CollectionUsage{"A": {}}}
				p, rec := newProvider(t, sizer, "A")
				if tc.setup != nil {
					tc.setup(t, p)
				}
				dataPath := filepath.Dir(filepath.Dir(p.dir))
				var synced, syncedAtCompletion []string
				p.syncDir = func(path string) error {
					rel, err := filepath.Rel(dataPath, path)
					assert.NoError(t, err)
					synced = append(synced, rel)
					if rel == tc.failOn {
						return errors.New("fsync: input/output error")
					}
					return nil
				}
				rec.onCompletion = func() { syncedAtCompletion = append([]string(nil), synced...) }

				h, err := p.StartTask(newTask(distributedtask.UnitStatusInProgress, localNode))
				require.NoError(t, err)
				waitDone(t, h)

				assert.Equal(t, tc.wantCalls, rec.kinds())
				assert.Equal(t, tc.wantSynced, synced)
				if tc.failOn == "" {
					assert.Equal(t, tc.wantSynced, syncedAtCompletion)
				}
				assert.Equal(t, tc.wantSizing, sizer.calls.Load())
				if tc.wantDraft {
					assert.FileExists(t, draftPath(p))
				} else {
					assert.NoFileExists(t, draftPath(p))
				}
			})
		}
	})

	t.Run("terminate stops drafting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sizer := &draftTestSizer{waitForCancel: true}
			p, rec := newProvider(t, sizer, "A")

			h, err := p.StartTask(newTask(distributedtask.UnitStatusPending, ""))
			require.NoError(t, err)
			synctest.Wait()
			require.Equal(t, int64(1), sizer.calls.Load())

			h.Terminate()
			<-h.Done()
			time.Sleep(2 * draftHeartbeatInterval)
			synctest.Wait()

			assert.Equal(t, []draftTestCall{{kind: "progress", unit: localNode, progress: 0}}, rec.kinds())
			assert.NoFileExists(t, draftPath(p))
		})
	})

	t.Run("prunes other drafts", func(t *testing.T) {
		p, _ := newProvider(t, &draftTestSizer{})
		require.NoError(t, os.MkdirAll(p.dir, 0o755))
		for _, name := range []string{"plan-0-1.json", "plan-1-2.json", "plan-1-3.json.123.tmp", "plan-1-3.json"} {
			require.NoError(t, os.WriteFile(filepath.Join(p.dir, name), []byte("{}"), 0o644))
		}

		h, err := p.StartTask(newTask(distributedtask.UnitStatusCompleted, localNode))
		require.NoError(t, err)
		waitDone(t, h)

		entries, err := os.ReadDir(p.dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "plan-1-3.json", entries[0].Name())
	})

	t.Run("a saved draft reads back by task and version", func(t *testing.T) {
		dataPath := t.TempDir()
		logger, _ := test.NewNullLogger()
		sizer := &draftTestSizer{usage: map[string]*usagetypes.CollectionUsage{
			"A": {Shards: usagetypes.ShardsUsage{{Name: "a1", FullShardStorageBytes: 100}}},
		}}
		p := NewRebalanceDraftProvider(sizer, func() []*models.Class { return []*models.Class{{Class: "A"}} },
			dataPath, localNode, logger, context.Background())
		p.SetCompletionRecorder(&draftTestRecorder{})

		h, err := p.StartTask(newTask(distributedtask.UnitStatusPending, ""))
		require.NoError(t, err)
		waitDone(t, h)

		shards, err := ReadRebalanceDraft(dataPath, "plan-1", 3)
		require.NoError(t, err)
		assert.Equal(t, []types.RebalanceShardSize{{Collection: "A", Shard: "a1", Bytes: 100}}, shards)
		_, err = ReadRebalanceDraft(dataPath, "plan-1", 4)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
}

// draftTestSizer returns canned usages per collection. A call for a collection
// in block waits until its channel closes. With waitForCancel set, every call
// waits until ctx is done. sems records the semaphore passed to each call.
type draftTestSizer struct {
	usage         map[string]*usagetypes.CollectionUsage
	errs          map[string]error
	block         map[string]chan struct{}
	waitForCancel bool
	calls         atomic.Int64

	mu   sync.Mutex
	sems []*semaphore.Weighted
}

func (s *draftTestSizer) UsageForIndex(ctx context.Context, className schema.ClassName,
	shardReadSem *semaphore.Weighted, _ bool, _ map[string]models.VectorConfig,
) (*usagetypes.CollectionUsage, error) {
	s.calls.Add(1)
	s.mu.Lock()
	s.sems = append(s.sems, shardReadSem)
	s.mu.Unlock()
	if s.waitForCancel {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if ch, ok := s.block[string(className)]; ok {
		<-ch
	}
	return s.usage[string(className)], s.errs[string(className)]
}

type draftTestCall struct {
	kind     string
	unit     string
	progress float32
}

type draftTestTimedCall struct {
	draftTestCall
	at time.Time
}

// draftTestRecorder records every call in order. onCompletion runs inside
// the completion call, before it is recorded.
type draftTestRecorder struct {
	mu           sync.Mutex
	calls        []draftTestTimedCall
	onCompletion func()
}

func (r *draftTestRecorder) add(c draftTestCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, draftTestTimedCall{draftTestCall: c, at: time.Now()})
}

func (r *draftTestRecorder) snapshot() []draftTestTimedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]draftTestTimedCall(nil), r.calls...)
}

func (r *draftTestRecorder) kinds() []draftTestCall {
	var out []draftTestCall
	for _, c := range r.snapshot() {
		out = append(out, c.draftTestCall)
	}
	return out
}

func (r *draftTestRecorder) RecordDistributedTaskUnitCompletion(_ context.Context, _, _ string, _ uint64, _, unitID string) error {
	if r.onCompletion != nil {
		r.onCompletion()
	}
	r.add(draftTestCall{kind: "completion", unit: unitID})
	return nil
}

func (r *draftTestRecorder) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, _ string) error {
	r.add(draftTestCall{kind: "failure", unit: unitID})
	return nil
}

func (r *draftTestRecorder) RecordDistributedTaskRetryableUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, _ string) error {
	r.add(draftTestCall{kind: "retryable failure", unit: unitID})
	return nil
}

func (r *draftTestRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, _, _ string, _ uint64, _, unitID string, progress float32) error {
	r.add(draftTestCall{kind: "progress", unit: unitID, progress: progress})
	return nil
}
