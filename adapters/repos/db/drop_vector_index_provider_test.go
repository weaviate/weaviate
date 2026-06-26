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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// --- fakes ---

type fakeEditOpBucket struct {
	mu         sync.Mutex
	registered map[string]lsmkv.OpDescriptor
	pendingSeq [][]string // successive EditOpPending responses; last repeats
	callIdx    int
}

func (f *fakeEditOpBucket) RegisterEditOp(opID string, desc lsmkv.OpDescriptor) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registered == nil {
		f.registered = map[string]lsmkv.OpDescriptor{}
	}
	f.registered[opID] = desc
	return nil
}

func (f *fakeEditOpBucket) EditOpPending(opID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.callIdx
	if i >= len(f.pendingSeq) {
		i = len(f.pendingSeq) - 1
	}
	f.callIdx++
	return f.pendingSeq[i], nil
}

type removedCall struct {
	collection, shard string
	targets           []string
}

type fakeShards struct {
	bucket    editOpBucket
	bucketErr error
	mu        sync.Mutex
	removed   []removedCall
}

func (f *fakeShards) EditOpBucketForShard(collection, shard string) (editOpBucket, error) {
	if f.bucketErr != nil {
		return nil, f.bucketErr
	}
	return f.bucket, nil
}

func (f *fakeShards) EnsureDroppedVectorFilesRemoved(collection, shard string, targets []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, removedCall{collection, shard, targets})
	return nil
}

type fakeFinalizer struct {
	called     bool
	collection string
	targets    []string
	err        error
}

func (f *fakeFinalizer) RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error {
	f.called = true
	f.collection = collection
	f.targets = targets
	return f.err
}

type fakeRecorder struct {
	mu        sync.Mutex
	completed []string
	failed    map[string]string
	progress  map[string]float32
}

func newFakeRecorder() *fakeRecorder {
	return &fakeRecorder{failed: map[string]string{}, progress: map[string]float32{}}
}

func (r *fakeRecorder) RecordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = append(r.completed, unitID)
	return nil
}

func (r *fakeRecorder) RecordDistributedTaskUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed[unitID] = errMsg
	return nil
}

func (r *fakeRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress[unitID] = progress
	return nil
}

// --- helpers ---

func newTestDropProvider(shards dropVectorShards, fin dropVectorSchemaFinalizer, rec distributedtask.TaskCompletionRecorder) *DropVectorIndexProvider {
	logger, _ := test.NewNullLogger()
	p := NewDropVectorIndexProvider(shards, fin, logger, "node1", context.Background())
	p.pollInterval = time.Millisecond
	p.SetCompletionRecorder(rec)
	return p
}

func dropTask(status distributedtask.TaskStatus, units map[string]*distributedtask.Unit) *distributedtask.Task {
	payload := &DropVectorIndexTaskPayload{
		Collection:  "Collection",
		Targets:     []string{"v1"},
		OpID:        "op1",
		UnitToNode:  map[string]string{"u1": "node1"},
		UnitToShard: map[string]string{"u1": "shard1"},
	}
	enc, _ := payload.encode()
	return &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        enc,
		Status:         status,
		Units:          units,
	}
}

func waitDone(t *testing.T, h distributedtask.TaskHandle) {
	t.Helper()
	select {
	case <-h.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("StartTask goroutine did not finish in time")
	}
}

// --- StartTask ---

func TestStartTask_FreshOp_FlushSnapshotRegister(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{"s1", "s2"}, {}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, bucket.registered, "op1", "fresh op must be registered on the bucket")
	require.Equal(t, []string{"v1"}, bucket.registered["op1"].Targets)
	require.Equal(t, lsmkv.OpTypeRemoveTargetVectors, bucket.registered["op1"].Type)
	require.Equal(t, []string{"u1"}, rec.completed, "unit completes once pending drains")
}

func TestStartTask_SkipsAlreadyCompletedUnit(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.NotContains(t, bucket.registered, "op1", "completed unit must not be re-registered")
	require.Empty(t, rec.completed)
}

func TestStartTask_BucketLookupFails_UnitFailed(t *testing.T) {
	shards := &fakeShards{bucketErr: errors.New("no such shard")}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1")
	require.Empty(t, rec.completed)
}

// --- OnGroupCompleted ---

func TestOnGroupCompleted_PerTenantIndexFilesRemoved(t *testing.T) {
	shards := &fakeShards{}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())

	task := dropTask(distributedtask.TaskStatusStarted, nil)
	require.NoError(t, p.OnGroupCompleted(task, "tenant1", []string{"u1"}))

	require.Len(t, shards.removed, 1)
	require.Equal(t, removedCall{collection: "Collection", shard: "shard1", targets: []string{"v1"}}, shards.removed[0])
}

// --- OnTaskCompleted ---

func TestOnTaskCompleted_FAILED_DoesNotMutateSchema(t *testing.T) {
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{}, fin, newFakeRecorder())

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusFailed, nil))
	require.False(t, fin.called, "FAILED task must not mutate schema")
}

func TestOnTaskCompleted_FINISHED_RemovesVectorConfig(t *testing.T) {
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{}, fin, newFakeRecorder())

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusFinished, nil))
	require.True(t, fin.called)
	require.Equal(t, "Collection", fin.collection)
	require.Equal(t, []string{"v1"}, fin.targets)
}

// --- detectors ---

func activeDropTask(id, collection string, targets ...string) *distributedtask.Task {
	payload := &DropVectorIndexTaskPayload{Collection: collection, Targets: targets, OpID: "op-" + id}
	enc, _ := payload.encode()
	return &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusStarted,
	}
}

func TestCheckConflict(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	newPayload, _ := (&DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v1"}, OpID: "new"}).encode()

	t.Run("overlapping target same collection conflicts", func(t *testing.T) {
		err := p.CheckConflict(newPayload, []*distributedtask.Task{activeDropTask("t1", "C", "v1", "v2")})
		require.Error(t, err)
	})
	t.Run("different collection does not conflict", func(t *testing.T) {
		require.NoError(t, p.CheckConflict(newPayload, []*distributedtask.Task{activeDropTask("t1", "Other", "v1")}))
	})
	t.Run("disjoint targets do not conflict", func(t *testing.T) {
		require.NoError(t, p.CheckConflict(newPayload, []*distributedtask.Task{activeDropTask("t1", "C", "v9")}))
	})
	t.Run("terminal task does not conflict", func(t *testing.T) {
		done := activeDropTask("t1", "C", "v1")
		done.Status = distributedtask.TaskStatusFinished
		require.NoError(t, p.CheckConflict(newPayload, []*distributedtask.Task{done}))
	})
}

func TestCheckClassMutation_DoesNotBlockDeleteDuringDrop(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	// DeleteClass supersedes an in-flight drop (the whole bucket is going away);
	// the schema FSM cascade-deletes the task, so the guard must not block it.
	require.NoError(t, p.CheckClassMutation("C", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
	require.NoError(t, p.CheckClassMutation("Other", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
}

func TestCheckTenantMutation_BlocksDuringDrop(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	require.Error(t, p.CheckTenantMutation("C", []string{"tenant1"}, []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
}

func TestCheckPropertyUpdate_NeverConflicts(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	require.NoError(t, p.CheckPropertyUpdate("C", "someProp", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
}

func TestExtractDropVectorIndexTaskCollection(t *testing.T) {
	enc, _ := (&DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v"}, OpID: "op"}).encode()
	got, ok := ExtractDropVectorIndexTaskCollection(enc)
	require.True(t, ok)
	require.Equal(t, "C", got)

	_, ok = ExtractDropVectorIndexTaskCollection([]byte("not json"))
	require.False(t, ok)
}
