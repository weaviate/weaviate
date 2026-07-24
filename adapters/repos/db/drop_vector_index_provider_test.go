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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// --- fakes ---

type pendingStep struct {
	vals []string
	err  error
}

type fakeEditOpBucket struct {
	mu             sync.Mutex
	registered     map[string]lsmkv.OpDescriptor
	pendingSeq     [][]string    // successive EditOpPending responses; last repeats
	script         []pendingStep // if set, takes precedence over pendingSeq (per-call val/err; last repeats)
	callIdx        int
	deleted        []string // opIDs passed to DeleteEditOp
	deleteErr      error
	quarantined    []string   // EditOpQuarantined response (nil = none)
	quarantinedSeq [][]string // successive responses; last repeats (precedence over quarantined)
	quarantinedErr error      // when set, EditOpQuarantined returns it
	quarantineIdx  int
	pendingErr     error                          // when set, EditOpPending returns it
	pendingFn      func(string) ([]string, error) // when set, overrides all other pending sources
	polled         chan struct{}                  // if set, a non-blocking signal per EditOpPending call
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
	if f.polled != nil {
		select {
		case f.polled <- struct{}{}:
		default:
		}
	}
	if f.pendingErr != nil {
		return nil, f.pendingErr
	}
	if f.pendingFn != nil {
		return f.pendingFn(opID)
	}
	if f.script != nil {
		i := f.callIdx
		if i >= len(f.script) {
			i = len(f.script) - 1
		}
		f.callIdx++
		return f.script[i].vals, f.script[i].err
	}
	i := f.callIdx
	if i >= len(f.pendingSeq) {
		i = len(f.pendingSeq) - 1
	}
	f.callIdx++
	return f.pendingSeq[i], nil
}

func (f *fakeEditOpBucket) EditOpQuarantined(opID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.quarantinedErr != nil {
		return nil, f.quarantinedErr
	}
	if len(f.quarantinedSeq) > 0 {
		i := f.quarantineIdx
		if i >= len(f.quarantinedSeq) {
			i = len(f.quarantinedSeq) - 1
		}
		f.quarantineIdx++
		return f.quarantinedSeq[i], nil
	}
	return f.quarantined, nil
}

func (f *fakeEditOpBucket) DeleteEditOp(opID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, opID)
	return nil
}

type removedCall struct {
	collection, shard string
	targets           []string
}

type fakeShards struct {
	bucket    editOpBucket
	buckets   map[string]editOpBucket // per-shard override; takes precedence over bucket
	bucketErr error
	mu        sync.Mutex
	removed   []removedCall
	removeErr map[string]error // per-shard EnsureDroppedVectorFilesRemoved error
}

func (f *fakeShards) resolve(shardNames []string) (map[string]editOpBucket, error) {
	if f.bucketErr != nil {
		// Simulates "shard not locally available": absent from the result map.
		return map[string]editOpBucket{}, nil
	}
	out := make(map[string]editOpBucket, len(shardNames))
	for _, name := range shardNames {
		if f.buckets != nil {
			if b, ok := f.buckets[name]; ok {
				out[name] = b
			}
			continue
		}
		out[name] = f.bucket
	}
	return out, nil
}

func (f *fakeShards) EditOpBucketsForShards(ctx context.Context, collection string, shardNames []string) (map[string]editOpBucket, error) {
	return f.resolve(shardNames)
}

func (f *fakeShards) EditOpBucketsForLoadedShards(collection string, shardNames []string) (map[string]editOpBucket, error) {
	return f.resolve(shardNames)
}

func (f *fakeShards) EnsureDroppedVectorFilesRemoved(collection, shard string, targets []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.removeErr[shard]; err != nil {
		return err
	}
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

// fakeShardingReader returns a sharding state whose Physical set is exactly
// `shards` (the coverage guard compares it against the task's units) and a class
// whose VectorConfig is `vectorCfg` (the arm-time still-dropped guard reads it).
type fakeShardingReader struct {
	shards         []string
	vectorCfg      map[string]models.VectorConfig
	activeTasks    []*distributedtask.Task // returned by ListDistributedTasks
	err            error
	readClassErrs  int // QueryReadOnlyClasses fails this many times, then succeeds
	readClassCalls int
	listTasksErr   error
}

func (f *fakeShardingReader) QueryShardingState(class string) (*sharding.State, uint64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	state := &sharding.State{Physical: map[string]sharding.Physical{}}
	for _, name := range f.shards {
		state.Physical[name] = sharding.Physical{}
	}
	return state, 0, nil
}

func (f *fakeShardingReader) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	if f.listTasksErr != nil {
		return nil, f.listTasksErr
	}
	return map[string][]*distributedtask.Task{DropVectorIndexNamespace: f.activeTasks}, nil
}

func (f *fakeShardingReader) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	f.readClassCalls++
	if f.readClassErrs > 0 {
		f.readClassErrs--
		return nil, errors.New("transient leader read failure")
	}
	if f.err != nil {
		return nil, f.err
	}
	cfg := f.vectorCfg
	if cfg == nil {
		cfg = map[string]models.VectorConfig{"v1": droppedCfg()} // default: target still dropped
	}
	out := map[string]versioned.Class{}
	for _, name := range classes {
		out[name] = versioned.Class{Class: &models.Class{Class: name, VectorConfig: cfg}}
	}
	return out, nil
}

func newTestDropProvider(shards dropVectorShards, fin dropVectorSchemaFinalizer, rec distributedtask.TaskCompletionRecorder) *DropVectorIndexProvider {
	return newTestDropProviderCtx(shards, fin, rec, context.Background())
}

func newTestDropProviderCtx(shards dropVectorShards, fin dropVectorSchemaFinalizer, rec distributedtask.TaskCompletionRecorder, serverCtx context.Context) *DropVectorIndexProvider {
	logger, _ := test.NewNullLogger()
	// Default sharding state: exactly the shard the default dropTask covers.
	p := NewDropVectorIndexProvider(shards, fin, &fakeShardingReader{shards: []string{"shard1"}}, logger, "node1", serverCtx)
	p.pollInterval = time.Millisecond
	p.verifyRetryBackoff = time.Millisecond
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

// TestStartTask_ShutdownMidDrain_LeavesUnitUnfailed pins the data-safety guard: a
// graceful shutdown (serverCtx cancelled) mid-drain must leave the unit neither
// FAILED nor COMPLETED, so the task resumes after restart. Deleting the ctx.Err()
// guards would fail the unit here (and strand the marker).
func TestStartTask_ShutdownMidDrain_LeavesUnitUnfailed(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{"s1"}}, polled: make(chan struct{}, 1)}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	p := newTestDropProviderCtx(shards, &fakeFinalizer{}, rec, ctx)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)

	<-bucket.polled // the drain is running (never drains: pendingSeq stays {"s1"})
	cancel()        // graceful shutdown
	waitDone(t, h)

	require.Empty(t, rec.failed, "shutdown must not fail the unit")
	require.Empty(t, rec.completed, "shutdown must not complete the unit")
}

// TestStartTask_DrainError_FailsUnit confirms a pending-read error that can't be
// tolerated fails the unit (here the baseline read errors, which is terminal).
func TestStartTask_DrainError_FailsUnit(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{"s1"}}, pendingErr: errors.New("bolt read failed")}
	shards := &fakeShards{bucket: bucket}
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

// TestPollUntilEmpty_ProgressClampedWhenPendingGrows pins the progress clamp: when
// the pending set transiently grows (a re-queue), reported progress must stay in
// [0,1] rather than going negative.
func TestPollUntilEmpty_ProgressClampedWhenPendingGrows(t *testing.T) {
	// total=1 (baseline), grows to 3 (progress would be -2 without the clamp), drains.
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{"s1"}, {"s1", "s2", "s3"}, {}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.GreaterOrEqual(t, rec.progress["u1"], float32(0))
	require.LessOrEqual(t, rec.progress["u1"], float32(1))
	require.Equal(t, []string{"u1"}, rec.completed)
}

// TestPollUntilEmpty_ToleratesTransientErrors: a few consecutive pending-read
// errors mid-drain are tolerated (not failed) and the unit completes on recovery.
func TestPollUntilEmpty_ToleratesTransientErrors(t *testing.T) {
	blip := errors.New("transient bolt read")
	bucket := &fakeEditOpBucket{script: []pendingStep{
		{vals: []string{"s1"}}, // baseline: total=1
		{err: blip},            // tolerated (1)
		{err: blip},            // tolerated (2)
		{vals: []string{}},     // recovered + drained
	}}
	rec := newFakeRecorder()
	p := newTestDropProvider(&fakeShards{bucket: bucket}, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Equal(t, []string{"u1"}, rec.completed, "transient poll errors must not fail the unit")
	require.Empty(t, rec.failed)
}

// TestPollUntilEmpty_PersistentErrorsFailUnit: once errors exceed the tolerance
// the unit fails (so a permanently broken shard doesn't hang the task forever).
func TestPollUntilEmpty_PersistentErrorsFailUnit(t *testing.T) {
	boom := errors.New("bolt read down")
	bucket := &fakeEditOpBucket{script: []pendingStep{
		{vals: []string{"s1"}},                // baseline
		{err: boom}, {err: boom}, {err: boom}, // last repeats; trips at maxConsecutivePollErrors
	}}
	rec := newFakeRecorder()
	p := newTestDropProvider(&fakeShards{bucket: bucket}, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1")
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

// TestStartTask_TargetNoLongerDropped_RefusesToArm pins the arm-time guard: if
// the leader-consistent class shows the target live again (class deleted and
// re-created in the marker->enqueue window), the unit must fail WITHOUT
// registering the op — arming would strip live user data.
func TestStartTask_TargetNoLongerDropped_RefusesToArm(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{
		shards:    []string{"shard1"},
		vectorCfg: map[string]models.VectorConfig{"v1": {VectorIndexType: "hnsw"}}, // live again
	}

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Empty(t, bucket.registered, "op must not be armed against a live vector")
	require.Contains(t, rec.failed, "u1")
	require.Empty(t, rec.completed)
}

// TestStartTask_VerifyErrorPersistent_UnitsFailWithoutArming pins the arm-time
// tolerance ceiling: a persistent leader-read failure exhausts the bounded
// retries, fails every unit, and never arms an op.
func TestStartTask_VerifyErrorPersistent_UnitsFailWithoutArming(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{shards: []string{"shard1"}, err: errors.New("leader unreachable")}

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Empty(t, bucket.registered, "op must not be armed when the marker can't be verified")
	require.Contains(t, rec.failed, "u1")
	require.Empty(t, rec.completed)
}

// TestStartTask_VerifyErrorTransient_RecoversAndArms pins the tolerance itself:
// a single leader-read blip is retried, the verify succeeds, and the unit arms
// and drains normally.
func TestStartTask_VerifyErrorTransient_RecoversAndArms(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{shards: []string{"shard1"}, readClassErrs: 1}

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.NotEmpty(t, bucket.registered, "a transient blip must not stop the arm")
	require.Contains(t, rec.completed, "u1")
	require.Empty(t, rec.failed)
}

// TestStartTask_ProcessesOnlyLocalUnits pins the node filter: units owned by
// other nodes are neither armed nor completed here.
func TestStartTask_ProcessesOnlyLocalUnits(t *testing.T) {
	bucket := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{shards: []string{"shard1", "shard2"}}

	payload := &DropVectorIndexTaskPayload{
		Collection: "Collection", Targets: []string{"v1"}, OpID: "op1",
		UnitToNode:  map[string]string{"u1": "node1", "u2": "node2"},
		UnitToShard: map[string]string{"u1": "shard1", "u2": "shard2"},
	}
	enc, _ := payload.encode()
	task := &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusStarted,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
			"u2": {ID: "u2", Status: distributedtask.UnitStatusPending},
		},
	}
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Equal(t, []string{"u1"}, rec.completed, "only the local node's unit completes")
	require.Empty(t, rec.failed, "the remote node's unit is not touched")
}

// TestProcessUnits_PartialArm_DrainsArmedFailsMissing pins the partial-arm
// journey: one shard unavailable fails ONLY its unit; the available unit still
// arms and drains to completion (the task fails overall, terminal-status op
// deletion + reconciliation handle the rest).
func TestProcessUnits_PartialArm_DrainsArmedFailsMissing(t *testing.T) {
	bucket2 := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	shards := &fakeShards{buckets: map[string]editOpBucket{"shard2": bucket2}} // shard1 absent
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{shards: []string{"shard1", "shard2"}}

	payload := &DropVectorIndexTaskPayload{
		Collection: "Collection", Targets: []string{"v1"}, OpID: "op1",
		UnitToNode:  map[string]string{"u1": "node1", "u2": "node1"},
		UnitToShard: map[string]string{"u1": "shard1", "u2": "shard2"},
	}
	enc, _ := payload.encode()
	task := &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusStarted,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
			"u2": {ID: "u2", Status: distributedtask.UnitStatusPending},
		},
	}
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1", "the unavailable shard's unit fails")
	require.Equal(t, []string{"u2"}, rec.completed, "the available unit still arms and drains")
	require.Contains(t, bucket2.registered, "op1")
}

// TestStartTask_QuarantinedSegment_UnitFailed pins the quarantine->FAILED
// handoff: the unit must fail, not complete, when a segment is quarantined.
func TestStartTask_QuarantinedSegment_UnitFailed(t *testing.T) {
	bucket := &fakeEditOpBucket{
		pendingSeq:  [][]string{{"s1"}}, // never drains on its own
		quarantined: []string{"s1"},
	}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1", "a quarantined segment must fail the unit")
	require.Contains(t, rec.failed["u1"], "quarantined")
	require.Empty(t, rec.completed, "the unit must not complete when a segment is quarantined")
}

// TestStartTask_QuarantineAppearsMidDrain_UnitFailed pins that the check runs
// every tick, not only at start.
func TestStartTask_QuarantineAppearsMidDrain_UnitFailed(t *testing.T) {
	bucket := &fakeEditOpBucket{
		pendingSeq:     [][]string{{"s1", "s2"}, {"s1"}},
		quarantinedSeq: [][]string{{}, {"s1"}},
	}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1")
	require.Contains(t, rec.failed["u1"], "quarantined")
	require.Empty(t, rec.completed)
}

// TestStartTask_QuarantineReadErrorPersistent_UnitFailed pins that a
// quarantine-read failure shares the poll's blip tolerance: persistent errors
// fail the unit after maxConsecutivePollErrors.
func TestStartTask_QuarantineReadErrorPersistent_UnitFailed(t *testing.T) {
	bucket := &fakeEditOpBucket{
		pendingSeq:     [][]string{{"s1"}},
		quarantinedErr: errors.New("bolt read failed"),
	}
	shards := &fakeShards{bucket: bucket}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)

	task := dropTask(distributedtask.TaskStatusStarted, map[string]*distributedtask.Unit{
		"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
	})
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.Contains(t, rec.failed, "u1")
	require.Contains(t, rec.failed["u1"], "consecutive errors")
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

// TestOnGroupCompleted_OneFailingShardDoesNotBlockOthers: removal errors are
// accumulated, so a persistently failing shard can't block the file cleanup of
// every other tenant in the group; the joined error still surfaces for retry.
func TestOnGroupCompleted_OneFailingShardDoesNotBlockOthers(t *testing.T) {
	shards := &fakeShards{removeErr: map[string]error{"shard1": errors.New("busy")}}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())

	payload := &DropVectorIndexTaskPayload{
		Collection: "Collection", Targets: []string{"v1"}, OpID: "op1",
		UnitToNode:  map[string]string{"u1": "node1", "u2": "node1"},
		UnitToShard: map[string]string{"u1": "shard1", "u2": "shard2"},
	}
	enc, _ := payload.encode()
	task := &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusStarted,
	}

	err := p.OnGroupCompleted(task, "g", []string{"u1", "u2"})
	require.Error(t, err, "the failing shard's error must surface for retry")
	require.Len(t, shards.removed, 1, "the healthy shard must still be cleaned")
	require.Equal(t, "shard2", shards.removed[0].shard)
}

// TestOnGroupCompleted_ReplayAfterRecreate_SkipsFileRemoval pins the restart
// replay guard: once the target is no longer marked dropped (finalized, or
// finalized and re-created), a replayed group completion must not delete the
// live index's files.
func TestOnGroupCompleted_ReplayAfterRecreate_SkipsFileRemoval(t *testing.T) {
	shards := &fakeShards{}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())
	p.sharding = &fakeShardingReader{
		shards:    []string{"shard1"},
		vectorCfg: map[string]models.VectorConfig{"v1": {VectorIndexType: "hnsw"}}, // re-created: live again
	}

	require.NoError(t, p.OnGroupCompleted(dropTask(distributedtask.TaskStatusFinished, nil), "g", []string{"u1"}))
	require.Empty(t, shards.removed, "a live index's files must not be touched")
}

// TestOnGroupCompleted_VerifyError_SurfacesForRetry: an unverifiable marker
// state must not proceed to file removal.
func TestOnGroupCompleted_VerifyError_SurfacesForRetry(t *testing.T) {
	shards := &fakeShards{}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())
	p.sharding = &fakeShardingReader{shards: []string{"shard1"}, err: errors.New("no leader")}

	require.Error(t, p.OnGroupCompleted(dropTask(distributedtask.TaskStatusFinished, nil), "g", []string{"u1"}))
	require.Empty(t, shards.removed)
}

// TestOnGroupCompleted_VerifyTolerantOfLeaderBlips: the verify gets the same
// bounded retry as the arm-time one — a momentary leader read failure must not
// fail the group's ack (which would fail the whole task).
func TestOnGroupCompleted_VerifyTolerantOfLeaderBlips(t *testing.T) {
	shards := &fakeShards{}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())
	p.sharding = &fakeShardingReader{shards: []string{"shard1"}, readClassErrs: 1}

	require.NoError(t, p.OnGroupCompleted(dropTask(distributedtask.TaskStatusFinished, nil), "g", []string{"u1"}))
	require.Len(t, shards.removed, 1)
}

// TestOnGroupCompleted_VerifyMemoizedPerTask: one group completion fires per
// tenant; the verify is task-invariant while the task lives, so it must cost
// one leader read, not one per tenant. OnTaskCompleted ends the
// group-callback phase and evicts the memo (CleanupTask is a dead path here —
// GetLocalTasks returns nil), so a post-restart replay of the completion
// against a finalized + re-created name re-verifies instead of trusting a
// stale "still dropped".
func TestOnGroupCompleted_VerifyMemoizedPerTask(t *testing.T) {
	shards := &fakeShards{bucket: &fakeEditOpBucket{}}
	p := newTestDropProvider(shards, &fakeFinalizer{}, newFakeRecorder())
	reader := &fakeShardingReader{shards: []string{"shard1"}}
	p.sharding = reader

	task := dropTask(distributedtask.TaskStatusFinished, nil)
	require.NoError(t, p.OnGroupCompleted(task, "tenant1", []string{"u1"}))
	require.NoError(t, p.OnGroupCompleted(task, "tenant2", []string{"u1"}))
	require.Equal(t, 1, reader.readClassCalls, "per-tenant completions share one verify")

	require.NoError(t, p.OnTaskCompleted(task))
	require.NotContains(t, p.verifiedStillDropped, task.ID,
		"task completion ends the group-callback phase and must evict the memo")

	// Replayed completion after the eviction, target re-created since: a fresh
	// verify must run and see the live index, not a memoized verdict.
	reader.vectorCfg = map[string]models.VectorConfig{"v1": {VectorIndexType: "hnsw"}}
	before := reader.readClassCalls
	shards.removed = nil
	require.NoError(t, p.OnGroupCompleted(task, "tenant1", []string{"u1"}))
	require.Greater(t, reader.readClassCalls, before, "the replay re-verifies")
	require.Empty(t, shards.removed, "the fresh verify sees the re-created index")

	// CleanupTask stays as a belt eviction.
	require.NoError(t, p.OnGroupCompleted(task, "tenant2", []string{"u1"}))
	require.NoError(t, p.CleanupTask(task.TaskDescriptor))
	require.NotContains(t, p.verifiedStillDropped, task.ID)
}

// --- OnTaskCompleted ---

// TestOnTaskCompleted_TerminalTasks_DeleteOpButKeepSchema: FAILED/CANCELLED leave
// the schema marker (operator retry) but MUST delete the edit op — a lingering op
// would survive a later successful re-drop (fresh op ID), and once that re-drop
// frees the name it would strip a re-created same-name vector.
func TestOnTaskCompleted_TerminalTasks_DeleteOpButKeepSchema(t *testing.T) {
	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			bucket := &fakeEditOpBucket{}
			fin := &fakeFinalizer{}
			p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())

			p.OnTaskCompleted(dropTask(status, nil))
			require.False(t, fin.called, "%s task must not mutate schema", status)
			require.Equal(t, []string{"op1"}, bucket.deleted, "%s task must disarm its edit op", status)
		})
	}
}

// TestOnTaskCompleted_Success_DeletesEditOpThenRemovesVectorConfig: success is
// delivered as SWAPPING (the scheduler finalizes the task only after this
// callback) — that live completion deletes the local op AND removes the schema
// entry. A node observing the task as FINISHED (peer finalized first, or a
// restart replay) still deletes its local op but leaves the schema entry: the
// peer's finalize removed it, and if that finalize was missed, a FINISHED
// record must not remove a marker that may belong to a re-created name —
// reconciliation heals with a fresh-epoch re-clean instead.
func TestOnTaskCompleted_Success_DeletesEditOpThenRemovesVectorConfig(t *testing.T) {
	t.Run("SWAPPING finalizes", func(t *testing.T) {
		bucket := &fakeEditOpBucket{}
		fin := &fakeFinalizer{}
		p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())

		p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))

		require.Equal(t, []string{"op1"}, bucket.deleted, "must delete the completed op on the local shard")
		require.True(t, fin.called)
		require.Equal(t, "Collection", fin.collection)
		require.Equal(t, []string{"v1"}, fin.targets)
	})

	t.Run("FINISHED deletes the op but leaves finalize to the peer or reconciliation", func(t *testing.T) {
		bucket := &fakeEditOpBucket{}
		fin := &fakeFinalizer{}
		p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())

		p.OnTaskCompleted(dropTask(distributedtask.TaskStatusFinished, nil))

		require.Equal(t, []string{"op1"}, bucket.deleted, "the local op is still deleted")
		require.False(t, fin.called)
	})
}

func TestOnTaskCompleted_DeleteOpFailure_DefersSchemaRemoval(t *testing.T) {
	bucket := &fakeEditOpBucket{deleteErr: errors.New("bolt busy")}
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))

	require.False(t, fin.called, "schema removal must be deferred when op deletion fails on a loaded shard")
}

// TestOnTaskCompleted_UnloadedShard_SkipsDeleteAndFinalizes: an unloaded shard's
// op is NOT force-loaded for deletion (a replayed completion callback would
// otherwise mass-load inactive shards) — it is skipped, and the sweep on the
// shard's next load (plus the periodic cleanup-cycle sweep) disarms the op.
// Finalize proceeds when coverage holds.
func TestOnTaskCompleted_UnloadedShard_SkipsDeleteAndFinalizes(t *testing.T) {
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{bucketErr: errors.New("shard not loaded")}, fin, newFakeRecorder())

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))
	require.True(t, fin.called, "an unloaded shard is skipped (sweep disarms it), not a finalize blocker")
}

// TestOnTaskCompleted_ActiveOverlappingDrop_DefersFinalize: a replayed old task's
// finalize must not remove the marker of a NEWER active drop on the same target —
// that would free the name while the newer op still strips it.
func TestOnTaskCompleted_ActiveOverlappingDrop_DefersFinalize(t *testing.T) {
	bucket := &fakeEditOpBucket{}
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())
	newer := activeDropTask("t2", "Collection", "v1")
	p.sharding = &fakeShardingReader{shards: []string{"shard1"}, activeTasks: []*distributedtask.Task{newer}}

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))

	require.Equal(t, []string{"op1"}, bucket.deleted, "own ops are still deleted")
	require.False(t, fin.called, "finalize must be deferred while a newer drop on the target is active")
}

// TestProcessUnits_ArmsAllBeforeDraining pins the two-phase contract: every
// unit's op is registered before any unit's drain begins, so all shards'
// compaction/cleanup cycles work concurrently instead of serializing.
func TestProcessUnits_ArmsAllBeforeDraining(t *testing.T) {
	bucket2 := &fakeEditOpBucket{pendingSeq: [][]string{{}}}
	// bucket1 drains only once shard2's op has been registered — under the old
	// one-unit-at-a-time flow this deadlocks unit1's poll (bounded by waitDone).
	bucket1 := &fakeEditOpBucket{}
	bucket1.script = []pendingStep{{vals: []string{"s1"}}}
	gate := func(opID string) ([]string, error) {
		bucket2.mu.Lock()
		registered := len(bucket2.registered) > 0
		bucket2.mu.Unlock()
		if registered {
			return nil, nil
		}
		return []string{"s1"}, nil
	}
	bucket1.pendingFn = gate

	shards := &fakeShards{buckets: map[string]editOpBucket{"shard1": bucket1, "shard2": bucket2}}
	rec := newFakeRecorder()
	p := newTestDropProvider(shards, &fakeFinalizer{}, rec)
	p.sharding = &fakeShardingReader{shards: []string{"shard1", "shard2"}}

	payload := &DropVectorIndexTaskPayload{
		Collection: "Collection", Targets: []string{"v1"}, OpID: "op1",
		UnitToNode:  map[string]string{"u1": "node1", "u2": "node1"},
		UnitToShard: map[string]string{"u1": "shard1", "u2": "shard2"},
	}
	enc, _ := payload.encode()
	task := &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusStarted,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
			"u2": {ID: "u2", Status: distributedtask.UnitStatusPending},
		},
	}
	h, err := p.StartTask(task)
	require.NoError(t, err)
	waitDone(t, h)

	require.ElementsMatch(t, []string{"u1", "u2"}, rec.completed,
		"unit1 can only drain if unit2's op was registered before unit1's poll — the two-phase contract")
}

// TestOnTaskCompleted_UncoveredTenant_DefersFinalize pins the cold-tenant guard:
// when the collection has a shard this task carried no unit for (a tenant that was
// inactive at enqueue, or created since), the schema marker must stay — removing
// it would strand that tenant's data and stale index files under a re-creatable
// name. The completed local ops are still deleted.
func TestOnTaskCompleted_UncoveredTenant_DefersFinalize(t *testing.T) {
	bucket := &fakeEditOpBucket{}
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())
	p.sharding = &fakeShardingReader{shards: []string{"shard1", "coldTenant"}}

	p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))

	require.Equal(t, []string{"op1"}, bucket.deleted, "covered shards' ops are still deleted")
	require.False(t, fin.called, "schema removal must be deferred while a shard is uncovered")
}

// TestOnTaskCompleted_CleanedShardsVouchForColdTenant pins the cross-task
// coverage memory: a shard with no unit in THIS task but recorded as cleaned by
// an earlier task of the same epoch (CleanedShards) counts as covered, so the
// drop finalizes even though that tenant is cold again at completion.
func TestOnTaskCompleted_CleanedShardsVouchForColdTenant(t *testing.T) {
	bucket := &fakeEditOpBucket{}
	fin := &fakeFinalizer{}
	p := newTestDropProvider(&fakeShards{bucket: bucket}, fin, newFakeRecorder())
	p.sharding = &fakeShardingReader{shards: []string{"shard1", "coldTenant"}}

	task := dropTask(distributedtask.TaskStatusSwapping, nil)
	payload := &DropVectorIndexTaskPayload{
		Collection:    "Collection",
		Targets:       []string{"v1"},
		OpID:          "op1",
		UnitToNode:    map[string]string{"u1": "node1"},
		UnitToShard:   map[string]string{"u1": "shard1"},
		CleanedShards: []string{"coldTenant"},
	}
	task.Payload, _ = payload.encode()

	p.OnTaskCompleted(task)

	require.True(t, fin.called, "cleaned-but-cold shards are covered by the inherited set")
}

// TestOnTaskCompleted_CheckError_DefersFinalize: an unreadable leader state —
// task list (epoch-blindness guard) or sharding state (coverage) — must defer
// the schema removal, not proceed blind.
func TestOnTaskCompleted_CheckError_DefersFinalize(t *testing.T) {
	tests := []struct {
		name     string
		sharding *fakeShardingReader
	}{
		{"overlap check cannot list tasks", &fakeShardingReader{shards: []string{"shard1"}, listTasksErr: errors.New("no leader")}},
		{"coverage check cannot read sharding state", &fakeShardingReader{err: errors.New("no leader")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fin := &fakeFinalizer{}
			p := newTestDropProvider(&fakeShards{bucket: &fakeEditOpBucket{}}, fin, newFakeRecorder())
			p.sharding = tt.sharding

			p.OnTaskCompleted(dropTask(distributedtask.TaskStatusSwapping, nil))
			require.False(t, fin.called)
		})
	}
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

// TestCheckConflict_InheritanceSourceGuard pins the enqueue-to-commit TOCTOU
// guard: a payload's CleanedShards claim must be re-provable from records
// still in the FSM task list when AddTask applies. The dangerous interleaving:
// coverage read → DeleteClass (cascade wipes the records) + re-create with the
// same tenant names + re-drop (nothing left to purge, nothing active to
// refuse) → the stale AddTask commits. Without the guard that task inherits
// the dead class generation's coverage and finalizes over the new class's
// never-cleaned "cleaned" tenants.
func TestCheckConflict_InheritanceSourceGuard(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())

	payload := func(epoch string, cleaned []string) []byte {
		pl := &DropVectorIndexTaskPayload{
			Collection: "C", Targets: []string{"v1"}, OpID: "new",
			DropEpochID: epoch, CleanedShards: cleaned,
		}
		enc, _ := pl.encode()
		return enc
	}
	record := func(id, epoch string, status distributedtask.TaskStatus, unitShards, cleaned []string, targets ...string) *distributedtask.Task {
		pl := &DropVectorIndexTaskPayload{
			Collection: "C", Targets: targets, OpID: "op-" + id,
			DropEpochID: epoch, CleanedShards: cleaned, UnitToShard: map[string]string{},
		}
		for i, s := range unitShards {
			pl.UnitToShard[fmt.Sprintf("%s__u%d", s, i)] = s
		}
		enc, _ := pl.encode()
		return &distributedtask.Task{
			Namespace:      DropVectorIndexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
			Payload:        enc,
			Status:         status,
		}
	}
	fin := distributedtask.TaskStatusFinished

	tests := []struct {
		name    string
		payload []byte
		tasks   []*distributedtask.Task
		wantErr string // "" = accepted
	}{
		{
			name:    "claim provable from a FINISHED same-epoch record",
			payload: payload("E1", []string{"s1"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", fin, []string{"s1"}, nil, "v1")},
		},
		{
			// SWAPPING is "completed" for inheritance but still ACTIVE for the
			// overlap rule, which fires first — a new drop never lands next to
			// a SWAPPING sibling, so the guard only ever vouches off FINISHED.
			name:    "SWAPPING record hits the in-flight overlap rule before the guard",
			payload: payload("E1", []string{"s1"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", distributedtask.TaskStatusSwapping, []string{"s1"}, nil, "v1")},
			wantErr: "already in flight",
		},
		{
			name:    "claim provable from the union of several records (units + cleaned)",
			payload: payload("E1", []string{"s1", "s2"}),
			tasks: []*distributedtask.Task{
				record("t1", "E1", fin, []string{"s1"}, nil, "v1"),
				record("t2", "E1", fin, nil, []string{"s2"}, "v1"),
			},
		},
		{
			name:    "empty claim needs no source",
			payload: payload("E1", nil),
		},
		{
			name:    "no surviving records: the wiped-records interleaving is refused",
			payload: payload("E1", []string{"s1"}),
			wantErr: "no surviving source record",
		},
		{
			name:    "another epoch's records do not vouch",
			payload: payload("E2", []string{"s1"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", fin, []string{"s1"}, nil, "v1")},
			wantErr: "no surviving source record",
		},
		{
			name:    "another target set's records do not vouch",
			payload: payload("E1", []string{"s1"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", fin, []string{"s1"}, nil, "v1", "v2")},
			wantErr: "no surviving source record",
		},
		{
			name:    "FAILED records do not vouch (inheritance is completed-only)",
			payload: payload("E1", []string{"s1"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", distributedtask.TaskStatusFailed, []string{"s1"}, nil, "v1")},
			wantErr: "no surviving source record",
		},
		{
			name:    "a corrupt record is skipped, a valid sibling still vouches",
			payload: payload("E1", []string{"s1"}),
			tasks: []*distributedtask.Task{
				corruptDropTask("bad", fin),
				record("t1", "E1", fin, []string{"s1"}, nil, "v1"),
			},
		},
		{
			name:    "partially provable claim is refused, not trimmed",
			payload: payload("E1", []string{"s1", "s2"}),
			tasks:   []*distributedtask.Task{record("t1", "E1", fin, []string{"s1"}, nil, "v1")},
			wantErr: "no surviving source record",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.CheckConflict(tt.payload, tt.tasks)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestCheckClassMutation_DoesNotBlockDeleteDuringDrop(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	// DeleteClass supersedes an in-flight drop (the whole bucket is going away);
	// the schema FSM cascade-deletes the task, so the guard must not block it.
	require.NoError(t, p.CheckClassMutation("C", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
	require.NoError(t, p.CheckClassMutation("Other", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
}

func finishedDropTask(id, collection string, targets ...string) *distributedtask.Task {
	t := activeDropTask(id, collection, targets...)
	t.Status = distributedtask.TaskStatusFinished
	return t
}

func corruptDropTask(id string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        []byte("not json"),
		Status:         status,
	}
}

// swappingDropTaskCovering is swappingDropTask with units covering the given shards.
func swappingDropTaskCovering(id, collection string, shards []string, targets ...string) *distributedtask.Task {
	task := finishedDropTaskCovering(id, collection, shards, targets...)
	task.Status = distributedtask.TaskStatusSwapping
	return task
}

// finishedDropTaskCovering is finishedDropTask with units covering the given shards.
func finishedDropTaskCovering(id, collection string, shards []string, targets ...string) *distributedtask.Task {
	payload := &DropVectorIndexTaskPayload{
		Collection: collection, Targets: targets, OpID: "op-" + id,
		UnitToShard: map[string]string{},
	}
	for i, shard := range shards {
		payload.UnitToShard[fmt.Sprintf("%s__u%d", shard, i)] = shard
	}
	enc, _ := payload.encode()
	return &distributedtask.Task{
		Namespace:      DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        enc,
		Status:         distributedtask.TaskStatusFinished,
	}
}

func TestCheckVectorConfigRemoval_GatesOnFinished(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())

	t.Run("finished task never vouches (stale-record replay safety)", func(t *testing.T) {
		// A FINISHED record outlives finalize by the task TTL; after a
		// re-create + re-drop it would remove the new drop's marker.
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{finishedDropTask("t1", "C", "v1", "v2")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "only the completing cleanup task")
	})

	t.Run("collection matches case-insensitively, target exact-case only", func(t *testing.T) {
		require.NoError(t, p.CheckVectorConfigRemoval("c", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1")}),
			"collection names are case-insensitive")
		require.Error(t, p.CheckVectorConfigRemoval("C", []string{"V1"}, []string{"s1"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1")}),
			"a case-differing sibling is a DIFFERENT vector; a voucher for v1 must not vouch for V1")
	})

	t.Run("swapping task covering the vector allows removal", func(t *testing.T) {
		// OnTaskCompleted fires at SWAPPING; rejecting it self-blocks the finalizer.
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1")})
		require.NoError(t, err)
	})

	t.Run("corrupt swapping task does not vouch", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{corruptDropTask("t1", distributedtask.TaskStatusSwapping)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "only the completing cleanup task")
	})

	t.Run("corrupt active task does not block a valid voucher", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{
				corruptDropTask("t1", distributedtask.TaskStatusStarted),
				swappingDropTaskCovering("t2", "C", []string{"s1"}, "v1"),
			})
		require.NoError(t, err)
	})

	t.Run("active (not finished) task rejects removal", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, nil,
			[]*distributedtask.Task{activeDropTask("t1", "C", "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "still active")
	})

	t.Run("newer active task blocks a replayed old task's removal", func(t *testing.T) {
		// Epoch-blindness: an old FINISHED task covers v1, but a newer drop of the
		// re-used name is running — removal would free the name mid-cleanup.
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, nil,
			[]*distributedtask.Task{finishedDropTask("t1", "C", "v1"), activeDropTask("t2", "C", "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "still active")
	})

	t.Run("no task at all rejects removal (manual PATCH to skip cleanup)", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"}, nil)
		require.Error(t, err)
	})

	t.Run("finished task on a different collection does not vouch", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{finishedDropTask("t1", "Other", "v1")})
		require.Error(t, err)
	})

	t.Run("finished task not covering the vector does not vouch", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1"},
			[]*distributedtask.Task{finishedDropTask("t1", "C", "v9")})
		require.Error(t, err)
	})

	t.Run("every removed vector must be covered", func(t *testing.T) {
		// v1 has a valid voucher and passes; the rejection must come from v2,
		// which no task covers.
		err := p.CheckVectorConfigRemoval("C", []string{"v1", "v2"}, []string{"s1"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), `"v2"`)
	})

	t.Run("empty removal list is a no-op", func(t *testing.T) {
		require.NoError(t, p.CheckVectorConfigRemoval("C", nil, nil, nil))
	})

	t.Run("empty shard set allows removal (MT with every tenant deleted)", func(t *testing.T) {
		// With no shards there is no data to strand, no active shard to ever
		// enqueue a cleanup for, and thus no SWAPPING voucher forthcoming —
		// requiring one would wedge the marker until a dummy tenant or
		// DeleteClass.
		require.NoError(t, p.CheckVectorConfigRemoval("C", []string{"v1"}, nil, nil))
		require.NoError(t, p.CheckVectorConfigRemoval("C", []string{"v1"}, nil,
			[]*distributedtask.Task{finishedDropTask("t1", "C", "v1")}),
			"stale records do not block the tenant-less removal")
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, nil,
			[]*distributedtask.Task{activeDropTask("t1", "C", "v1")})
		require.Error(t, err, "a still-active cleanup blocks removal even with no shards")
		require.Contains(t, err.Error(), "still active")
	})

	// Coverage half of the gate: a completed task vouches only if its units
	// covered every current shard — a FINISHED task that deferred finalize over
	// a cold tenant must not double as a removal voucher.
	t.Run("swapping task covering all shards vouches", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1", "s2"}, "v1")})
		require.NoError(t, err)
	})

	t.Run("finished task covering all shards still does not vouch", func(t *testing.T) {
		// The re-drop repro's manual-removal half: a closed epoch's FINISHED
		// record covers everything yet must not remove a newer drop's marker.
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2"},
			[]*distributedtask.Task{finishedDropTaskCovering("t1", "C", []string{"s1", "s2"}, "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "only the completing cleanup task")
	})

	t.Run("swapping task with an uncovered shard does not vouch", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2", "s3"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1", "s2"}, "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not covered")
	})

	t.Run("uncovered-shard error carries a count and no shard names", func(t *testing.T) {
		// The error reaches the HTTP body of a caller with only
		// collection-update rights; on MT collections shard names are tenant
		// names (otherwise gated behind ShardsMetadata READ), and a shifting
		// sorted sample would let repeat calls enumerate past any cap — no
		// name may appear. Operators get the sample from the server-side log.
		shards := make([]string, 0, 15)
		for i := 1; i <= 15; i++ {
			shards = append(shards, fmt.Sprintf("tenant%02d", i))
		}
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, shards,
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", nil, "v1")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "15 shards")
		require.NotContains(t, err.Error(), "tenant")
	})

	t.Run("any single swapping task covering everything vouches", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2"},
			[]*distributedtask.Task{
				finishedDropTaskCovering("t1", "C", []string{"s1"}, "v1"),
				swappingDropTaskCovering("t2", "C", []string{"s1", "s2"}, "v1"),
			})
		require.NoError(t, err)
	})

	t.Run("partial coverage across tasks does not combine", func(t *testing.T) {
		// Mirrors the finalize deferral: a single task must cover everyone —
		// cross-task memory travels as that task's CleanedShards, not by
		// unioning arbitrary records here.
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2"},
			[]*distributedtask.Task{
				swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1"),
				swappingDropTaskCovering("t2", "C", []string{"s2"}, "v1"),
			})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not covered")
	})

	t.Run("the closest task's missing-shard count is reported", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2", "s3"},
			[]*distributedtask.Task{
				swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1"),       // misses s2, s3
				swappingDropTaskCovering("t2", "C", []string{"s1", "s2"}, "v1"), // misses only s3
			})
		require.Error(t, err)
		require.Contains(t, err.Error(), "1 shards", "the closest task misses only s3")
		require.NotContains(t, err.Error(), "s2")
		require.NotContains(t, err.Error(), "s3")
	})

	t.Run("a multi-target voucher covers each removed vector", func(t *testing.T) {
		err := p.CheckVectorConfigRemoval("C", []string{"v1", "v2"}, []string{"s1"},
			[]*distributedtask.Task{swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1", "v2")})
		require.NoError(t, err)
	})

	t.Run("a task's inherited CleanedShards count as coverage", func(t *testing.T) {
		task := swappingDropTaskCovering("t1", "C", []string{"s1"}, "v1")
		payload := &DropVectorIndexTaskPayload{
			Collection: "C", Targets: []string{"v1"}, OpID: "op-t1",
			UnitToShard:   map[string]string{"s1__u0": "s1"},
			CleanedShards: []string{"s2"},
		}
		task.Payload, _ = payload.encode()
		err := p.CheckVectorConfigRemoval("C", []string{"v1"}, []string{"s1", "s2"},
			[]*distributedtask.Task{task})
		require.NoError(t, err, "units plus the inherited cleaned set span all shards")
	})
}

func TestCheckTenantMutation_BlocksDuringDrop(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	require.Error(t, p.CheckTenantMutation("C", []string{"tenant1"}, []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
	require.NoError(t, p.CheckTenantMutation("Other", []string{"tenant1"}, []*distributedtask.Task{activeDropTask("t1", "C", "v1")}),
		"a drop on another collection must not block this one's tenant mutations")
}

func TestCheckPropertyUpdate_NeverConflicts(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	require.NoError(t, p.CheckPropertyUpdate("C", "someProp", []*distributedtask.Task{activeDropTask("t1", "C", "v1")}))
}

// TestDecodeDropVectorIndexPayload_Rejects pins the only guard before
// removeVectorIndexFiles' os.RemoveAll (path-traversal targets) plus the
// missing-field checks.
func TestDecodeDropVectorIndexPayload_Rejects(t *testing.T) {
	valid := DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v1"}, OpID: "op"}
	enc := func(p DropVectorIndexTaskPayload) []byte { b, _ := p.encode(); return b }

	cases := map[string][]byte{
		"not json":           []byte("{"),
		"missing collection": enc(DropVectorIndexTaskPayload{Targets: []string{"v1"}, OpID: "op"}),
		"missing targets":    enc(DropVectorIndexTaskPayload{Collection: "C", OpID: "op"}),
		"missing opID":       enc(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v1"}}),
		"empty target":       enc(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{""}, OpID: "op"}),
		"slash target":       enc(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"a/b"}, OpID: "op"}),
		"backslash target":   enc(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{`a\b`}, OpID: "op"}),
		"dotdot target":      enc(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"../x"}, OpID: "op"}),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeDropVectorIndexPayload(payload)
			require.Error(t, err)
		})
	}

	_, err := decodeDropVectorIndexPayload(enc(valid))
	require.NoError(t, err, "a valid payload must decode")
}

// TestCheckConflict_SkipsCorruptActiveTask pins the fail-open behavior: a corrupt
// active task must not block a new drop (it can't be matched to a collection).
func TestCheckConflict_SkipsCorruptActiveTask(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	newPayload, _ := (&DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v1"}, OpID: "new"}).encode()
	corrupt := &distributedtask.Task{
		Namespace: DropVectorIndexNamespace,
		Payload:   []byte("not json"),
		Status:    distributedtask.TaskStatusStarted,
	}
	require.NoError(t, p.CheckConflict(newPayload, []*distributedtask.Task{corrupt}))
}

// TestCheckTenantMutation_SkipsCorruptActiveTask: likewise a corrupt task must not
// block every tenant mutation cluster-wide.
func TestCheckTenantMutation_SkipsCorruptActiveTask(t *testing.T) {
	p := newTestDropProvider(&fakeShards{}, &fakeFinalizer{}, newFakeRecorder())
	corrupt := &distributedtask.Task{
		Namespace: DropVectorIndexNamespace,
		Payload:   []byte("not json"),
		Status:    distributedtask.TaskStatusStarted,
	}
	require.NoError(t, p.CheckTenantMutation("C", []string{"t1"}, []*distributedtask.Task{corrupt}))
}

func TestExtractDropVectorIndexTaskCollection(t *testing.T) {
	enc, _ := (&DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v"}, OpID: "op"}).encode()
	got, ok := ExtractDropVectorIndexTaskCollection(enc)
	require.True(t, ok)
	require.Equal(t, "C", got)

	_, ok = ExtractDropVectorIndexTaskCollection([]byte("not json"))
	require.False(t, ok)
}

func TestExtractDropVectorIndexTaskTargets(t *testing.T) {
	enc, _ := (&DropVectorIndexTaskPayload{Collection: "C", Targets: []string{"v1", "v2"}, OpID: "op"}).encode()
	collection, targets, ok := ExtractDropVectorIndexTaskTargets(enc)
	require.True(t, ok)
	require.Equal(t, "C", collection)
	require.Equal(t, []string{"v1", "v2"}, targets)

	_, _, ok = ExtractDropVectorIndexTaskTargets([]byte("not json"))
	require.False(t, ok, "an unparseable payload must not match any purge")
}
