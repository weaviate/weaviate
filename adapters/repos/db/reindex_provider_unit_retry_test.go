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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type refusingRecorder struct {
	*fakeRecorder
	failureErr, completionErr, progressErr atomic.Pointer[error]
	refused                                atomic.Int32
	progressPanics, failurePanics          atomic.Bool
	beforeProgressPanic                    func()
}

var errRecorderUnavailable = errors.New("raft applyTimeout")

func refuse(write *atomic.Pointer[error], err error) (lift func()) {
	write.Store(&err)
	return func() { write.Store(nil) }
}

func (r *refusingRecorder) refusal(write *atomic.Pointer[error]) error {
	if err := write.Load(); err != nil {
		r.refused.Add(1)
		return *err
	}
	return nil
}

func (r *refusingRecorder) RecordDistributedTaskUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	if r.failurePanics.Load() {
		panic("failure write panicked")
	}
	if err := r.refusal(&r.failureErr); err != nil {
		return err
	}
	return r.fakeRecorder.RecordDistributedTaskUnitFailure(ctx, namespace, taskID, version, nodeID, unitID, errMsg)
}

func (r *refusingRecorder) RecordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error {
	if err := r.refusal(&r.completionErr); err != nil {
		return err
	}
	return r.fakeRecorder.RecordDistributedTaskUnitCompletion(ctx, namespace, taskID, version, nodeID, unitID)
}

func (r *refusingRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	if r.progressPanics.Load() {
		if r.beforeProgressPanic != nil {
			r.beforeProgressPanic()
		}
		panic("progress write panicked")
	}
	if err := r.refusal(&r.progressErr); err != nil {
		return err
	}
	return r.fakeRecorder.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress)
}

func (r *refusingRecorder) recorded(unitID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, failed := r.failed[unitID]
	return failed || slices.Contains(r.completed, unitID)
}

func newUnitRetryShard(t *testing.T, ctx context.Context) (*Shard, *Index, *ReindexTaskPayload) {
	className := "UnitRetry_" + uuid.NewString()[:8]
	shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{"title"}),
		enthnsw.UserConfig{Skip: true}, true, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })
	registerIndex(idx, className)
	for _, obj := range makeConvergenceTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	return shard, idx, &ReindexTaskPayload{Collection: className, MigrationType: ReindexTypeRepairFilterable, Properties: []string{"title"}}
}

func runUnits(ctx context.Context, p *ReindexProvider, task *distributedtask.Task, payload *ReindexTaskPayload,
	idx *Index, units []string, rec *refusingRecorder,
) (closed func() bool) {
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		p.processUnits(ctx, task, payload, idx, units, rec)
	}, p.logger)
	return func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

func countLines(hook *logrustest.Hook, stem, unitID string) int {
	n := 0
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, stem) && (unitID == "" || e.Data["unit"] == unitID) {
			n++
		}
	}
	return n
}

// A waiting unit keeps its handle open, a started unit retries only its failed write, and a panicking unit
// closes the handle and reaches GoWrapper's recover, which reports it to Sentry.
func TestHandleRetriesItsDeferredUnits(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
	const missing = "gone__node1"
	type fixture struct {
		p         *ReindexProvider
		shard     *Shard
		desc      distributedtask.TaskDescriptor
		tasks     []*ShardReindexTaskGeneric
		rec       *refusingRecorder
		terminate func()
	}
	panics := func(failureToo bool) func(*testing.T, fixture) func() {
		return func(t *testing.T, f fixture) func() {
			f.rec.progressPanics.Store(true)
			f.rec.failurePanics.Store(failureToo)
			return func() {}
		}
	}
	held := func(t *testing.T, f fixture) func() {
		f.shard.migrationMirrors.holdUnarmed(f.tasks)
		return f.shard.migrationMirrors.releaseHeld
	}
	refused := func(write func(*refusingRecorder) *atomic.Pointer[error], err error) func(*testing.T, fixture) func() {
		return func(t *testing.T, f fixture) func() { return refuse(write(f.rec), err) }
	}
	failures := func(r *refusingRecorder) *atomic.Pointer[error] { return &r.failureErr }
	completions := func(r *refusingRecorder) *atomic.Pointer[error] { return &r.completionErr }
	progress := func(r *refusingRecorder) *atomic.Pointer[error] { return &r.progressErr }
	terminal := errors.Join(distributedtask.ErrUnitAlreadyTerminal, distributedtask.ErrPermanentRejection)
	tests := []struct {
		name string
		// subjectMissing names a shard this node lacks, so the subject fails instead of completing.
		subjectMissing bool
		// next is dispatched after the subject at concurrency 1, so it runs only once the subject gives its slot back.
		next      bool
		hold      func(t *testing.T, f fixture) (lift func())
		terminate bool
		// In closes rows, the handle ends without a lift.
		closes bool
		// In claimed rows, the subject waits on a write after it started, so a teardown cannot seal it.
		claimed       bool
		starts, fails int
		// lost is set when the FAILED write itself panics, so nothing is recorded.
		lost   bool
		reason string
		panics int
	}{
		{name: "held shard", next: true, hold: held, starts: 1},
		{name: "held shard, handle terminated", hold: held, terminate: true},
		{name: "teardown seal", hold: func(t *testing.T, f fixture) func() {
			release, sealed := f.p.SealLocalUnit(f.desc, f.shard.migrationUnit())
			require.True(t, sealed)
			return release
		}, starts: 1},
		{name: "failure write refused", subjectMissing: true, hold: refused(failures, errRecorderUnavailable), fails: 1},
		{name: "completion write refused", hold: refused(completions, errRecorderUnavailable), claimed: true, starts: 1},
		{name: "completion write refused, handle terminated", hold: refused(completions, errRecorderUnavailable), claimed: true, terminate: true, starts: 1},
		{name: "initial progress refused", hold: refused(progress, errRecorderUnavailable), claimed: true, starts: 1},
		{name: "completion rejected for good", hold: refused(completions, terminal), closes: true, starts: 1},
		{name: "unit panics", hold: panics(false), closes: true, starts: 1, fails: 1, reason: "progress write panicked", panics: 1},
		{name: "unit panics, failure write panics too", hold: panics(true), closes: true, starts: 1, fails: 1, lost: true, panics: 1},
		{name: "handle terminated, then unit panics", hold: func(t *testing.T, f fixture) func() {
			f.rec.beforeProgressPanic = f.terminate
			return panics(false)(t, f)
		}, closes: true, starts: 1, panics: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(testCtx())
			defer cancel()
			shard, idx, payload := newUnitRetryShard(t, ctx)

			logger, hook := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", func() int { return 1 }, ctx)
			desc := distributedtask.TaskDescriptor{ID: "T_retry", Version: 1}
			subject := shard.migrationUnit()
			if tc.subjectMissing {
				subject = missing
			}
			task := &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusStarted,
				Units: map[string]*distributedtask.Unit{
					shard.migrationUnit(): {ID: shard.migrationUnit(), Shard: shard.Name(), NodeID: "node1"},
					missing:               {ID: missing, Shard: "gone", NodeID: "node1"},
				},
			}
			units := []string{subject}
			if tc.next {
				units = append(units, missing)
			}
			built, err := p.createReindexTasks(desc, shard.migrationUnit(), payload)
			require.NoError(t, err)
			rec := &refusingRecorder{fakeRecorder: newFakeRecorder()}
			lift := tc.hold(t, fixture{p: p, shard: shard, desc: desc, tasks: built, rec: rec, terminate: cancel})
			defer lift()
			sealable := func() bool {
				release, sealed := p.SealLocalUnit(desc, subject)
				if sealed {
					release()
				}
				return sealed
			}

			closed := runUnits(ctx, p, task, payload, idx, units, rec)
			switch {
			case tc.closes:
				require.Eventually(t, closed, time.Second, 10*time.Millisecond, "the handle ends without a lift")
			default:
				if tc.claimed {
					require.Eventually(t, func() bool { return rec.refused.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
				}
				require.Never(t, func() bool { return closed() || sealable() == tc.claimed },
					deferredUnitRetryInterval+time.Second, 50*time.Millisecond,
					"the handle stays open while its unit waits, and a started unit keeps its claim")
				require.False(t, rec.recorded(subject))
				require.Equal(t, tc.next, rec.recorded(missing), "a waiting unit gives its slot back")
				if tc.terminate {
					cancel()
					require.Eventually(t, closed, time.Second, 10*time.Millisecond)
					require.False(t, rec.recorded(subject))
					break
				}
				lift()
				require.Eventually(t, closed, 3*deferredUnitRetryInterval, 50*time.Millisecond)
				require.True(t, rec.recorded(subject), "the retry records the unit")
			}

			require.Equal(t, tc.starts, countLines(hook, "starting unit", subject), "the unit's work runs at most once")
			require.Equal(t, tc.fails, countLines(hook, "unit failed", subject))
			rec.mu.Lock()
			reason, failed := rec.failed[subject]
			rec.mu.Unlock()
			require.Equal(t, tc.fails > 0 && !tc.lost, failed, "a failed unit is recorded FAILED")
			require.Contains(t, reason, tc.reason)
			require.Equal(t, tc.panics, countLines(hook, "Recovered from panic", ""), "a unit panic reaches GoWrapper")
			require.True(t, sealable(), "a closed handle holds no claim")
		})
	}
}

// A recorder outage hits every waiting unit at once, so its retry lines are sampled per node.
func TestRecorderRetryLinesAreSampled(t *testing.T) {
	const units = 15
	ctx, cancel := context.WithCancel(testCtx())
	defer cancel()
	_, idx, payload := newUnitRetryShard(t, ctx)

	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", func() int { return units }, ctx)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_sampled", Version: 1},
		Status: distributedtask.TaskStatusStarted, Units: map[string]*distributedtask.Unit{},
	}
	ids := make([]string, 0, units)
	for i := range units {
		id := fmt.Sprintf("gone%d__node1", i)
		task.Units[id] = &distributedtask.Unit{ID: id, Shard: fmt.Sprintf("gone%d", i), NodeID: "node1"}
		ids = append(ids, id)
	}
	rec := &refusingRecorder{fakeRecorder: newFakeRecorder()}
	refuse(&rec.failureErr, errRecorderUnavailable)

	closed := runUnits(ctx, p, task, payload, idx, ids, rec)
	require.Eventually(t, func() bool { return rec.refused.Load() >= 3*units }, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.Eventually(t, closed, time.Second, 10*time.Millisecond)

	retries := countLines(hook, "failed to record unit failure", "")
	require.NotZero(t, retries)
	require.LessOrEqual(t, retries, maxReportedErrors)
}
