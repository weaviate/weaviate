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

package rest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/backup"
)

// ctxAwareTaskService is the task service a real RAFT client behaves like: both
// calls fail on a dead context instead of ignoring it.
type ctxAwareTaskService struct {
	mu           sync.Mutex
	tasks        []*distributedtask.Task
	cancelled    []distributedtask.TaskDescriptor
	listHadDeadl bool
	listCtxErr   error
}

func (s *ctxAwareTaskService) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	s.mu.Lock()
	_, s.listHadDeadl = ctx.Deadline()
	s.listCtxErr = ctx.Err()
	out := make([]*distributedtask.Task, len(s.tasks))
	copy(out, s.tasks)
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return map[string][]*distributedtask.Task{db.ReindexNamespace: out}, nil
}

func (s *ctxAwareTaskService) CancelDistributedTask(ctx context.Context, _, taskID string, version uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = append(s.cancelled, distributedtask.TaskDescriptor{ID: taskID, Version: version})
	for _, t := range s.tasks {
		if t.ID == taskID {
			t.Status = distributedtask.TaskStatusCancelled
		}
	}
	return nil
}

func (s *ctxAwareTaskService) AddDistributedTaskWithBarrier(context.Context, string, string, any, []string, bool) error {
	return nil
}

func (s *ctxAwareTaskService) AddDistributedTaskWithGroupsBarrier(context.Context, string, string, any, []distributedtask.UnitSpec, bool) error {
	return nil
}

// A dead request context must still roll the task back; see
// rollbackRacedReindexTask for why the two conditions coincide.
func TestRollbackRacedReindexTaskSurvivesRequestCancellation(t *testing.T) {
	const (
		taskID     = "Movies:rebuild-filterable:title:ab3f"
		collection = "Movies"
		property   = "title"
	)

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	svc := &ctxAwareTaskService{tasks: []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
	}}}
	h := &indexesHandlers{
		appState: &state.State{Logger: logger},
		tasks:    svc,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h.rollbackRacedReindexTask(ctx, taskID, collection, property)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	require.NoError(t, svc.listCtxErr,
		"the rollback must not inherit the caller's cancellation")
	require.True(t, svc.listHadDeadl,
		"a detached rollback still has to be bounded, or it outlives the request forever")
	require.Len(t, svc.cancelled, 1,
		"the raced task must still be cancelled after the caller disconnected")
	require.Equal(t, taskID, svc.cancelled[0].ID)
	require.Equal(t, distributedtask.TaskStatusCancelled, svc.tasks[0].Status)
	require.NotNil(t, warned(hook, "rollback: cancelled a reindex task"))
}

// scriptedRollbackService answers the two cluster calls the rollback makes from
// a fixed script and counts them.
type scriptedRollbackService struct {
	mu          sync.Mutex
	tasks       []*distributedtask.Task
	listErr     error
	cancelErr   error
	listCalls   int
	cancelCalls int
	// failFirstN makes the first N cancels fail, so the retry can be observed
	// succeeding rather than only exhausting itself.
	failFirstN int
}

func (s *scriptedRollbackService) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	if s.listErr != nil {
		return nil, s.listErr
	}
	return map[string][]*distributedtask.Task{db.ReindexNamespace: s.tasks}, nil
}

func (s *scriptedRollbackService) CancelDistributedTask(context.Context, string, string, uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelCalls++
	if s.cancelCalls <= s.failFirstN {
		return errors.New("raft: leader election in progress")
	}
	return s.cancelErr
}

func (s *scriptedRollbackService) AddDistributedTaskWithBarrier(context.Context, string, string, any, []string, bool) error {
	return nil
}

func (s *scriptedRollbackService) AddDistributedTaskWithGroupsBarrier(context.Context, string, string, any, []distributedtask.UnitSpec, bool) error {
	return nil
}

// Every outcome must leave exactly one audit line, at the right level: it is
// all the operator gets, since none of them fails the request.
func TestRollbackRacedReindexTaskOutcomes(t *testing.T) {
	const (
		taskID     = "Movies:rebuild-filterable:title:ab3f"
		collection = "Movies"
		property   = "title"
	)
	liveTask := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
	}}

	tests := []struct {
		name                string
		svc                 *scriptedRollbackService
		expectedListCalls   int
		expectedCancelCalls int
		expectedLevel       logrus.Level
		expectedMessage     string
		expectedAudit       string
	}{
		{
			name: "the listing fails, so the task to cancel is never identified",
			svc: &scriptedRollbackService{
				tasks:   liveTask,
				listErr: errors.New("raft: not leader"),
			},
			expectedListCalls:   reindexRollbackAttempts,
			expectedCancelCalls: 0,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			name: "the cancel itself fails",
			svc: &scriptedRollbackService{
				tasks:     liveTask,
				cancelErr: errors.New("raft: timeout"),
			},
			expectedListCalls:   reindexRollbackAttempts,
			expectedCancelCalls: reindexRollbackAttempts,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			name:                "the task is no longer in the listing",
			svc:                 &scriptedRollbackService{},
			expectedListCalls:   1,
			expectedCancelCalls: 0,
			expectedLevel:       logrus.WarnLevel,
			expectedMessage:     "rollback: the task was already gone",
			expectedAudit:       "reindex_task_rolled_back",
		},
		{
			name:                "the task is cancelled",
			svc:                 &scriptedRollbackService{tasks: liveTask},
			expectedListCalls:   1,
			expectedCancelCalls: 1,
			expectedLevel:       logrus.InfoLevel,
			expectedMessage:     "rollback: cancelled a reindex task that raced a backup claim",
			expectedAudit:       "reindex_task_rolled_back",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h := &indexesHandlers{
				appState: &state.State{Logger: logger},
				tasks:    test.svc,
			}

			h.rollbackRacedReindexTask(context.Background(), taskID, collection, property)

			test.svc.mu.Lock()
			defer test.svc.mu.Unlock()
			require.Equal(t, test.expectedListCalls, test.svc.listCalls,
				"a failing rollback is retried; a settled one is not")
			require.Equal(t, test.expectedCancelCalls, test.svc.cancelCalls)

			entries := hook.AllEntries()
			require.Len(t, entries, 1, "every outcome is worth exactly one audit line")
			require.Contains(t, entries[0].Message, test.expectedMessage)
			require.Equal(t, test.expectedLevel, entries[0].Level)
			require.Equal(t, test.expectedAudit, entries[0].Data["audit_event"])
			require.Equal(t, taskID, entries[0].Data["taskID"])
			require.Equal(t, collection, entries[0].Data["collection"])
			require.Equal(t, property, entries[0].Data["property"])
		})
	}
}

// gateWatchProber samples the cleanup gate at each backup probe — the point
// where a concurrent backup would be admitted or refused.
type gateWatchProber struct {
	mu         sync.Mutex
	provider   *db.ReindexProvider
	collection string
	gateAtCall []bool
	holdAtCall []db.ReindexHold
}

func (p *gateWatchProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	p.mu.Lock()
	p.gateAtCall = append(p.gateAtCall, p.provider.AnyCleanupInProgressForCollection(p.collection))
	p.holdAtCall = append(p.holdAtCall, p.provider.HoldForShard(p.collection, "shard1"))
	p.mu.Unlock()
	return backup.NodeActivity{}, nil
}

// holds mirrors calls, but records which hold covered the shard.
func (p *gateWatchProber) holds() []db.ReindexHold {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]db.ReindexHold, len(p.holdAtCall))
	copy(out, p.holdAtCall)
	return out
}

func (p *gateWatchProber) calls() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]bool, len(p.gateAtCall))
	copy(out, p.gateAtCall)
	return out
}

// The gate must enclose the pre-submit deletion; see updateIndex's call site.
// The fixture's DB holds no index, so the window is asserted rather than the
// loop: open at the first probe, closed at the commit and the second probe,
// open again once the handler returns.
func TestUpdateIndexHoldsCleanupGateAroundPreSubmitCleanup(t *testing.T) {
	const collection = "Movies"

	logger, _ := logrustest.NewNullLogger()
	provider := db.NewReindexProvider(nil, nil, logger, "node1",
		func() int { return 1 }, context.Background())

	svc := &raceTaskService{}
	prober := &gateWatchProber{provider: provider, collection: collection}

	var gateAtCommit atomic.Bool
	var committed atomic.Bool
	svc.onCommitted = func() {
		committed.Store(true)
		gateAtCommit.Store(provider.AnyCleanupInProgressForCollection(collection))
	}

	h := submissionHandlers(t, svc, prober)
	h.appState.ReindexProvider = provider

	require.False(t, provider.AnyCleanupInProgressForCollection(collection))

	responder := submitReindex(h)
	require.NotNil(t, responder)

	require.True(t, committed.Load(), "the task must have been committed")
	require.True(t, gateAtCommit.Load(),
		"the cleanup gate must still be closed when the task is committed, "+
			"so the deletion and the commit are one window and not two")

	probes := prober.calls()
	require.Len(t, probes, 2, "submission probes for backups before and after the commit")
	require.False(t, probes[0],
		"nothing is being deleted yet at the first probe")
	require.True(t, probes[1],
		"a backup probing here would be looking straight at the deletion; "+
			"it must see the gate closed")

	holds := prober.holds()
	require.Equal(t, db.ReindexHoldSubmit, holds[1],
		"an ordinary submission must refuse backups as a submission, not as a "+
			"cancelled migration that never happened")

	require.False(t, provider.AnyCleanupInProgressForCollection(collection),
		"the gate must be released once the handler returns")
}

// starvationProber models a slow owner that burns its whole budget and a fast
// one that answers only after the slow owner has been reached — sequentially,
// one of the two is always asked on an already-dead context.
type starvationProber struct {
	slow       string
	slowProbed chan struct{}
	slowOnce   sync.Once
}

func (p *starvationProber) CleanupInProgress(ctx context.Context, node, _ string) (bool, error) {
	if node == p.slow {
		p.slowOnce.Do(func() { close(p.slowProbed) })
		<-ctx.Done()
		return false, ctx.Err()
	}
	select {
	case <-p.slowProbed:
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// One owner that cannot answer must not cost the others their answer, or the
// degraded warning names healthy nodes on every cancel.
func TestAwaitOwnerCleanupGatesGivesEachOwnerItsOwnBudget(t *testing.T) {
	const (
		local      = "node1"
		slowOwner  = "node2"
		fastOwner  = "node3"
		collection = "Movies"
	)

	prober := &starvationProber{slow: slowOwner, slowProbed: make(chan struct{})}
	h, hook := gateHandlers(prober, local, slowOwner, fastOwner)

	// Keeps the test short. reindexOwnerGateTimeout is 5s of wall time, and
	// what is under test is the fan-out shape, not the budget's size.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	h.awaitOwnerCleanupGates(ctx, &db.ReindexTaskPayload{
		Collection: collection,
		UnitToNode: map[string]string{"u1": slowOwner, "u2": fastOwner, "u3": local},
	}, collection, "task-1")

	entry := warned(hook, "could not confirm")
	require.NotNil(t, entry, "the owner that never answered has to be visible to the operator")
	degraded, ok := entry.Data["nodes"].(map[string]string)
	require.True(t, ok)
	require.Contains(t, degraded, slowOwner)
	require.NotContains(t, degraded, fastOwner,
		"a healthy owner must not be reported degraded because another owner was slow")
}

// clearThenUnreachableProber answers the pre-commit probe and fails every probe
// after it, which is what a client disconnecting mid-submission looks like: the
// task is committed, then nothing can be reached to confirm anything.
type clearThenUnreachableProber struct {
	mu    sync.Mutex
	calls int
}

func (p *clearThenUnreachableProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls == 1 {
		return backup.NodeActivity{}, nil
	}
	return backup.NodeActivity{}, errors.New("dial tcp: connection refused")
}

// "Nobody answered" is not evidence that a backup claimed the slot, and a client
// disconnect produces exactly that verdict on every node. Rolling back on it
// destroys a cleanly committed migration at the one moment the caller is no
// longer there to resubmit — so the task must survive an unconfirmed probe.
func TestUpdateIndexKeepsTheTaskWhenThePostCommitProbeCannotConfirm(t *testing.T) {
	svc := &raceTaskService{}
	h := submissionHandlers(t, svc, &clearThenUnreachableProber{})

	responder := submitReindex(h)

	_, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "an unconfirmable probe must answer 503, got %T", responder)
	require.Equal(t, 1, svc.adds, "the task must have been committed, or the post-commit probe is untested")
	require.Empty(t, svc.cancelled,
		"an unreachable verdict is not proof of a backup; the committed migration must not be rolled back")
}

// disconnectingProber answers the pre-commit probe clear and kills the request
// context at the post-commit probe, in the same instant it delivers its verdict.
type disconnectingProber struct {
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
	// busy is the post-commit verdict: a node reporting a backup, or nobody
	// answering at all.
	busy bool
}

func (p *disconnectingProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls == 1 {
		return backup.NodeActivity{}, nil
	}
	p.cancel()
	if p.busy {
		return backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1"}, nil
	}
	return backup.NodeActivity{}, errors.New("dial tcp: connection refused")
}

// The two verdicts are deliberately opposite under the same client disconnect.
// A node that reports a backup answered before the context died: certain
// evidence that the migration is committed on top of a live backup, so it is
// rolled back even though nobody is left to hear the refusal. "Nobody answered"
// is what a disconnect alone produces, and rolling back on it would destroy a
// cleanly committed migration; the backup side's commit-time overlap check is
// what refuses that pairing instead.
func TestUpdateIndexPostCommitVerdictSurvivesClientDisconnect(t *testing.T) {
	tests := []struct {
		name           string
		busy           bool
		wantRolledBack bool
	}{
		{name: "a node reports a backup as the caller disconnects", busy: true, wantRolledBack: true},
		{name: "nobody answers as the caller disconnects", busy: false, wantRolledBack: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			svc := &raceTaskService{}
			prober := &disconnectingProber{busy: test.busy}
			h := submissionHandlers(t, svc, prober)
			h.appState.Logger = logger

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prober.cancel = cancel

			responder := submitReindexOn(h, ctx)

			require.Equal(t, 1, svc.adds, "the task must be committed, or the post-commit probe is untested")
			require.Equal(t, context.Canceled, ctx.Err(), "the caller has to be gone by the time the verdict lands")

			if test.wantRolledBack {
				_, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "a confirmed backup must be refused with 409, got %T", responder)
				require.Len(t, svc.cancelled, 1,
					"a node positively reported a backup; the committed migration must be rolled back "+
						"even though the caller disconnected")
				require.Empty(t, svc.startedTasks())
				for _, entry := range hook.AllEntries() {
					require.NotContains(t, entry.Message, "could not confirm the cluster is free of backups",
						"the probe confirmed the opposite; reporting it as unconfirmed misleads the operator")
				}
				return
			}

			unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
			require.Truef(t, ok, "an unconfirmable probe must answer 503, got %T", responder)
			require.Empty(t, svc.cancelled,
				"an unreachable verdict is not proof of a backup; the committed migration must not be rolled back")
			started := svc.startedTasks()
			require.Len(t, started, 1)
			require.Contains(t, errorMessage(t, unavailable.Payload), started[0].ID,
				"the migration is running; without its id the caller's retry is answered 409 for a task it never heard of")
		})
	}
}

// The retry exists for the transient case, so the transient case is what has to
// be pinned: a cancel that fails once and then lands must leave the task
// cancelled and report success, not the give-up line. It must also wait between
// attempts — the transient it retries (a RAFT leader election) lasts seconds
// while failing in microseconds, so three immediate attempts would all fail
// inside the same millisecond.
func TestRollbackRacedReindexTaskSucceedsOnRetry(t *testing.T) {
	const (
		taskID     = "Movies:change-tokenization:title:ab12"
		collection = "Movies"
		property   = "title"
	)

	svc := &scriptedRollbackService{
		tasks: []*distributedtask.Task{{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
			Namespace:      db.ReindexNamespace,
			Status:         distributedtask.TaskStatusStarted,
		}},
		failFirstN: 1,
	}

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	h := &indexesHandlers{appState: &state.State{Logger: logger}, tasks: svc}

	start := time.Now()
	h.rollbackRacedReindexTask(context.Background(), taskID, collection, property)
	elapsed := time.Since(start)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	require.Equal(t, 2, svc.cancelCalls, "the first cancel failed, so a second must have been made")
	// The delay is jittered down to half the base, so half is the floor.
	require.Greater(t, elapsed, reindexRollbackRetryDelay/2,
		"the second attempt must be spaced from the first, or the retry never outlives the transient it exists for")

	entries := hook.AllEntries()
	require.Len(t, entries, 1, "one outcome, one audit line")
	require.Contains(t, entries[0].Message, "rollback: cancelled a reindex task that raced a backup claim")
	require.Equal(t, logrus.InfoLevel, entries[0].Level,
		"a rollback that succeeded on retry is not an operator problem")
}

// countingProber records whether the gate probed the cluster at all.
type countingProber struct {
	mu    sync.Mutex
	calls int
}

func (p *countingProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return backup.NodeActivity{}, nil
}

// With RUNTIME_REINDEX_ENABLED off there is no reindex to gate, so the
// submission is refused before any of this branch's machinery runs. Pinned
// because the flag and the gate landed from different directions onto the same
// handler, and "off" must not mean "off except for the new cluster probes".
func TestUpdateIndexWithRuntimeReindexDisabledSkipsTheGate(t *testing.T) {
	svc := &raceTaskService{}
	prober := &countingProber{}
	h := submissionHandlers(t, svc, prober)
	h.appState.ServerConfig.Config.RuntimeReindexEnabled = false

	responder := submitReindex(h)

	_, ok := responder.(*schema.SchemaObjectsIndexesUpdateBadRequest)
	require.Truef(t, ok, "a disabled feature must answer 400, got %T", responder)

	prober.mu.Lock()
	defer prober.mu.Unlock()
	require.Zero(t, prober.calls,
		"the cluster must not be probed for backups when no reindex can start")
	require.Zero(t, svc.adds, "no task may be committed while the feature is off")
}
