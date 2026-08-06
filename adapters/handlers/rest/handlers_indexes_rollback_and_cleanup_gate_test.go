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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
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
	// statusAfterFailedCancel is applied to every task once a cancel has been
	// refused, reproducing the case the refusal is meant to describe: the task
	// reached that status between the listing and the cancel.
	statusAfterFailedCancel distributedtask.TaskStatus
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
		s.applyStatusAfterFailedCancel()
		return errors.New("raft: leader election in progress")
	}
	if s.cancelErr != nil {
		s.applyStatusAfterFailedCancel()
	}
	return s.cancelErr
}

func (s *scriptedRollbackService) applyStatusAfterFailedCancel() {
	if s.statusAfterFailedCancel == "" {
		return
	}
	for _, t := range s.tasks {
		t.Status = s.statusAfterFailedCancel
	}
}

// permanentRejection reproduces what the FSM actually returns: the specific
// sentinel AND the umbrella, joined. distributedtask.wrapPermanent is
// unexported, and a fixture built from the bare umbrella alone would let a
// classifier that only matches the umbrella look correct.
func permanentRejection(sentinel error, msg string) error {
	return fmt.Errorf("%s: %w", msg,
		errors.Join(sentinel, distributedtask.ErrPermanentRejection))
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
	// A fresh slice per row: the rows below move the task's status, and a
	// shared one would carry that move into the next row.
	taskIn := func(status distributedtask.TaskStatus) []*distributedtask.Task {
		return []*distributedtask.Task{{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
			Namespace:      db.ReindexNamespace,
			Status:         status,
		}}
	}
	liveTask := func() []*distributedtask.Task { return taskIn(distributedtask.TaskStatusStarted) }

	tests := []struct {
		name                string
		svc                 *scriptedRollbackService
		expectedListCalls   int
		expectedCancelCalls int
		expectedLevel       logrus.Level
		expectedMessage     string
		expectedAudit       string
		// minElapsed, when set, is the floor on how long the whole rollback
		// must take. Only the rows that retry have one.
		minElapsed time.Duration
	}{
		{
			name: "the listing fails, so the task to cancel is never identified",
			svc: &scriptedRollbackService{
				tasks:   liveTask(),
				listErr: errors.New("raft: not leader"),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 0,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			name: "the cancel itself fails",
			svc: &scriptedRollbackService{
				tasks:     liveTask(),
				cancelErr: errors.New("raft: timeout"),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 3,
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
			expectedAudit:       "reindex_task_rollback_not_needed",
		},
		{
			// The task finished between the listing and the cancel, so the FSM
			// refuses it. The rollback wanted the task not running and it is
			// not, so the next attempt's listing settles it: no escalation, and
			// no audit line claiming a rollback that never happened.
			name: "the task reaches a terminal status between the listing and the cancel",
			svc: &scriptedRollbackService{
				tasks: liveTask(),
				cancelErr: permanentRejection(distributedtask.ErrTaskNotRunning,
					"[dtm-perm/task-not-running] task reindex/Movies:rebuild-filterable:title:ab3f/3 is no longer running"),
				statusAfterFailedCancel: distributedtask.TaskStatusFinished,
			},
			expectedListCalls:   2,
			expectedCancelCalls: 1,
			expectedLevel:       logrus.InfoLevel,
			expectedMessage:     "rollback: the reindex task that raced a backup claim had already reached a terminal status",
			expectedAudit:       "reindex_task_rollback_already_terminal",
		},
		{
			// PREPARING is not cancellable, so the FSM answers with the same
			// permanent rejection — but the migration is live and will swap and
			// flip the schema. Reading the rejection as "settled" would tell the
			// operator nothing while the submitter was told the submission was
			// refused.
			name: "the cancel is permanently rejected while the task is still preparing",
			svc: &scriptedRollbackService{
				tasks: taskIn(distributedtask.TaskStatusPreparing),
				cancelErr: permanentRejection(distributedtask.ErrTaskNotRunning,
					"[dtm-perm/task-not-running] task reindex/Movies:rebuild-filterable:title:ab3f/3 is no longer running"),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 3,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			// A version that moved under a STARTED task is reported as "does not
			// exist". The task is running; the rollback has to keep trying and,
			// failing that, escalate.
			name: "the cancel is permanently rejected on a version mismatch",
			svc: &scriptedRollbackService{
				tasks: liveTask(),
				cancelErr: permanentRejection(distributedtask.ErrTaskDoesNotExist,
					"[dtm-perm/task-not-exist] task reindex/Movies:rebuild-filterable:title:ab3f/3 does not exist"),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 3,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			// A permanent rejection carrying a marker this build does not know
			// is rehydrated as the bare umbrella. Nothing about it says the task
			// is settled, so it must not be read as such.
			name: "the cancel is permanently rejected with an unrecognized marker",
			svc: &scriptedRollbackService{
				tasks: liveTask(),
				cancelErr: fmt.Errorf("cancel task: %w",
					distributedtask.ErrPermanentRejection),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 3,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			// The listing already reports the task terminal, so there is nothing
			// to cancel and the cluster is never asked.
			name:                "the task is already terminal in the listing",
			svc:                 &scriptedRollbackService{tasks: taskIn(distributedtask.TaskStatusCancelled)},
			expectedListCalls:   1,
			expectedCancelCalls: 0,
			expectedLevel:       logrus.InfoLevel,
			expectedMessage:     "rollback: the reindex task that raced a backup claim had already reached a terminal status",
			expectedAudit:       "reindex_task_rollback_already_terminal",
		},
		{
			// A retryable RAFT failure must still escalate.
			name: "the cancel fails without a permanent rejection",
			svc: &scriptedRollbackService{
				tasks:     liveTask(),
				cancelErr: errors.New("raft: leader election in progress"),
			},
			expectedListCalls:   3,
			expectedCancelCalls: 3,
			expectedLevel:       logrus.ErrorLevel,
			expectedMessage:     "rollback: could not cancel the task in",
			expectedAudit:       "reindex_task_rollback_failed",
		},
		{
			name:                "the task is cancelled",
			svc:                 &scriptedRollbackService{tasks: liveTask()},
			expectedListCalls:   1,
			expectedCancelCalls: 1,
			expectedLevel:       logrus.InfoLevel,
			expectedMessage:     "rollback: cancelled a reindex task that raced a backup claim",
			expectedAudit:       "reindex_task_rolled_back",
		},
		{
			// The transient case the retry exists for. It must report success
			// rather than the give-up line, and it must wait between attempts:
			// a RAFT leader election lasts seconds while failing in
			// microseconds, so three immediate attempts all fail inside the
			// same millisecond.
			name:                "the cancel fails once and then lands",
			svc:                 &scriptedRollbackService{tasks: liveTask(), failFirstN: 1},
			expectedListCalls:   2,
			expectedCancelCalls: 2,
			expectedLevel:       logrus.InfoLevel,
			expectedMessage:     "rollback: cancelled a reindex task that raced a backup claim",
			expectedAudit:       "reindex_task_rolled_back",
			// The base delay is 500ms and the library jitters it down to half,
			// so 250ms is the floor. Spelled out rather than derived from the
			// constant, so that shrinking the constant fails here instead of
			// moving the bar with it.
			minElapsed: 250 * time.Millisecond,
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

			start := time.Now()
			h.rollbackRacedReindexTask(context.Background(), taskID, collection, property)
			elapsed := time.Since(start)

			if test.minElapsed > 0 {
				require.Greater(t, elapsed, test.minElapsed,
					"the second attempt must be spaced from the first, or the retry never "+
						"outlives the transient it exists for")
			}

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

// The gate must enclose the pre-submit deletion AND the probe that decides
// whether the deletion may happen at all; see updateIndex's call site. The
// fixture's DB holds no index, so the window is asserted rather than the loop:
// closed at both probes and at the commit, open again once the handler returns.
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
	h.appState.ReindexProvider.Store(provider)

	require.False(t, provider.AnyCleanupInProgressForCollection(collection))

	responder := submitReindex(h)
	require.NotNil(t, responder)

	require.True(t, committed.Load(), "the task must have been committed")
	require.True(t, gateAtCommit.Load(),
		"the cleanup gate must still be closed when the task is committed, "+
			"so the deletion and the commit are one window and not two")

	probes := prober.calls()
	require.Len(t, probes, 2, "submission probes for backups before and after the commit")
	require.True(t, probes[0],
		"the gate must already be closed at the first probe: the probe fans out "+
			"node by node, so a backup admitted on a node answered early is "+
			"admitted into the deletion this probe is deciding to run")
	require.True(t, probes[1],
		"a backup probing here would be looking straight at the deletion; "+
			"it must see the gate closed")

	holds := prober.holds()
	require.Equal(t, db.ReindexHoldSubmit, holds[0],
		"an ordinary submission must refuse backups as a submission, not as a "+
			"cancelled migration that never happened")
	require.Equal(t, db.ReindexHoldSubmit, holds[1])

	require.False(t, provider.AnyCleanupInProgressForCollection(collection),
		"the gate must be released once the handler returns")
}

// backupDuringScanProber is a backup trying to claim its slot while the
// submission's cluster-wide probe is still running. The probe fans out to every
// node concurrently, so it records which nodes' probes found the gate open —
// each of those is a backup that would have been admitted into the sweep.
type backupDuringScanProber struct {
	provider   *db.ReindexProvider
	collection string

	mu       sync.Mutex
	probes   int
	admitted []string
}

func (p *backupDuringScanProber) NodeActivity(_ context.Context, node string) (backup.NodeActivity, error) {
	p.mu.Lock()
	p.probes++
	if p.provider.HoldForShard(p.collection, "shard1") == db.ReindexHoldNone {
		p.admitted = append(p.admitted, node)
	}
	p.mu.Unlock()
	return backup.NodeActivity{}, nil
}

func (p *backupDuringScanProber) result() (int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.probes, append([]string(nil), p.admitted...)
}

// The submit gate has to close before the probe, not after it. The probe is a
// cluster-wide fan-out, so the whole scan is a window: a backup that claims its
// slot anywhere in it is admitted, and then has its sidecar dirs and its
// .migrations tracker removed underneath it by the sweep the probe was
// authorizing.
//
// The post-commit rollback is not a repair for this. It produces a cancelled
// task no unit was ever claimed on, which the commit-time backstop waives on
// purpose, so a backup admitted here is published as clean.
func TestSubmitGateIsClosedBeforeTheClusterWideProbe(t *testing.T) {
	const collection = "Movies"

	logger, _ := logrustest.NewNullLogger()
	provider := db.NewReindexProvider(nil, nil, logger, "node1",
		func() int { return 1 }, context.Background())

	prober := &backupDuringScanProber{provider: provider, collection: collection}
	h := submissionHandlers(t, &raceTaskService{}, prober)
	h.appState.ReindexProvider.Store(provider)
	h.cluster = fixedMembership{"node1", "node2", "node3"}

	require.NotNil(t, submitReindex(h))

	probes, admitted := prober.result()
	require.Equal(t, 6, probes,
		"both probes fan out over all three nodes")
	require.Empty(t, admitted,
		"no node may report the collection free of reindex submissions while one "+
			"is in flight; every node listed here is a backup that would have been "+
			"captured across the pre-submit deletion")
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

// An exhausted backoff means stop, not "wait zero": backoff.Stop is a negative
// duration, and a timer built from it fires at once, turning the give-up into an
// immediate retry.
func TestWaitBeforeRollbackRetryStopsOnExhaustedBackoff(t *testing.T) {
	require.False(t, waitBeforeRollbackRetry(context.Background(), &backoff.StopBackOff{}),
		"a backoff that has nothing left to give must end the retry loop")
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
// submission is refused before any of the gate's machinery runs. Pinned because
// the flag check and the gate sit in the same handler and are easy to reorder,
// and "off" must not mean "off except for the cluster probes".
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

// rollbackObservingService reports what the submission still holds at the
// moment the rollback cancels the task. CancelDistributedTask is only reached
// from the rollback, so it is an unambiguous hook for that instant.
type rollbackObservingService struct {
	*raceTaskService
	onCancel func()
}

func (s *rollbackObservingService) CancelDistributedTask(
	ctx context.Context, namespace, taskID string, version uint64,
) error {
	s.onCancel()
	return s.raceTaskService.CancelDistributedTask(ctx, namespace, taskID, version)
}

// A rollback takes up to reindexRollbackTimeout, and the client that triggered
// it has usually disconnected — so nobody is waiting for it, yet an unrelated
// DELETE on the same property and every backup of the collection would wait for
// it if it ran under the submit lock and the submit gate. The committed task is
// what the backup gate reads from here on; neither hold protects anything, so
// both must be back before the rollback starts.
func TestUpdateIndexRollbackRunsWithoutTheSubmitHolds(t *testing.T) {
	const (
		collection = "Movies"
		property   = "title"
		shard      = "shard1"
	)

	svc := &rollbackObservingService{raceTaskService: &raceTaskService{}}
	prober := &disconnectingProber{busy: true}
	h := submissionHandlers(t, svc, prober)

	provider := &db.ReindexProvider{}
	h.appState.ReindexProvider.Store(provider)

	var (
		rolledBack   atomic.Bool
		lockWasFree  atomic.Bool
		gateAtCancel atomic.Int32
	)
	propLock := h.submitLock(collection, property)
	svc.onCancel = func() {
		rolledBack.Store(true)
		if propLock.TryLock() {
			lockWasFree.Store(true)
			propLock.Unlock()
		}
		gateAtCancel.Store(int32(provider.HoldForShard(collection, shard)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prober.cancel = cancel

	responder := submitReindexOn(h, ctx)

	_, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a confirmed backup must be refused with 409, got %T", responder)
	require.Equal(t, context.Canceled, ctx.Err(), "the caller has to be gone by the time the rollback runs")
	require.True(t, rolledBack.Load(), "the rollback must run, or this test pins nothing")

	require.True(t, lockWasFree.Load(),
		"the submit lock is still held during the rollback; a DELETE on this property waits out "+
			"a rollback nobody is listening for")
	require.Equal(t, db.ReindexHoldNone, db.ReindexHold(gateAtCancel.Load()),
		"the submit gate is still closed during the rollback; every backup of this collection "+
			"waits out a rollback nobody is listening for")
}
