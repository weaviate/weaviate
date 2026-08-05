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

package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// awaitBound is the deadline every handshake in this file uses. Each is reached in
// microseconds when the code is correct, so the value only decides how long a broken
// build takes to report.
const awaitBound = 10 * time.Second

// parkedProducer builds a recordingSourcer whose producer signals `started`, blocks
// on `unpark`, then emits `descs` and returns (closing its channel). It stands in for
// a producer wedged behind a per-shard backupLock that no cancellation can shorten.
func parkedProducer(started, unpark chan struct{}, descs ...backup.ClassDescriptor) *recordingSourcer {
	var s *recordingSourcer
	s = newRecordingSourcer(func(_ context.Context, emit func(backup.ClassDescriptor)) {
		close(started)
		<-unpark
		for _, d := range descs {
			s.admit(d.Name)
			emit(d)
		}
	})
	return s
}

func awaitClosed(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(awaitBound):
		t.Fatal(msg)
	}
}

// A user abort must free the participant's admission slot immediately. lastOp is
// reset only after uploader.all returns, and until then a same-ID retry is refused at
// CanCommit — so waiting for a wedged producer here blocks the retry of the very
// backup the user just cancelled.
func TestCancelledBackupReleasesWithoutWaitingForProducer(t *testing.T) {
	started, unpark := make(chan struct{}), make(chan struct{})
	s := parkedProducer(started, unpark, backup.ClassDescriptor{Name: "A"})

	u := newJoinTestUploader(t, s)
	// An hour makes budget expiry impossible, so a prompt return can only come from
	// the cancellation path.
	u.joinBudget = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The assertions live on the test goroutine: require.FailNow is only valid there.
	var allErr error
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		allErr = u.all(ctx, []string{"A"}, &backup.BackupDescriptor{}, nil, "", "")
	}, logrus.New())

	awaitClosed(t, started, "the producer must reach its park before the abort")
	cancel()

	awaitClosed(t, done, "a cancelled backup must release without waiting for its producer")
	require.ErrorIs(t, allErr, context.Canceled)
	require.False(t, s.producerClosed.Load(),
		"precondition: the producer must still be wedged when the operation returns")

	require.Eventually(t, func() bool {
		_, _, sawOpen := s.snapshot()
		return sawOpen["A"]
	}, awaitBound, 5*time.Millisecond, "the release must be issued against the still-running producer")

	// The detached join is what keeps the early release safe: the class the producer
	// admits after it still gets a releaser.
	close(unpark)
	require.Eventually(t, func() bool {
		_, sawClose, _ := s.snapshot()
		return sawClose["A"]
	}, awaitBound, 5*time.Millisecond, "the detached join must re-release once the producer finishes")
}

// Returning promptly is only half the contract: the point of the early release is
// that the participant can accept a retry of the backup the user just cancelled.
// lastOp is reset by backupper.backup's deferred reset, which runs only after
// uploader.all returns, and until then CanCommit refuses the same ID.
func TestAbortedBackupAdmitsSameIDRetry(t *testing.T) {
	var (
		backendName = "gcs"
		backupID    = "retry-me"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = Request{
			Method:   OpCreate,
			ID:       backupID,
			Classes:  []string{"Class-A"},
			Backend:  backendName,
			Duration: time.Hour,
		}
	)

	// Nothing ever closes this channel: it stands in for a producer wedged behind a
	// per-shard lock, so a join before the release would never return.
	descs := make(chan backup.ClassDescriptor)
	started := make(chan struct{})
	var releases atomic.Int64

	sourcer := &fakeSourcer{}
	sourcer.On("Backupable", ctx, req.Classes).Return(nil)
	var ch <-chan backup.ClassDescriptor = descs
	sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(ch).Run(func(mock.Arguments) { close(started) })
	sourcer.On("ReleaseBackup", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(mock.Arguments) { releases.Add(1) })

	backend := newFakeBackend()
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
	backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
	backend.On("Initialize", ctx, nodeHome).Return(nil)
	backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil)

	m := createManager(sourcer, nil, backend, nil)

	require.Empty(t, m.OnCanCommit(ctx, &req).Err)
	require.NoError(t, m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName, "", "", ""}))
	awaitClosed(t, started, "the operation must reach its producer before the abort")

	// Precondition: the gate really is held while the operation runs, so the
	// admission below can only come from the abort.
	require.Contains(t, m.OnCanCommit(ctx, &req).Err, "already in progress")

	require.NoError(t, m.OnAbort(ctx, &AbortRequest{OpCreate, req.ID, backendName, "", "", ""}))

	require.Eventually(t, func() bool {
		return m.OnCanCommit(ctx, &req).Err == ""
	}, awaitBound, 5*time.Millisecond,
		"a retry of an aborted backup must be admitted without waiting for its producer")

	// Unpark the drain so the detached goroutine and its re-release finish inside the
	// test: one release from the aborted operation, one from the detached join.
	close(descs)
	require.Eventually(t, func() bool { return releases.Load() >= 2 },
		awaitBound, 5*time.Millisecond, "the detached join must re-release once the producer finishes")
}

// Budget expiry is the non-cancelled route to the same detached join. This pins the
// whole two-phase shape: an early release, a drain that runs to the producer's close,
// a late re-release, and both releases scoped to this operation's Op.
func TestJoinBudgetExpiryDetachesAndReReleases(t *testing.T) {
	started, unpark := make(chan struct{}), make(chan struct{})
	// The producer never emits: the loop is ended by the descriptor below, not by it.
	s := parkedProducer(started, unpark)

	boom := errors.New("class A failed")
	inner := s.produce
	s.produce = func(ctx context.Context, emit func(backup.ClassDescriptor)) {
		// Fail the operation first, so the consume loop exits while the producer is
		// still parked — the state the budget exists for.
		emit(backup.ClassDescriptor{Name: "A", Error: boom})
		inner(ctx, emit)
	}

	u := newJoinTestUploader(t, s)
	u.joinBudget = 20 * time.Millisecond

	err := u.all(context.Background(), []string{"A"}, &backup.BackupDescriptor{}, nil, "", "")
	require.ErrorIs(t, err, boom)
	require.False(t, s.producerClosed.Load(),
		"precondition: the budget must expire against a producer that is still running")

	require.Eventually(t, func() bool {
		_, _, sawOpen := s.snapshot()
		return sawOpen["A"]
	}, awaitBound, 5*time.Millisecond, "budget expiry must release without the producer")

	close(unpark)
	require.Eventually(t, func() bool {
		_, sawClose, _ := s.snapshot()
		return sawClose["A"]
	}, awaitBound, 5*time.Millisecond, "the drain must run to the close and re-release")

	// Op-scoping is what makes the late re-release inert against a successor: it can
	// only ever name the operation that issued it.
	ops := s.ops()
	require.NotEmpty(t, ops)
	for _, op := range ops {
		require.Equal(t, u.op, op, "every release must carry the releasing operation's own Op")
	}
	require.NotEqual(t, u.op, backup.NewOp(u.op.ID),
		"precondition: a same-ID successor mints a distinct Op, so the fence is real")
}

// The watcher goroutines that drain coordChan both return once the operation is over,
// leaving a buffered channel nobody reads. OnAbort runs on an HTTP handler goroutine,
// so a blocking send there parks a request thread for the life of the process.
func TestOnAbortDoesNotBlockOnFullCoordChan(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	const id = "abort-me"
	c := &shardSyncChan{coordChan: make(chan interface{}, 5), log: logger}
	require.Empty(t, c.lastOp.renew(id, "", "", ""))

	req := &AbortRequest{Method: OpCreate, ID: id}
	for i := 0; i < cap(c.coordChan); i++ {
		require.NoError(t, c.OnAbort(context.Background(), req))
	}

	var abortErr error
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		abortErr = c.OnAbort(context.Background(), req)
	}, logger)
	awaitClosed(t, done, "OnAbort must not block once the unattended buffer is full")
	require.NoError(t, abortErr)

	require.Len(t, hook.AllEntries(), 1, "exactly the dropped send must be reported")
	require.Equal(t, logrus.DebugLevel, hook.LastEntry().Level)
	require.Contains(t, hook.LastEntry().Message, "dropping abort request")
}
