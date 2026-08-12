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
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// restoreMetaBackend serves and stores the global restore descriptor. It
// stands in for fakeBackend, which closes on the first terminal descriptor,
// while these tests need a second one to land too.
type restoreMetaBackend struct {
	sync.Mutex
	stored   []byte
	statuses []backup.Status
	// onRead runs before every read answers, numbered from one. It is the seam
	// a test changes the world in between two steps of a restore goroutine.
	onRead func(n int)
	reads  int
}

func (b *restoreMetaBackend) IsExternal() bool       { return true }
func (b *restoreMetaBackend) Name() string           { return "restoreMetaBackend" }
func (b *restoreMetaBackend) SourceDataPath() string { return "" }

func (b *restoreMetaBackend) HomeDir(backupID, _, _ string) string { return "bucket/" + backupID }

func (b *restoreMetaBackend) AllBackups(context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	return nil, nil
}

func (b *restoreMetaBackend) Initialize(context.Context, string, string, string) error { return nil }

func (b *restoreMetaBackend) GetObject(_ context.Context, _, key, _, _ string) ([]byte, error) {
	if key != GlobalRestoreFile {
		return nil, backup.ErrNotFound{}
	}
	b.Lock()
	b.reads++
	n, onRead := b.reads, b.onRead
	b.Unlock()

	if onRead != nil {
		onRead(n)
	}
	// Read after the hook, so a hook standing in for another writer is what
	// this read answers with.
	b.Lock()
	stored := b.stored
	b.Unlock()
	if stored == nil {
		return nil, backup.ErrNotFound{}
	}
	return stored, nil
}

func (b *restoreMetaBackend) PutObject(_ context.Context, _, key, _, _ string, data []byte) error {
	if key != GlobalRestoreFile {
		return nil
	}
	var desc backup.DistributedBackupDescriptor
	if err := json.Unmarshal(data, &desc); err != nil {
		return err
	}
	b.Lock()
	defer b.Unlock()
	b.stored = append([]byte(nil), data...)
	b.statuses = append(b.statuses, desc.Status)
	return nil
}

// setStored replaces the stored descriptor, standing in for the write another
// coordinator makes while this one is running.
func (b *restoreMetaBackend) setStored(t *testing.T, desc backup.DistributedBackupDescriptor) {
	t.Helper()
	data, err := json.Marshal(desc)
	// assert, not require: callers run this from a mock callback on the
	// restore goroutine, where Goexit surfaces as a hang.
	if !assert.NoError(t, err) {
		return
	}
	b.Lock()
	defer b.Unlock()
	b.stored = data
}

func (b *restoreMetaBackend) readCount() int {
	b.Lock()
	defer b.Unlock()
	return b.reads
}

func (b *restoreMetaBackend) storedStatuses(t *testing.T) []backup.Status {
	t.Helper()
	b.Lock()
	defer b.Unlock()
	return append([]backup.Status(nil), b.statuses...)
}

func (b *restoreMetaBackend) Write(context.Context, string, string, string, string, backup.ReadCloserWithError) (int64, error) {
	return 0, nil
}

func (b *restoreMetaBackend) Read(context.Context, string, string, string, string, io.WriteCloser) (int64, error) {
	return 0, nil
}

func (b *restoreMetaBackend) storedStatus(t *testing.T) backup.Status {
	t.Helper()
	b.Lock()
	defer b.Unlock()
	if b.stored == nil {
		return ""
	}
	var desc backup.DistributedBackupDescriptor
	// assert, not require: this also runs inside require.Never/Eventually
	// condition closures, which testify runs off the test goroutine.
	assert.NoError(t, json.Unmarshal(b.stored, &desc))
	return desc.Status
}

func restoreDescriptor(id, node string) *backup.DistributedBackupDescriptor {
	return &backup.DistributedBackupDescriptor{
		ID:            id,
		Version:       Version,
		ServerVersion: config.ServerVersion,
		NodeMapping:   map[string]string{},
		Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: []string{"C1"}}},
	}
}

// overlappingRestores wires a coordinator whose participant never fails, so a
// restore keeps polling until the test says otherwise.
type overlappingRestores struct {
	c        *coordinator
	backend  *restoreMetaBackend
	store    coordStore
	client   *fakeClient
	finished *atomic.Bool
	// logs is how a test waits for a restore goroutine that stops without
	// touching storage: the decision to stop is only observable as a log line.
	logs *test.Hook
}

func newOverlappingRestores(t *testing.T, backupID, node string) *overlappingRestores {
	t.Helper()
	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	logger, hook := test.NewNullLogger()
	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, logger, fc.nodeResolver, nil)
	c.timeoutNextRound = time.Millisecond

	backend := &restoreMetaBackend{}
	fc.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)

	return &overlappingRestores{
		c:        c,
		backend:  backend,
		store:    coordStore{objectStore{backend, backupID, "", "", ""}},
		client:   &fc.client,
		finished: &atomic.Bool{},
		logs:     hook,
	}
}

// awaitLog waits for a restore goroutine to log msg.
func (o *overlappingRestores) awaitLog(t *testing.T, msg string) {
	t.Helper()
	awaitLog(t, o.logs, msg)
}

// awaitLog is how a test waits for a goroutine whose decision leaves no
// other trace.
func awaitLog(t *testing.T, hook *test.Hook, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, e := range hook.AllEntries() {
			if e.Message == msg {
				return true
			}
		}
		return false
	}, 20*time.Second, 10*time.Millisecond, "nothing logged %q", msg)
}

// staleRestoreStopped is logged when a restore goroutine finds the slot
// belongs to somebody else.
const staleRestoreStopped = "restore no longer holds the slot, stopping without publishing"

// restoreCancelledInStorage is logged when a restore reads its own
// cancellation back from the descriptor and stops.
const restoreCancelledInStorage = "restore cancelled (detected from storage after commit)"

// staleOutcomeRefused is logged when the slot refuses a restore's own outcome,
// the last step before that outcome would be written to storage.
const staleOutcomeRefused = "restore outcome refused by the slot, stopping without publishing"

// slotAtLog captures what the slot held the moment a message was logged. It is
// how a test reaches inside the window between a goroutine's decision and the
// release that follows it, which is over before the test could poll for it.
type slotAtLog struct {
	msg  string
	stat *backupStat
	once sync.Once
	seen chan reqState
}

// watchSlotAt arms the capture. Safe to read the slot from a log hook: nothing
// in this package logs while holding the slot's lock.
func watchSlotAt(logger *logrus.Logger, stat *backupStat, msg string) *slotAtLog {
	w := &slotAtLog{msg: msg, stat: stat, seen: make(chan reqState, 1)}
	logger.AddHook(w)
	return w
}

func (w *slotAtLog) Levels() []logrus.Level { return logrus.AllLevels }

func (w *slotAtLog) Fire(e *logrus.Entry) error {
	if e.Message == w.msg {
		w.once.Do(func() { w.seen <- w.stat.get() })
	}
	return nil
}

func (w *slotAtLog) await(t *testing.T) reqState {
	t.Helper()
	select {
	case st := <-w.seen:
		return st
	case <-time.After(20 * time.Second):
		t.Fatalf("nothing logged %q", w.msg)
		return reqState{}
	}
}

// restore starts one restore and returns once its synchronous part is done.
func (o *overlappingRestores) restore(t *testing.T, backendName, backupID, node string) {
	t.Helper()
	req := newReq(nil, backendName, backupID)
	require.NoError(t, o.c.Restore(context.Background(), o.store, &req,
		restoreDescriptor(backupID, node), nil))
}

// finish lets every still-running restore complete and joins both goroutines,
// so the cancelled one can't outlive the test and touch t after it returns.
func (o *overlappingRestores) finish(t *testing.T) {
	t.Helper()
	o.finished.Store(true)
	o.awaitLog(t, staleRestoreStopped)
	require.Eventually(t, func() bool { return o.c.lastOp.get().ID == "" },
		20*time.Second, 10*time.Millisecond, "a restore goroutine never released its slot")
}

// Runs a cancelled restore's goroutine and the retry that took its slot at the
// same time, so the detectors can prove they share no operation state. There is
// no assertion for that by design: a violation surfaces as a data race under
// `go test -race` (CI runs with it), or as the runtime's "concurrent map
// writes" fatal without it. What the assertions below pin is that the two
// really did overlap, without which the run proves nothing.
func TestCoordinatorRestoreStaleGoroutineSharesNoStateWithTheRetry(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	o := newOverlappingRestores(t, backupID, node)
	staging := &StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}
	staged := &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}

	var (
		once   sync.Once
		freed  = make(chan struct{})
		unfini = func(*StatusRequest) bool { return !o.finished.Load() }
		fini   = func(*StatusRequest) bool { return o.finished.Load() }
		// inFlight counts concurrent pollers; the second to arrive closes
		// overlapped, so both proceed together instead of serially.
		inFlight    atomic.Int32
		overlapOnce sync.Once
		overlapped  = make(chan struct{})
	)
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(unfini)).Return(staging, nil).
		Run(func(mock.Arguments) {
			if inFlight.Add(1) == 2 {
				overlapOnce.Do(func() { close(overlapped) })
			}
			defer inFlight.Add(-1)
			once.Do(func() {
				cancelAndFreeSlot(t, &o.c.lastOp, backupID)
				close(freed)
			})
			// Bounded: the first poll runs before the retry exists, so waiting
			// forever here would deadlock the overlap this is trying to produce.
			select {
			case <-overlapped:
			case <-time.After(100 * time.Millisecond):
			}
		})
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(fini)).Return(staged, nil)

	o.restore(t, backendName, backupID, node)
	select {
	case <-freed:
	case <-time.After(20 * time.Second):
		t.Fatal("the cancelled restore never gave the slot back")
	}

	// The retry claims the freed slot and starts while the cancelled restore's
	// goroutine is still polling participants.
	o.restore(t, backendName, backupID, node)

	awaitInterference(t, overlapped,
		"the two restores never polled at the same time, so they never overlapped")
	o.finish(t)
}

// Pins that a cancelled restore's stale write cannot overwrite the retry's
// stored metadata. The takeover lands after the slot check the goroutine makes
// when staging ends, so what has to hold here is the refusal of the outcome
// itself, the last step before it would be written.
func TestCoordinatorRestoreStaleGoroutineDoesNotOverwriteTheRetrysStoredMeta(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	o := newOverlappingRestores(t, backupID, node)

	var (
		staged  = make(chan struct{})
		resumed = make(chan struct{})
	)
	// Read one is the check the first restore opens with; read two is the one
	// its goroutine makes once staging is over, past the slot check. Holding it
	// there is the window the cancel and the retry land in.
	o.backend.onRead = func(n int) {
		if n != 2 {
			return
		}
		close(staged)
		<-resumed
	}
	// Staging ends for the first restore; the retry stays in it until the
	// assertion below has run.
	o.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).Once()
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(
		func(*StatusRequest) bool { return !o.finished.Load() })).
		Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil)
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(
		func(*StatusRequest) bool { return o.finished.Load() })).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	o.restore(t, backendName, backupID, node)
	awaitInterference(t, staged, "the cancelled restore never reached the read after its staging")

	cancelAndFreeSlot(t, &o.c.lastOp, backupID)
	o.restore(t, backendName, backupID, node)
	require.Equal(t, backup.Transferring, o.backend.storedStatus(t))
	close(resumed)

	// Wait for the stale goroutine to actually reach its publish decision,
	// otherwise "no write observed" would only mean "not yet".
	o.awaitLog(t, staleOutcomeRefused)
	require.Never(t, func() bool { return o.backend.storedStatus(t) != backup.Transferring },
		200*time.Millisecond, 10*time.Millisecond,
		"the cancelled restore persisted its own outcome as the retry's")

	// Let the retry finish, so it cannot outlive the test and touch t after it
	// has returned.
	o.finished.Store(true)
	require.Eventually(t, func() bool { return o.c.lastOp.get().ID == "" },
		20*time.Second, 10*time.Millisecond, "the retry never released its slot")
}
