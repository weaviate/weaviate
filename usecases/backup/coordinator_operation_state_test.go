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
}

func newOverlappingRestores(t *testing.T, backupID, node string) *overlappingRestores {
	t.Helper()
	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil)
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
	}
}

// restore starts one restore and returns once its synchronous part is done.
func (o *overlappingRestores) restore(t *testing.T, backendName, backupID, node string) {
	t.Helper()
	req := newReq(nil, backendName, backupID)
	require.NoError(t, o.c.Restore(context.Background(), o.store, &req,
		restoreDescriptor(backupID, node), nil))
}

// finish lets every still-running restore complete and waits for the slot.
func (o *overlappingRestores) finish(t *testing.T) {
	t.Helper()
	o.finished.Store(true)
	require.Eventually(t, func() bool { return o.c.lastOp.get().ID == "" },
		20*time.Second, 10*time.Millisecond, "a restore goroutine never released its slot")
}

// Pins that a cancelled restore's goroutine and the retry that took its slot
// share no operation state (race-detected; requires `go test -race`).
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
		// Counting distinct request values seen is how the test knows both
		// goroutines polled at the same time, not one after the other.
		mu      sync.Mutex
		pollers = map[*StatusRequest]struct{}{}
	)
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(unfini)).Return(staging, nil).
		Run(func(args mock.Arguments) {
			mu.Lock()
			pollers[args.Get(2).(*StatusRequest)] = struct{}{}
			mu.Unlock()
			once.Do(func() {
				cancelAndFreeSlot(t, &o.c.lastOp, backupID)
				close(freed)
			})
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

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pollers) == 2
	}, 20*time.Second, time.Millisecond,
		"the two restores never polled at the same time, so they never overlapped")
	o.finish(t)
}

// Pins that a cancelled restore's stale write cannot overwrite the retry's
// stored metadata once the retry owns the slot.
func TestCoordinatorRestoreStaleGoroutineDoesNotOverwriteTheRetrysStoredMeta(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	o := newOverlappingRestores(t, backupID, node)

	var (
		once    sync.Once
		staged  = make(chan struct{})
		resumed = make(chan struct{})
	)
	// The first poll ends the cancelled restore's staging and holds it there,
	// which is the window the takeover and the retry land in. Every later poll
	// answers the retry, which stays in staging for the assertion.
	o.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).Once().
		Run(func(mock.Arguments) {
			once.Do(func() {
				close(staged)
				<-resumed
			})
		})
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(
		func(*StatusRequest) bool { return !o.finished.Load() })).
		Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil)
	o.client.On("Status", mock.Anything, node, mock.MatchedBy(
		func(*StatusRequest) bool { return o.finished.Load() })).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	o.restore(t, backendName, backupID, node)
	select {
	case <-staged:
	case <-time.After(20 * time.Second):
		t.Fatal("the cancelled restore never finished staging")
	}

	cancelAndFreeSlot(t, &o.c.lastOp, backupID)
	o.restore(t, backendName, backupID, node)
	require.Equal(t, backup.Transferring, o.backend.storedStatus(t))
	close(resumed)

	require.Never(t, func() bool { return o.backend.storedStatus(t) != backup.Transferring },
		200*time.Millisecond, 10*time.Millisecond,
		"the cancelled restore persisted its own outcome as the retry's")
	o.finish(t)
}
