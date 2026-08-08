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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

var chunks map[string][]byte

func init() {
	path := "test_data/chunk-1.tar.gz"
	data, err := os.ReadFile(path)
	if err != nil {
		panic("missing test file: " + path)
	}

	chunks = map[string][]byte{
		chunkKey("Article", 1): data,
	}
}

type fakeBackupBackendProvider struct {
	backend modulecapabilities.BackupBackend
	err     error
}

func (bsp *fakeBackupBackendProvider) BackupBackend(backend string, _ modulecapabilities.BackendUseCase) (modulecapabilities.BackupBackend, error) {
	return bsp.backend, bsp.err
}

func (bsp *fakeBackupBackendProvider) EnabledBackupBackends() []modulecapabilities.BackupBackend {
	return []modulecapabilities.BackupBackend{bsp.backend}
}

type fakeSourcer struct {
	mock.Mock

	// Plain field so pre-existing restore tests pass without a mock.On call.
	reindexInFlightErr error
	reindexOverlapErr  error
	// reindexOverlapFn lets a test act on the context the lookup is handed, so
	// an abort landing while the lookup runs can be reproduced.
	reindexOverlapFn func(ctx context.Context) error

	// overlapMu guards the recorded arguments below: the participant backup
	// runs the commit-time check on its own goroutine.
	overlapMu          sync.Mutex
	overlapClasses     []string
	overlapSince       time.Time
	overlapCalls       int
	inFlightCollection [][]string
}

func (s *fakeSourcer) ReleaseBackup(ctx context.Context, id, class string) error {
	args := s.Called(ctx, id, class)
	return args.Error(0)
}

// The collections are recorded for the same reason the overlap check's are
// (below): the answer alone says nothing about which collections the gate was
// asked about, and asking about a wildcard pattern rather than a resolved class
// name is a silent way for this half of the gate to stop gating.
func (s *fakeSourcer) RefuseIfAnyReindexInFlight(_ context.Context, collections []string) error {
	s.overlapMu.Lock()
	s.inFlightCollection = append(s.inFlightCollection, collections)
	s.overlapMu.Unlock()
	return s.reindexInFlightErr
}

// reindexInFlightCollections returns what each participant-side gate call was
// scoped to.
func (s *fakeSourcer) reindexInFlightCollections() [][]string {
	s.overlapMu.Lock()
	defer s.overlapMu.Unlock()
	return append([][]string(nil), s.inFlightCollection...)
}

// reindexOverlapErr backs RefuseIfReindexOverlapped as a plain field so a test
// can distinguish "live at commit" from "overlapped and already finished".
//
// The arguments are recorded as well: the answer alone says nothing about which
// window the check was asked about, and asking about the wrong one is a silent
// way for this backstop to stop backstopping.
func (s *fakeSourcer) RefuseIfReindexOverlapped(ctx context.Context, classes []string, since time.Time) error {
	s.overlapMu.Lock()
	s.overlapCalls++
	s.overlapClasses = classes
	s.overlapSince = since
	s.overlapMu.Unlock()

	if s.reindexOverlapFn != nil {
		return s.reindexOverlapFn(ctx)
	}
	return s.reindexOverlapErr
}

// lastOverlapQuery returns the arguments of the most recent
// RefuseIfReindexOverlapped call, plus how many calls have been made.
func (s *fakeSourcer) lastOverlapQuery() (classes []string, since time.Time, calls int) {
	s.overlapMu.Lock()
	defer s.overlapMu.Unlock()
	return s.overlapClasses, s.overlapSince, s.overlapCalls
}

func (s *fakeSourcer) Backupable(ctx context.Context, classes []string) error {
	args := s.Called(ctx, classes)
	return args.Error(0)
}

func (s *fakeSourcer) BackupDescriptors(ctx context.Context, bakid string, classes []string, baseDescr []*backup.BackupDescriptor,
) <-chan backup.ClassDescriptor {
	args := s.Called(ctx, bakid, classes, baseDescr)
	return args.Get(0).(<-chan backup.ClassDescriptor)
}

type fakeBackend struct {
	mock.Mock
	sync.RWMutex
	meta     backup.BackupDescriptor
	glMeta   backup.DistributedBackupDescriptor
	files    map[string][]byte
	chunks   map[string][]byte
	doneChan chan bool
}

func (fb *fakeBackend) getMetaStatus() (backup.Status, string) {
	fb.RLock()
	defer fb.RUnlock()
	return fb.meta.Status, fb.meta.Error
}

func (fb *fakeBackend) getMetaBaseBackupID() string {
	fb.RLock()
	defer fb.RUnlock()
	return fb.meta.BaseBackupID
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		doneChan: make(chan bool),
		files:    map[string][]byte{},
		chunks:   chunks,
	}
}

func (fb *fakeBackend) HomeDir(backupID, overrideBucket, overridePath string) string {
	fb.RLock()
	defer fb.RUnlock()
	args := fb.Called(overrideBucket, overridePath, backupID)
	return args.String(0)
}

func (fb *fakeBackend) AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	fb.RLock()
	defer fb.RUnlock()
	args := fb.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).([]*backup.DistributedBackupDescriptor), args.Error(1)
	}
	return nil, args.Error(1)
}

func (fb *fakeBackend) PutFile(ctx context.Context, backupID, key, srcPath, overrideBucket, overridePath string) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID, key, srcPath)
	return args.Error(0)
}

func (fb *fakeBackend) PutObject(ctx context.Context, backupID, key, overrideBucket, overridePath string, bytes []byte) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID, key, bytes)
	switch key {
	case BackupFile:
		json.Unmarshal(bytes, &fb.meta)
	case GlobalBackupFile, GlobalRestoreFile:
		json.Unmarshal(bytes, &fb.glMeta)
		if fb.glMeta.Status == backup.Success || fb.glMeta.Status == backup.Failed {
			close(fb.doneChan)
		}
	default:
		// do nothing
	}
	return args.Error(0)
}

func (fb *fakeBackend) GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error) {
	fb.RLock()
	defer fb.RUnlock()

	// For GlobalRestoreFile, dynamically return current glMeta state if it has been set
	// by PutObject during an active restore. This allows coordinator code to read the
	// current status (e.g., to check for cancellation) without requiring explicit mock
	// expectations for each read. Falls back to mock expectations for tests that
	// explicitly set them (like status check tests).
	if key == GlobalRestoreFile && fb.glMeta.ID != "" {
		bytes, err := json.Marshal(fb.glMeta)
		if err != nil {
			return nil, err
		}
		return bytes, nil
	}

	args := fb.Called(ctx, backupID, key)
	if args.Get(0) != nil {
		return args.Get(0).([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (fb *fakeBackend) Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID)
	return args.Error(0)
}

func (fb *fakeBackend) SourceDataPath() string {
	fb.RLock()
	defer fb.RUnlock()
	args := fb.Called()
	return args.String(0)
}

func (fb *fakeBackend) IsExternal() bool {
	return true
}

func (fb *fakeBackend) Name() string {
	return "fakeBackend"
}

func (fb *fakeBackend) Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	fb.Lock()
	defer fb.Unlock()
	defer w.Close()

	args := fb.Called(ctx, backupID, key, w)
	if err := args.Error(1); err != nil {
		return 0, err
	}

	if data := fb.chunks[key]; data != nil {
		io.Copy(w, bytes.NewReader(data))
	}
	return 0, args.Error(1)
}

func (fb *fakeBackend) Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r backup.ReadCloserWithError) (int64, error) {
	fb.Lock()
	defer fb.Unlock()
	defer r.Close()

	args := fb.Called(ctx, backupID, key, r)
	if err := args.Error(1); err != nil {
		return 0, err
	}
	buf := bytes.Buffer{}
	n, err := io.Copy(&buf, r)
	fb.files[backupID+"/"+key] = buf.Bytes()

	return n, err
}

// fakeStatusSlot stands in for the node's operation slot, recording what a
// status poll would read at each change.
type fakeStatusSlot struct {
	statuses []backup.Status
	reason   string
}

func (f *fakeStatusSlot) set(st backup.Status) {
	f.statuses = append(f.statuses, st)
}

func (f *fakeStatusSlot) setFailed(reason string) {
	f.reason = reason
	f.set(backup.Failed)
}

func (f *fakeStatusSlot) last() backup.Status {
	if len(f.statuses) == 0 {
		return ""
	}
	return f.statuses[len(f.statuses)-1]
}
