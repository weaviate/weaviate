//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
		chunkKey("DemoClass", 1): data,
	}
}

type fakeBackupBackendProvider struct {
	backend modulecapabilities.BackupBackend
	err     error
}

func (bsp *fakeBackupBackendProvider) BackupBackend(backend string) (modulecapabilities.BackupBackend, error) {
	return bsp.backend, bsp.err
}

type fakeSourcer struct {
	mock.Mock
}

func (s *fakeSourcer) ReleaseBackup(ctx context.Context, id, class string) error {
	args := s.Called(ctx, id, class)
	return args.Error(0)
}

func (s *fakeSourcer) Backupable(ctx context.Context, classes []string) error {
	args := s.Called(ctx, classes)
	return args.Error(0)
}

func (s *fakeSourcer) ListBackupable() []string {
	args := s.Called()
	return args.Get(0).([]string)
}

func (s *fakeSourcer) BackupDescriptors(ctx context.Context, bakid string, classes []string,
) <-chan backup.ClassDescriptor {
	args := s.Called(ctx, bakid, classes)
	return args.Get(0).(<-chan backup.ClassDescriptor)
}

func (s *fakeSourcer) ClassExists(name string) bool {
	args := s.Called(name)
	return args.Bool(0)
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

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		doneChan: make(chan bool),
		files:    map[string][]byte{},
		chunks:   chunks,
	}
}

func (fb *fakeBackend) HomeDir(backupID string) string {
	fb.RLock()
	defer fb.RUnlock()
	args := fb.Called(backupID)
	return args.String(0)
}

func (fb *fakeBackend) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID, key, srcPath)
	return args.Error(0)
}

func (fb *fakeBackend) PutObject(ctx context.Context, backupID, key string, bytes []byte) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID, key, bytes)
	if key == BackupFile {
		json.Unmarshal(bytes, &fb.meta)
	} else if key == GlobalBackupFile || key == GlobalRestoreFile {
		json.Unmarshal(bytes, &fb.glMeta)
		if fb.glMeta.Status == backup.Success || fb.glMeta.Status == backup.Failed {
			close(fb.doneChan)
		}
	}
	return args.Error(0)
}

func (fb *fakeBackend) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	fb.RLock()
	defer fb.RUnlock()
	args := fb.Called(ctx, backupID, key)
	if args.Get(0) != nil {
		return args.Get(0).([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (fb *fakeBackend) Initialize(ctx context.Context, backupID string) error {
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

func (fb *fakeBackend) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	fb.Lock()
	defer fb.Unlock()
	args := fb.Called(ctx, backupID, key, destPath)
	return args.Error(0)
}

func (fb *fakeBackend) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
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

func (fb *fakeBackend) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error) {
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
