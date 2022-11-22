//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/stretchr/testify/mock"
)

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
	doneChan chan bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{doneChan: make(chan bool)}
}

func (s *fakeBackend) HomeDir(backupID string) string {
	s.RLock()
	defer s.RUnlock()
	args := s.Called(backupID)
	return args.String(0)
}

func (s *fakeBackend) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called(ctx, backupID, key, srcPath)
	return args.Error(0)
}

func (s *fakeBackend) PutObject(ctx context.Context, backupID, key string, bytes []byte) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called(ctx, backupID, key, bytes)
	if key == BackupFile {
		json.Unmarshal(bytes, &s.meta)
	} else if key == GlobalBackupFile || key == GlobalRestoreFile {
		json.Unmarshal(bytes, &s.glMeta)
		if s.glMeta.Status == backup.Success || s.glMeta.Status == backup.Failed {
			close(s.doneChan)
		}
	}
	return args.Error(0)
}

func (s *fakeBackend) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	args := s.Called(ctx, backupID, key)
	if args.Get(0) != nil {
		return args.Get(0).([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeBackend) Initialize(ctx context.Context, backupID string) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called(ctx, backupID)
	return args.Error(0)
}

func (s *fakeBackend) SourceDataPath() string {
	s.RLock()
	defer s.RUnlock()
	args := s.Called()
	return args.String(0)
}

func (s *fakeBackend) IsExternal() bool {
	return true
}

func (f *fakeBackend) Name() string {
	return "fakeBackend"
}

func (s *fakeBackend) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called(ctx, backupID, key, destPath)
	return args.Error(0)
}
