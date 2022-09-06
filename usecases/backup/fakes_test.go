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
	return nil
}

func (s *fakeSourcer) ClassExists(name string) bool {
	args := s.Called(name)
	return args.Bool(0)
}

type fakeBackend struct {
	mock.Mock
}

func (s *fakeBackend) HomeDir(snapshotID string) string {
	args := s.Called(snapshotID)
	return args.String(0)
}

func (s *fakeBackend) PutFile(ctx context.Context, snapshotID, key, srcPath string) error {
	return nil
}

func (s *fakeBackend) PutObject(ctx context.Context, snapshotID, key string, _ []byte) error {
	return nil
}

func (s *fakeBackend) GetObject(ctx context.Context, snapshotID, key string) ([]byte, error) {
	args := s.Called(ctx, snapshotID, key)
	if args.Get(0) != nil {
		return args.Get(0).([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeBackend) Initialize(ctx context.Context, snapshotID string) error {
	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *fakeBackend) SourceDataPath() string {
	args := s.Called()
	return args.String(0)
}

func (s *fakeBackend) WriteToFile(ctx context.Context, snapshotID, key, destPath string) error {
	args := s.Called(ctx, snapshotID, key, destPath)
	return args.Error(0)
}
