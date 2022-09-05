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

type fakeBackupStorageProvider struct {
	storage modulecapabilities.SnapshotStorage
	err     error
}

func (bsp *fakeBackupStorageProvider) BackupStorage(storageName string) (modulecapabilities.SnapshotStorage, error) {
	return bsp.storage, bsp.err
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

func (s *fakeSourcer) BackupDescriptors(ctx context.Context, bakid string, classes []string,
) <-chan backup.ClassDescriptor {
	return nil
}

func (s *fakeSourcer) ClassExists(name string) bool {
	args := s.Called(name)
	return args.Bool(0)
}

type fakeStorage struct {
	mock.Mock
}

func (s *fakeStorage) DestinationPath(snapshotID string) string {
	args := s.Called(snapshotID)
	return args.String(0)
}

func (s *fakeStorage) PutFile(ctx context.Context, snapshotID, key, srcPath string) error {
	return nil
}

func (s *fakeStorage) PutObject(ctx context.Context, snapshotID, key string, _ []byte) error {
	return nil
}

func (s *fakeStorage) GetObject(ctx context.Context, snapshotID, key string) ([]byte, error) {
	args := s.Called(ctx, snapshotID, key)
	if args.Get(0) != nil {
		return args.Get(0).([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeStorage) Initialize(ctx context.Context, snapshotID string) error {
	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *fakeStorage) SourceDataPath() string {
	args := s.Called()
	return args.String(0)
}

func (s *fakeStorage) WriteToFile(ctx context.Context, snapshotID, key, destPath string) error {
	args := s.Called(ctx, snapshotID, key, destPath)
	return args.Error(0)
}
