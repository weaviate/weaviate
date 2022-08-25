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

package backups

import (
	"context"
	"time"

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/stretchr/testify/mock"
)

type fakeSnapshotterProvider struct {
	snapshotter Snapshotter
}

func (sp *fakeSnapshotterProvider) Snapshotter(className string) Snapshotter {
	return sp.snapshotter
}

type fakeBackupStorageProvider struct {
	storage modulecapabilities.SnapshotStorage
	err     error
}

func (bsp *fakeBackupStorageProvider) BackupStorage(storageName string) (modulecapabilities.SnapshotStorage, error) {
	return bsp.storage, bsp.err
}

type fakeSnapshotter struct {
	mock.Mock
}

func (s *fakeSnapshotter) CreateSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) (*snapshots.Snapshot, error) {
	args := s.Called(ctx, snapshot)
	if args.Get(0) != nil {
		return args.Get(0).(*snapshots.Snapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeSnapshotter) ReleaseSnapshot(ctx context.Context, id string) error {
	args := s.Called(ctx, id)
	return args.Error(0)
}

type fakeStorage struct {
	mock.Mock

	getMetaStatusSleep   time.Duration
	storeSnapshotSleep   time.Duration
	restoreSnapshotSleep time.Duration
}

func (s *fakeStorage) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	time.Sleep(s.storeSnapshotSleep)
	args := s.Called(ctx, snapshot)
	return args.Error(0)
}

func (s *fakeStorage) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	time.Sleep(s.restoreSnapshotSleep)
	args := s.Called(ctx, className, snapshotID)
	if args.Get(0) != nil {
		return args.Get(0).(*snapshots.Snapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeStorage) InitSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	args := s.Called(ctx, className, snapshotID)
	if args.Get(0) != nil {
		return args.Get(0).(*snapshots.Snapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeStorage) GetMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	time.Sleep(s.getMetaStatusSleep)
	args := s.Called(ctx, className, snapshotID)
	if args.Get(0) != nil {
		return args.Get(0).(*snapshots.Snapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *fakeStorage) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	args := s.Called(ctx, className, snapshotID, status)
	return args.Error(0)
}

func (s *fakeStorage) SetMetaError(ctx context.Context, className, snapshotID string, err error) error {
	args := s.Called(ctx, className, snapshotID, err)
	return args.Error(0)
}

func (s *fakeStorage) DestinationPath(className, snapshotID string) string {
	args := s.Called(className, snapshotID)
	return args.String(0)
}
