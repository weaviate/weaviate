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
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSnapshotProvider_InitSnapshot(t *testing.T) {
	className := "DemoClass"
	snapshotID := "SnapshotID"

	t.Run("fails on storage init snapshot fail", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(nil, errors.New("some kind of error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		snapshot, err := sp.initSnapshot()

		assert.Nil(t, snapshot)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "init snapshot meta")
	})

	t.Run("successfully inits", func(t *testing.T) {
		snap := snapshots.Snapshot{ClassName: className, ID: snapshotID}
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(&snap, nil)
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		snapshot, err := sp.initSnapshot()

		assert.NotNil(t, snapshot)
		assert.Equal(t, className, snapshot.ClassName)
		assert.Equal(t, snapshotID, snapshot.ID)
		assert.Nil(t, err)
	})
}

func TestSnapshotProvider_CreatesBackup(t *testing.T) {
	className := "DemoClass"
	snapshotID := "SnapshotID"
	ctx := context.Background()
	snap := snapshots.Snapshot{ClassName: className, ID: snapshotID}

	t.Run("fails and set meta on create snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(nil, errors.New("snapshotter create error"))
		storage := &fakeStorage{}
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(nil)
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "snapshotter create error")
		storage.AssertExpectations(t)
	})

	t.Run("fails and do not set meta on create snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(nil, errors.New("snapshotter create error"))
		storage := &fakeStorage{}
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(errors.New("storage failed error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "snapshotter create error")
		assert.Contains(t, err.Error(), "storage failed error")
		storage.AssertExpectations(t)
	})

	t.Run("fails to change status to transferring", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		storage := &fakeStorage{}
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(errors.New("storage transferring error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "storage transferring error")
		storage.AssertExpectations(t)
	})

	t.Run("fails and set meta on store snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(errors.New("storage store error"))
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(nil)
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "storage store error")
		storage.AssertExpectations(t)
	})

	t.Run("fails and do not set meta on store snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(errors.New("storage store error"))
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(errors.New("storage failed error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "storage store error")
		assert.Contains(t, err.Error(), "storage failed error")
		storage.AssertExpectations(t)
	})

	t.Run("fails to change status to transferred", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferred)).Return(errors.New("storage transferred error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "storage transferred error")
		storage.AssertExpectations(t)
	})

	t.Run("fails and set meta on release snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		snapshotter.On("ReleaseSnapshot", mock.Anything, snapshotID).Return(errors.New("snapshotter release error"))
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferred)).Return(nil)
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(nil)
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "snapshotter release error")
		storage.AssertExpectations(t)
	})

	t.Run("fails and do not set meta on release snapshot", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		snapshotter.On("ReleaseSnapshot", mock.Anything, snapshotID).Return(errors.New("snapshotter release error"))
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferred)).Return(nil)
		storage.On("SetMetaError", mock.Anything, className, snapshotID, mock.Anything).Return(errors.New("storage failed error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "snapshotter release error")
		assert.Contains(t, err.Error(), "storage failed error")
		storage.AssertExpectations(t)
	})

	t.Run("fails to change status to success", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		snapshotter.On("ReleaseSnapshot", mock.Anything, snapshotID).Return(nil)
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferred)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateSuccess)).Return(errors.New("storage success error"))
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "storage success error")
		storage.AssertExpectations(t)
	})

	t.Run("successfully creates backup", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, &snap).Return(&snap, nil)
		snapshotter.On("ReleaseSnapshot", mock.Anything, snapshotID).Return(nil)
		storage := &fakeStorage{}
		storage.On("StoreSnapshot", mock.Anything, &snap).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferring)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateTransferred)).Return(nil)
		storage.On("SetMetaStatus", mock.Anything, className, snapshotID, string(snapshots.CreateSuccess)).Return(nil)
		sp := newSnapshotProvider(snapshotter, storage, className, snapshotID)

		err := sp.backup(ctx, &snap)

		assert.Nil(t, err)
		snapshotter.AssertExpectations(t)
		storage.AssertExpectations(t)
	})
}
