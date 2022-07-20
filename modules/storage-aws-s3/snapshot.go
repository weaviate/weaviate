package modstgs3

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/snapshots"
)

func (m *StorageAWSS3Module) StoreSnapshot(ctx context.Context, snapshot snapshots.Snapshot) error {
	// TODO implement
	m.logger.Errorf("StoreSnapshot of StorageAWSS3Module not yet implemented")
	return nil
}

func (m *StorageAWSS3Module) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	// TODO implement
	m.logger.Errorf("RestoreSnapshot of StorageAWSS3Module not yet implemented")
	return nil
}

func (m *StorageAWSS3Module) initSnapshotStorage(ctx context.Context) error {
	// TODO implement
	m.logger.Errorf("initSnapshotStorage of StorageAWSS3Module not yet implemented")
	return nil
}
