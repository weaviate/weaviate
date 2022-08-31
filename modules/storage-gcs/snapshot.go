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

package modstggcs

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
)

func (m *StorageGCSModule) StoreSnapshot(ctx context.Context, snapshot *backup.Snapshot) error {
	return m.storageProvider.StoreSnapshot(ctx, snapshot)
}

func (m *StorageGCSModule) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.RestoreSnapshot(ctx, className, snapshotID)
}

func (m *StorageGCSModule) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	return m.storageProvider.SetMetaStatus(ctx, className, snapshotID, status)
}

func (m *StorageGCSModule) SetMetaError(ctx context.Context, className, snapshotID string, err error) error {
	return m.storageProvider.SetMetaError(ctx, className, snapshotID, err)
}

func (m *StorageGCSModule) GetMeta(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.GetMeta(ctx, className, snapshotID)
}

func (m *StorageGCSModule) DestinationPath(className, snapshotID string) string {
	return m.storageProvider.DestinationPath(className, snapshotID)
}

func (m *StorageGCSModule) InitSnapshot(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.InitSnapshot(ctx, className, snapshotID)
}

func (m *StorageGCSModule) initSnapshotStorage(ctx context.Context) error {
	bucketName := os.Getenv(gcsBucket)
	if bucketName == "" {
		return errors.Errorf("snapshot init: '%s' must be set", gcsBucket)
	}

	config := gcs.NewConfig(bucketName, os.Getenv(gcsSnapshotRoot))
	storageProvider, err := gcs.New(ctx, config, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "init gcs client")
	}
	m.storageProvider = storageProvider
	m.config = config
	return nil
}
