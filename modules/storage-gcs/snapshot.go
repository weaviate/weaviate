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
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
)

func (m *StorageGCSModule) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	return m.storageProvider.StoreSnapshot(ctx, snapshot)
}

func (m *StorageGCSModule) RestoreSnapshot(ctx context.Context, className, snapshotID string) error {
	return m.storageProvider.RestoreSnapshot(ctx, className, snapshotID)
}

func (m *StorageGCSModule) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	return m.storageProvider.SetMetaStatus(ctx, className, snapshotID, status)
}

func (m *StorageGCSModule) GetMetaStatus(ctx context.Context, className, snapshotID string) (string, error) {
	return m.storageProvider.GetMetaStatus(ctx, className, snapshotID)
}

func (m *StorageGCSModule) DestinationPath(className, snapshotID string) string {
	return m.storageProvider.DestinationPath(className, snapshotID)
}

func (m *StorageGCSModule) initSnapshotStorage(ctx context.Context) error {
	config := gcs.NewConfig(os.Getenv(gcsBucket))
	storageProvider, err := gcs.New(ctx, config, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "init gcs client")
	}
	m.storageProvider = storageProvider
	m.config = config
	return nil
}
