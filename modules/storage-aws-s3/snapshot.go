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

package modstgs3

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
)

func (m *StorageS3Module) StoreSnapshot(ctx context.Context, snapshot *backup.Snapshot) error {
	return m.storageProvider.StoreSnapshot(ctx, snapshot)
}

func (m *StorageS3Module) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.RestoreSnapshot(ctx, className, snapshotID)
}

func (m *StorageS3Module) SetMetaError(ctx context.Context, className, snapshotID string, err error) error {
	return m.storageProvider.SetMetaError(ctx, className, snapshotID, err)
}

func (m *StorageS3Module) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	return m.storageProvider.SetMetaStatus(ctx, className, snapshotID, status)
}

func (m *StorageS3Module) GetMeta(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.GetMeta(ctx, className, snapshotID)
}

func (m *StorageS3Module) DestinationPath(className, snapshotID string) string {
	return m.storageProvider.DestinationPath(className, snapshotID)
}

func (m *StorageS3Module) InitSnapshot(ctx context.Context, className, snapshotID string) (*backup.Snapshot, error) {
	return m.storageProvider.InitSnapshot(ctx, className, snapshotID)
}

func (m *StorageS3Module) initSnapshotStorage(ctx context.Context) error {
	bucketName := os.Getenv(s3Bucket)
	if bucketName == "" {
		return errors.Errorf("snapshot init: '%s' must be set", s3Bucket)
	}

	endpoint := os.Getenv(s3Endpoint)
	rootName := os.Getenv(s3SnapshotRoot)
	useSSL := strings.ToLower(os.Getenv(s3UseSSL)) == "true"
	config := s3.NewConfig(endpoint, bucketName, rootName, useSSL)
	storageProvider, err := s3.New(config, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize AWS S3 module")
	}
	m.config = config
	m.storageProvider = storageProvider
	return nil
}
