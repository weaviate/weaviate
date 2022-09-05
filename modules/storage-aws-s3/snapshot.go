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
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
)

func (m *StorageS3Module) DestinationPath(snapshotID string) string {
	return m.storageProvider.DestinationPath(snapshotID)
}

func (m *StorageS3Module) GetObject(ctx context.Context, snapshotID, key string) ([]byte, error) {
	return m.storageProvider.GetObject(ctx, snapshotID, key)
}

func (m *StorageS3Module) PutFile(ctx context.Context, snapshotID, key, srcPath string) error {
	return m.storageProvider.PutFile(ctx, snapshotID, key, srcPath)
}

func (m *StorageS3Module) PutObject(ctx context.Context, snapshotID, key string, byes []byte) error {
	return m.storageProvider.PutObject(ctx, snapshotID, key, byes)
}

func (m *StorageS3Module) Initialize(ctx context.Context, snapshotID string) error {
	return m.storageProvider.Initialize(ctx, snapshotID)
}

func (m *StorageS3Module) WriteToFile(ctx context.Context, snapshotID, key, destPath string) error {
	return m.storageProvider.WriteToFile(ctx, snapshotID, key, destPath)
}

func (m *StorageS3Module) SourceDataPath() string {
	return m.storageProvider.SourceDataPath()
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
