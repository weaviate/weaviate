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
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
)

func (m *StorageS3Module) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	return m.storageProvider.StoreSnapshot(ctx, snapshot)
}

func (m *StorageS3Module) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	return m.storageProvider.RestoreSnapshot(ctx, snapshotId)
}

func (m *StorageS3Module) initSnapshotStorage(ctx context.Context) error {
	endpoint := os.Getenv(s3Endpoint)
	bucketName := os.Getenv(s3Bucket)
	useSSL := strings.ToLower(os.Getenv(s3UseSSL)) == "true"
	config := s3.NewConfig(endpoint, bucketName, useSSL)
	storageProvider, err := s3.New(config, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize AWS S3 module")
	}
	m.config = config
	m.storageProvider = storageProvider
	return nil
}
