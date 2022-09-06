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
	"github.com/semi-technologies/weaviate/modules/backup-gcs/gcs"
)

func (m *BackupGCSModule) HomeDir(snapshotID string) string {
	return m.backendProvider.HomeDir(snapshotID)
}

func (m *BackupGCSModule) GetObject(ctx context.Context, snapshotID, key string) ([]byte, error) {
	return m.backendProvider.GetObject(ctx, snapshotID, key)
}

func (m *BackupGCSModule) PutFile(ctx context.Context, snapshotID, key, srcPath string) error {
	return m.backendProvider.PutFile(ctx, snapshotID, key, srcPath)
}

func (m *BackupGCSModule) PutObject(ctx context.Context, snapshotID, key string, byes []byte) error {
	return m.backendProvider.PutObject(ctx, snapshotID, key, byes)
}

func (m *BackupGCSModule) Initialize(ctx context.Context, snapshotID string) error {
	return m.backendProvider.Initialize(ctx, snapshotID)
}

func (m *BackupGCSModule) WriteToFile(ctx context.Context, snapshotID, key, destPath string) error {
	return m.backendProvider.WriteToFile(ctx, snapshotID, key, destPath)
}

func (m *BackupGCSModule) SourceDataPath() string {
	return m.backendProvider.SourceDataPath()
}

func (m *BackupGCSModule) initBackupBackend(ctx context.Context) error {
	bucketName := os.Getenv(gcsBucket)
	if bucketName == "" {
		return errors.Errorf("backup init: '%s' must be set", gcsBucket)
	}

	config := gcs.NewConfig(bucketName, os.Getenv(gcsPath))
	backendProvider, err := gcs.New(ctx, config, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "init gcs client")
	}
	m.backendProvider = backendProvider
	m.config = config
	return nil
}
