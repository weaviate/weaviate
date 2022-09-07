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
	"github.com/semi-technologies/weaviate/modules/backup-s3/s3"
)

func (m *BackupS3Module) HomeDir(backupID string) string {
	return m.backendProvider.HomeDir(backupID)
}

func (m *BackupS3Module) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	return m.backendProvider.GetObject(ctx, backupID, key)
}

func (m *BackupS3Module) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	return m.backendProvider.PutFile(ctx, backupID, key, srcPath)
}

func (m *BackupS3Module) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	return m.backendProvider.PutObject(ctx, backupID, key, byes)
}

func (m *BackupS3Module) Initialize(ctx context.Context, backupID string) error {
	return m.backendProvider.Initialize(ctx, backupID)
}

func (m *BackupS3Module) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	return m.backendProvider.WriteToFile(ctx, backupID, key, destPath)
}

func (m *BackupS3Module) SourceDataPath() string {
	return m.backendProvider.SourceDataPath()
}

func (m *BackupS3Module) initBackupBackend(ctx context.Context) error {
	bucketName := os.Getenv(s3Bucket)
	if bucketName == "" {
		return errors.Errorf("backup init: '%s' must be set", s3Bucket)
	}

	endpoint := os.Getenv(s3Endpoint)
	pathName := os.Getenv(s3Path)
	// SSL on by default
	useSSL := true
	if strings.ToLower(os.Getenv(s3UseSSL)) == "false" {
		useSSL = false
	}
	config := s3.NewConfig(endpoint, bucketName, pathName, useSSL)
	backendProvider, err := s3.New(config, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize S3 backup module")
	}
	m.config = config
	m.backendProvider = backendProvider
	return nil
}
