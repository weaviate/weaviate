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

package modstgfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
)

func (m *StorageFileSystemModule) GetObject(ctx context.Context, snapshotID, key string) ([]byte, error) {
	metaPath := filepath.Join(m.snapshotsPath, snapshotID, key)

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "get object '%s'", metaPath))
	}

	if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
		return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", metaPath))
	} else if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", metaPath))
	}

	contents, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", metaPath))
	}

	return contents, nil
}

func (m *StorageFileSystemModule) PutFile(ctx context.Context, snapshotID, key, srcPath string) error {
	contents, err := os.ReadFile(path.Join(m.dataPath, srcPath))
	if err != nil {
		return errors.Wrapf(err, "read file '%s'", srcPath)
	}

	return m.PutObject(ctx, snapshotID, key, contents)
}

func (m *StorageFileSystemModule) PutObject(ctx context.Context, snapshotID, key string, byes []byte) error {
	snapshotPath := path.Join(m.makeSnapshotDirPath(snapshotID), key)

	dir := path.Dir(snapshotPath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir '%s'", dir)
	}

	if err := os.WriteFile(snapshotPath, byes, os.ModePerm); err != nil {
		return errors.Wrapf(err, "write file '%s'", snapshotPath)
	}

	return nil
}

func (m *StorageFileSystemModule) Initialize(ctx context.Context, snapshotID string) error {
	// TODO: does anything need to be done here?
	return nil
}

func (m *StorageFileSystemModule) WriteToFile(ctx context.Context, snapshotID, key, destPath string) error {
	obj, err := m.GetObject(ctx, snapshotID, key)
	if err != nil {
		return errors.Wrapf(err, "get object '%s'", key)
	}

	dir := path.Dir(destPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir '%s'", dir)
	}

	if err := os.WriteFile(destPath, obj, os.ModePerm); err != nil {
		return errors.Wrapf(err, "write file '%s'", destPath)
	}

	return nil
}

func (m *StorageFileSystemModule) SourceDataPath() string {
	return m.dataPath
}

func (m *StorageFileSystemModule) initSnapshotStorage(ctx context.Context, snapshotsPath string) error {
	if snapshotsPath == "" {
		return fmt.Errorf("empty snapshots path provided")
	}
	snapshotsPath = filepath.Clean(snapshotsPath)
	if !filepath.IsAbs(snapshotsPath) {
		return fmt.Errorf("relative snapshots path provided")
	}
	if err := m.createBackupsDir(snapshotsPath); err != nil {
		return errors.Wrap(err, "invalid snapshots path provided")
	}
	m.snapshotsPath = snapshotsPath

	return nil
}

func (m *StorageFileSystemModule) createBackupsDir(snapshotsPath string) error {
	if err := os.MkdirAll(snapshotsPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_snapshots_dir").
			WithError(err).
			Errorf("failed creating snapshots directory %v", snapshotsPath)
		return backup.NewErrInternal(errors.Wrap(err, "make snapshot dir"))
	}
	return nil
}
