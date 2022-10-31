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
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

func (m *Module) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	metaPath := filepath.Join(m.backupsPath, backupID, key)

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

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(m.Name(), "class")
	if err == nil {
		metric.Add(float64(len(contents)))
	}

	return contents, nil
}

func (m *Module) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	contents, err := os.ReadFile(path.Join(m.dataPath, srcPath))
	if err != nil {
		return errors.Wrapf(err, "read file '%s'", srcPath)
	}

	return m.PutObject(ctx, backupID, key, contents)
}

func (m *Module) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	backupPath := path.Join(m.makeBackupDirPath(backupID), key)

	dir := path.Dir(backupPath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir '%s'", dir)
	}

	if err := os.WriteFile(backupPath, byes, os.ModePerm); err != nil {
		return errors.Wrapf(err, "write file '%s'", backupPath)
	}

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(m.Name(), "class")
	if err == nil {
		metric.Add(float64(len(byes)))
	}

	return nil
}

func (m *Module) Initialize(ctx context.Context, backupID string) error {
	// TODO: does anything need to be done here?
	return nil
}

func (m *Module) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	obj, err := m.GetObject(ctx, backupID, key)
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

func (m *Module) SourceDataPath() string {
	return m.dataPath
}

func (m *Module) initBackupBackend(ctx context.Context, backupsPath string) error {
	if backupsPath == "" {
		return fmt.Errorf("empty backup path provided")
	}
	backupsPath = filepath.Clean(backupsPath)
	if !filepath.IsAbs(backupsPath) {
		return fmt.Errorf("relative backup path provided")
	}
	if err := m.createBackupsDir(backupsPath); err != nil {
		return errors.Wrap(err, "invalid backup path provided")
	}
	m.backupsPath = backupsPath

	return nil
}

func (m *Module) createBackupsDir(backupsPath string) error {
	if err := os.MkdirAll(backupsPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_backups_dir").
			WithError(err).
			Errorf("failed creating backups directory %v", backupsPath)
		return backup.NewErrInternal(errors.Wrap(err, "make backups dir"))
	}
	return nil
}
