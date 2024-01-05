//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modstgfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (m *Module) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	metaPath, err := m.getObjectPath(ctx, backupID, key)
	if err != nil {
		return nil, err
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

func (m *Module) getObjectPath(ctx context.Context, backupID, key string) (string, error) {
	metaPath := filepath.Join(m.backupsPath, backupID, key)

	if err := ctx.Err(); err != nil {
		return "", backup.NewErrContextExpired(errors.Wrapf(err, "get object '%s'", metaPath))
	}

	if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
		return "", backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", metaPath))
	} else if err != nil {
		return "", backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", metaPath))
	}

	return metaPath, nil
}

func (m *Module) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	sourcePath := path.Join(m.dataPath, srcPath)
	backupPath := path.Join(m.makeBackupDirPath(backupID), key)

	bytesWritten, err := m.copyFile(sourcePath, backupPath)
	if err != nil {
		return err
	}

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(m.Name(), "class")
	if err == nil {
		metric.Add(float64(bytesWritten))
	}

	return nil
}

func (m *Module) copyFile(sourcePath, destinationPath string) (int64, error) {
	source, err := os.Open(sourcePath)
	defer func() error {
		return source.Close()
	}()
	if err != nil {
		return 0, errors.Wrapf(err, "open file '%s'", sourcePath)
	}

	if _, err := os.Stat(destinationPath); err != nil {
		if err := os.MkdirAll(path.Dir(destinationPath), os.ModePerm); err != nil {
			return 0, errors.Wrapf(err, "make dir '%s'", destinationPath)
		}
	}

	destination, err := os.Create(destinationPath)
	defer func() error {
		return destination.Close()
	}()
	if err != nil {
		return 0, errors.Wrapf(err, "create destination file '%s'", destinationPath)
	}

	written, err := io.Copy(destination, source)
	if err != nil {
		return 0, errors.Wrapf(err, "copy file from '%s' to '%s'", sourcePath, destinationPath)
	}

	return written, nil
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
	sourcePath, err := m.getObjectPath(ctx, backupID, key)
	if err != nil {
		return err
	}

	bytesWritten, err := m.copyFile(sourcePath, destPath)
	if err != nil {
		return err
	}

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(m.Name(), "class")
	if err == nil {
		metric.Add(float64(bytesWritten))
	}

	return nil
}

func (m *Module) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error) {
	defer r.Close()
	backupPath := path.Join(m.makeBackupDirPath(backupID), key)
	dir := path.Dir(backupPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return 0, fmt.Errorf("make dir %q: %w", dir, err)
	}
	f, err := os.OpenFile(backupPath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return 0, fmt.Errorf("open file %q: %w", backupPath, err)
	}
	defer f.Close()

	written, err := io.Copy(f, r)
	if err != nil {
		return 0, fmt.Errorf("write file %q: %w", backupPath, err)
	}
	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(m.Name(), "class"); err == nil {
		metric.Add(float64(written))
	}

	return written, err
}

func (m *Module) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
	defer w.Close()
	sourcePath, err := m.getObjectPath(ctx, backupID, key)
	if err != nil {
		return 0, fmt.Errorf("source path %s/%s: %w", backupID, key, err)
	}

	// open file
	f, err := os.Open(sourcePath)
	if err != nil {
		return 0, fmt.Errorf("open file %q: %w", sourcePath, err)
	}
	defer f.Close()

	// copy file
	read, err := io.Copy(w, f)
	if err != nil {
		return 0, fmt.Errorf("write : %w", err)
	}

	if metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.
		GetMetricWithLabelValues(m.Name(), "class"); err == nil {
		metric.Add(float64(read))
	}
	return read, err
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
