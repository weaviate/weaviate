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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

func (m *StorageFileSystemModule) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return snapshots.NewErrContextExpired(
			errors.Wrap(err, "store snapshot aborted"))
	}

	dstSnapshotPath, err := m.createSnapshotDir(snapshot)
	if err != nil {
		return err
	}

	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return snapshots.NewErrContextExpired(
				errors.Wrap(err, "store snapshot aborted"))
		}
		if err := m.copyFile(dstSnapshotPath, m.dataPath, srcRelPath); err != nil {
			return err
		}
	}

	if err := m.saveMeta(snapshot); err != nil {
		return err
	}

	return nil
}

func (m *StorageFileSystemModule) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotID)
	if err != nil {
		return nil, errors.Wrap(err, "restore snapshot")
	}

	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrap(err, "restore snapshot aborted, system might be in an invalid state")
		}
		if err := m.copyFile(m.dataPath, m.makeSnapshotDirPath(className, snapshotID), srcRelPath); err != nil {
			return nil, errors.Wrapf(err, "restore snapshot aborted, system might be in an invalid state: file %v", srcRelPath)
		}
		if err := m.copyFile(m.dataPath, m.makeSnapshotDirPath(className, snapshotID), srcRelPath); err != nil {
			return nil, errors.Wrapf(err, "restore snapshot aborted, system might be in an invalid state: file %v", srcRelPath)
		}
	}
	return snapshot, nil
}

func (m *StorageFileSystemModule) loadSnapshotMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "load snapshot meta")
	}

	metaPath := m.makeMetaFilePath(className, snapshotID)

	if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
		return nil, snapshots.NewErrNotFound(err)
	} else if err != nil {
		return nil, snapshots.NewErrInternal(err)
	}

	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, snapshots.NewErrInternal(
			errors.Wrapf(err, "read snapshot meta file '%v'", metaPath))
	}

	var snapshot snapshots.Snapshot
	if err := json.Unmarshal(metaData, &snapshot); err != nil {
		return nil, snapshots.NewErrInternal(
			errors.Wrap(err, "unmarshal snapshot meta"))
	}

	return &snapshot, nil
}

func (m *StorageFileSystemModule) GetMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	return m.loadSnapshotMeta(ctx, className, snapshotID)
}

func (m *StorageFileSystemModule) InitSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	snapshot := snapshots.New(className, snapshotID, time.Now())
	snapshot.Status = string(snapshots.CreateStarted)

	if err := m.saveMeta(snapshot); err != nil {
		return nil, snapshots.NewErrInternal(errors.Wrap(err, "init snapshot meta"))
	}

	return snapshot, nil
}

func (m *StorageFileSystemModule) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotID)
	if err != nil {
		return snapshots.NewErrInternal(errors.Wrap(err, "set meta status"))
	}

	snapshot.Status = string(status)

	if err := m.saveMeta(snapshot); err != nil {
		return snapshots.NewErrInternal(errors.Wrap(err, "set meta status"))
	}

	return nil
}

func (m *StorageFileSystemModule) SetMetaError(ctx context.Context, className, snapshotID string, snapErr error) error {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotID)
	if err != nil {
		return snapshots.NewErrInternal(errors.Wrap(err, "set meta error"))
	}

	snapshot.Status = string(snapshots.CreateFailed)
	snapshot.Error = snapErr.Error()

	if err := m.saveMeta(snapshot); err != nil {
		return snapshots.NewErrInternal(errors.Wrap(err, "set meta error"))
	}

	return nil
}

func (m *StorageFileSystemModule) initSnapshotStorage(ctx context.Context, snapshotsPath string) error {
	if snapshotsPath == "" {
		return fmt.Errorf("empty snapshots path provided")
	}
	snapshotsPath = filepath.Clean(snapshotsPath)
	if !filepath.IsAbs(snapshotsPath) {
		return fmt.Errorf("relative snapshots path provided")
	}
	if err := m.createSnapshotsDir(snapshotsPath); err != nil {
		return errors.Wrap(err, "invalid snapshots path provided")
	}
	m.snapshotsPath = snapshotsPath

	return nil
}

func (m *StorageFileSystemModule) createSnapshotsDir(snapshotsPath string) error {
	if err := os.MkdirAll(snapshotsPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_snapshots_dir").
			WithError(err).
			Errorf("failed creating snapshots directory %v", snapshotsPath)
		return snapshots.NewErrInternal(errors.Wrap(err, "make snapshot dir"))
	}
	return nil
}

func (m *StorageFileSystemModule) createSnapshotDir(snapshot *snapshots.Snapshot) (snapshotPath string, err error) {
	snapshotPath = m.makeSnapshotDirPath(snapshot.ClassName, snapshot.ID)
	return snapshotPath, m.createSnapshotsDir(snapshotPath)
}

func (m *StorageFileSystemModule) copyFile(dstSnapshotPath, srcBasePath, srcRelPath string) error {
	srcAbsPath := filepath.Join(srcBasePath, srcRelPath)
	dstAbsPath := filepath.Join(dstSnapshotPath, srcRelPath)

	src, err := os.Open(srcAbsPath)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed opening source file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "open snapshot source file '%v'", srcRelPath))
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dstAbsPath), os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed creating destication dir for file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "create snapshot destination dir for file '%v'", srcRelPath))
	}
	dst, err := os.Create(dstAbsPath)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed creating destication file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "create snapshot destination file '%v'", srcRelPath))
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed copying snapshot file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "copy snapshot file '%v'", srcRelPath))
	}

	return nil
}

func (m *StorageFileSystemModule) saveMeta(snapshot *snapshots.Snapshot) error {
	content, err := json.Marshal(snapshot)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_classname", snapshot.ClassName).
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "create meta file for snapshot '%v'", snapshot.ID))
	}

	metaFile := m.makeMetaFilePath(snapshot.ClassName, snapshot.ID)
	metaDir := path.Dir(metaFile)

	if err := os.MkdirAll(metaDir, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_classname", snapshot.ClassName).
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "create meta file for snapshot '%v'", snapshot.ID))
	}

	if err := os.WriteFile(metaFile, content, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_classname", snapshot.ClassName).
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return snapshots.NewErrInternal(
			errors.Wrapf(err, "create meta file for snapshot %v", snapshot.ID))
	}

	return nil
}
