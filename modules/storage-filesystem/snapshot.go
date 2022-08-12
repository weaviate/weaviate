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
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

func (m *StorageFileSystemModule) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrapf(err, "store snapshot aborted")
	}

	dstSnapshotPath, err := m.createSnapshotDir(snapshot)
	if err != nil {
		return errors.Wrapf(err, "could not create snapshot dir")
	}

	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "store snapshot aborted")
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

func (m *StorageFileSystemModule) RestoreSnapshot(ctx context.Context, className, snapshotId string) error {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotId)
	if err != nil {
		return errors.Wrap(err, "load snapshot meta")
	}

	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "restore snapshot aborted, system might be in an invalid state")
		}
		if err := m.copyFile(m.dataPath, m.makeSnapshotDirPath(className, snapshotId), srcRelPath); err != nil {
			return errors.Wrapf(err, "restore snapshot aborted, system might be in an invalid state: file %v", srcRelPath)
		}
	}
	return nil
}

func (m *StorageFileSystemModule) loadSnapshotMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "restore snapshot aborted, invalid context")
	}

	metaPath := m.makeMetaFilePath(className, snapshotID)

	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not read snapshot meta file %v", metaPath)
	}
	var snapshot snapshots.Snapshot
	if err := json.Unmarshal(metaData, &snapshot); err != nil {
		return nil, errors.Wrapf(err, "Could not unmarshal snapshot meta file %v", metaPath)
	}

	return &snapshot, nil
}

func (m *StorageFileSystemModule) GetMetaStatus(ctx context.Context, className, snapshotID string) (string, error) {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotID)
	if err != nil {
		return "", errors.Wrap(err, "load snapshot meta")
	}

	return string(snapshot.Status), nil
}

func (m *StorageFileSystemModule) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	snapshot, err := m.loadSnapshotMeta(ctx, className, snapshotID)
	if err != nil {
		return errors.Wrap(err, "load snapshot meta")
	}

	snapshot.Status = snapshots.Status(status)

	if err := m.saveMeta(snapshot); err != nil {
		return errors.Wrap(err, "save snapshot meta")
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
		return errors.Wrapf(err, "invalid snapshots path provided")
	}
	m.snapshotsPath = snapshotsPath

	return nil
}

func (m *StorageFileSystemModule) createSnapshotsDir(snapshotsPath string) error {
	if err := os.MkdirAll(snapshotsPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_snapshots_dir").
			WithError(err).
			Errorf("failed creating snapshots directory")
		return err
	}
	return nil
}

func (m *StorageFileSystemModule) createSnapshotDir(snapshot *snapshots.Snapshot) (snapshotPath string, err error) {
	snapshotPath = m.makeSnapshotDirPath(snapshot.ClassName, snapshot.ID)
	if err = os.MkdirAll(snapshotPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_snapshot_dir").
			WithField("snapshot_classname", snapshot.ClassName).
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating snapshots directory")
		return "", err
	}
	return snapshotPath, nil
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
		return errors.Wrapf(err, "Could not open snapshot source file %v", srcRelPath)
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dstAbsPath), os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed creating destication dir for file")
		return errors.Wrapf(err, "Could not create snapshot destination dir for file %v", srcRelPath)
	}
	dst, err := os.Create(dstAbsPath)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed creating destication file")
		return errors.Wrapf(err, "Could not create snapshot destination file %v", srcRelPath)
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "copy_file").
			WithError(err).
			Errorf("failed copying snapshot file")
		return errors.Wrapf(err, "Could not copy snapshot file %v", srcRelPath)
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
		return errors.Wrapf(err, "Could not create meta file for snapshot %v", snapshot.ID)
	}

	metaFile := m.makeMetaFilePath(snapshot.ClassName, snapshot.ID)
	if err := os.WriteFile(metaFile, content, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_classname", snapshot.ClassName).
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return errors.Wrapf(err, "Could not create meta file for snapshot %v", snapshot.ID)
	}

	return nil
}
