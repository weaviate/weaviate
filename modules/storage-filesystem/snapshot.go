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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

func (m *StorageFileSystemModule) StoreSnapshot(ctx context.Context, snapshot snapshots.Snapshot) error {
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
		if err := m.copyFile(dstSnapshotPath, snapshot.BasePath, srcRelPath); err != nil {
			return err
		}
	}

	if err := m.saveMeta(dstSnapshotPath, snapshot); err != nil {
		return err
	}

	return nil
}

func (m *StorageFileSystemModule) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	// TODO implement
	m.logger.Errorf("RestoreSnapshot of StorageFileSystemModule not yet implemented")
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

func (m *StorageFileSystemModule) createSnapshotDir(snapshot snapshots.Snapshot) (snapshotPath string, err error) {
	snapshotPath = filepath.Join(m.snapshotsPath, snapshot.ID)
	if err = os.Mkdir(snapshotPath, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "create_snapshot_dir").
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

func (m *StorageFileSystemModule) saveMeta(dstSnapshotPath string, snapshot snapshots.Snapshot) error {
	content, err := json.Marshal(snapshot)
	if err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return errors.Wrapf(err, "Could not create meta file for snapshot %v", snapshot.ID)
	}

	metaFile := filepath.Join(dstSnapshotPath, "snapshot.json")
	if err := ioutil.WriteFile(metaFile, content, os.ModePerm); err != nil {
		m.logger.WithField("module", m.Name()).
			WithField("action", "save_meta").
			WithField("snapshot_id", snapshot.ID).
			WithError(err).
			Errorf("failed creating meta file")
		return errors.Wrapf(err, "Could not create meta file for snapshot %v", snapshot.ID)
	}

	return nil
}
