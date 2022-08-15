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
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus"
)

const (
	Name              = "storage-filesystem"
	AltName1          = "filesystem"
	snapshotsPathName = "STORAGE_FS_SNAPSHOTS_PATH"
)

type StorageFileSystemModule struct {
	logger        logrus.FieldLogger
	dataPath      string // path to the current (operational) data
	snapshotsPath string // complete(?) path to the directory that holds all the snapshots
}

func New() *StorageFileSystemModule {
	return &StorageFileSystemModule{}
}

func (m *StorageFileSystemModule) Name() string {
	return Name
}

func (m *StorageFileSystemModule) AltNames() []string {
	return []string{AltName1}
}

func (m *StorageFileSystemModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *StorageFileSystemModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()
	snapshotsPath := os.Getenv(snapshotsPathName)
	if err := m.initSnapshotStorage(ctx, snapshotsPath); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *StorageFileSystemModule) DestinationPath(className, snapshotID string) string {
	return m.makeSnapshotDirPath(className, snapshotID)
}

func (m *StorageFileSystemModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *StorageFileSystemModule) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["snapshotsPath"] = m.snapshotsPath
	return metaInfo, nil
}

func (m *StorageFileSystemModule) makeSnapshotDirPath(className, id string) string {
	return filepath.Join(m.snapshotsPath, className, id)
}

func (m *StorageFileSystemModule) makeSnapshotFilePath(className, id, relPath string) string {
	return filepath.Join(m.makeSnapshotDirPath(className, id), relPath)
}

func (m *StorageFileSystemModule) makeMetaFilePath(className, id string) string {
	dir := m.makeSnapshotDirPath(className, id)
	return filepath.Join(dir, "snapshot.json")
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.SnapshotStorage(New())
	_ = modulecapabilities.MetaProvider(New())
)
