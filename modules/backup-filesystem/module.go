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
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus"
)

const (
	Name            = "backup-filesystem"
	AltName1        = "filesystem"
	backupsPathName = "BACKUP_FILESYSTEM_PATH"
)

type BackupFileSystemModule struct {
	logger        logrus.FieldLogger
	dataPath      string // path to the current (operational) data
	snapshotsPath string // complete(?) path to the directory that holds all the snapshots
}

func New() *BackupFileSystemModule {
	return &BackupFileSystemModule{}
}

func (m *BackupFileSystemModule) Name() string {
	return Name
}

func (m *BackupFileSystemModule) AltNames() []string {
	return []string{AltName1}
}

func (m *BackupFileSystemModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *BackupFileSystemModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()
	backupsPath := os.Getenv(backupsPathName)
	if err := m.initSnapshotStorage(ctx, backupsPath); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *BackupFileSystemModule) HomeDir(snapshotID string) string {
	return path.Join(m.makeSnapshotDirPath(snapshotID))
}

func (m *BackupFileSystemModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BackupFileSystemModule) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["snapshotsPath"] = m.snapshotsPath
	return metaInfo, nil
}

func (m *BackupFileSystemModule) makeSnapshotDirPath(id string) string {
	return filepath.Join(m.snapshotsPath, id)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupStorage(New())
	_ = modulecapabilities.MetaProvider(New())
)
