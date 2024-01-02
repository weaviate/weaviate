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
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	Name            = "backup-filesystem"
	AltName1        = "filesystem"
	backupsPathName = "BACKUP_FILESYSTEM_PATH"
)

type Module struct {
	logger      logrus.FieldLogger
	dataPath    string // path to the current (operational) data
	backupsPath string // complete(?) path to the directory that holds all the backups
}

func New() *Module {
	return &Module{}
}

func (m *Module) Name() string {
	return Name
}

func (m *Module) IsExternal() bool {
	return false
}

func (m *Module) AltNames() []string {
	return []string{AltName1}
}

func (m *Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()
	backupsPath := os.Getenv(backupsPathName)
	if err := m.initBackupBackend(ctx, backupsPath); err != nil {
		return errors.Wrap(err, "init backup backend")
	}

	return nil
}

func (m *Module) HomeDir(backupID string) string {
	return path.Join(m.makeBackupDirPath(backupID))
}

func (m *Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["backupsPath"] = m.backupsPath
	return metaInfo, nil
}

func (m *Module) makeBackupDirPath(id string) string {
	return filepath.Join(m.backupsPath, id)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
)
