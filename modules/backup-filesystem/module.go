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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	ubak "github.com/weaviate/weaviate/usecases/backup"
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

func (m *Module) HomeDir(backupID, overrideBucket, overridePath string) string {
	if overridePath != "" {
		return path.Join(overridePath, backupID)
	} else {
		return path.Join(m.makeBackupDirPath(m.backupsPath, backupID))
	}
}

func (m *Module) AllBackups(context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	var meta []*backup.DistributedBackupDescriptor
	backups, err := os.ReadDir(m.backupsPath)
	if err != nil {
		return nil, fmt.Errorf("open backups path: %w", err)
	}
	for _, bak := range backups {
		if !bak.IsDir() {
			continue
		}
		backupPath := path.Join(m.backupsPath, bak.Name())
		contents, err := os.ReadDir(backupPath)
		if err != nil {
			return nil, fmt.Errorf("read backup contents: %w", err)
		}
		for _, file := range contents {
			if file.Name() == ubak.GlobalBackupFile {
				fileName := path.Join(backupPath, file.Name())
				bytes, err := os.ReadFile(fileName)
				if err != nil {
					return nil, fmt.Errorf("read backup meta file %q: %w",
						fileName, err)
				}
				var desc backup.DistributedBackupDescriptor
				if err := json.Unmarshal(bytes, &desc); err != nil {
					return nil, fmt.Errorf("unmarshal backup meta file %q: %w",
						path.Join(backupPath, file.Name()), err)
				}
				meta = append(meta, &desc)
			}
		}
	}
	return meta, nil
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

func (m *Module) makeBackupDirPath(path, id string) string {
	return filepath.Join(path, id)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
)
