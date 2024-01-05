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

package modstgazure

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	Name           = "backup-azure"
	AltName1       = "azure"
	azureContainer = "BACKUP_AZURE_CONTAINER"

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided container.
	//
	// if left unset, the backup files will
	// be stored directly in the root of the
	// container.
	azurePath = "BACKUP_AZURE_PATH"
)

type clientConfig struct {
	Container string

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket
	BackupPath string
}

type Module struct {
	logger logrus.FieldLogger
	*azureClient
	dataPath string
}

func New() *Module {
	return &Module{}
}

func (m *Module) Name() string {
	return Name
}

func (m *Module) IsExternal() bool {
	return true
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

	config := &clientConfig{
		Container:  os.Getenv(azureContainer),
		BackupPath: os.Getenv(azurePath),
	}
	if config.Container == "" {
		return errors.Errorf("backup init: '%s' must be set", azureContainer)
	}

	client, err := newClient(ctx, config, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "init Azure client")
	}
	m.azureClient = client
	return nil
}

func (m *Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["containerName"] = m.config.Container
	if root := m.config.BackupPath; root != "" {
		metaInfo["rootName"] = root
	}
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
)
