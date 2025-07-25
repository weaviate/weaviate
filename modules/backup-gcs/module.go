//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modstggcs

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	Name      = "backup-gcs"
	AltName1  = "gcs"
	gcsBucket = "BACKUP_GCS_BUCKET"

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket.
	//
	// if left unset, the backup files will
	// be stored directly in the root of the
	// bucket.
	gcsPath = "BACKUP_GCS_PATH"
)

type clientConfig struct {
	Bucket string

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket
	BackupPath string
}

type Module struct {
	logger logrus.FieldLogger
	*gcsClient
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
		Bucket:     os.Getenv(gcsBucket),
		BackupPath: os.Getenv(gcsPath),
	}
	if config.Bucket == "" {
		return errors.Errorf("backup init: '%s' must be set", gcsBucket)
	}

	client, err := newClient(ctx, config, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "init gcs client")
	}
	m.gcsClient = client
	return nil
}

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["bucketName"] = m.config.Bucket
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
