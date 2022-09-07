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

package modstggcs

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/backup-gcs/gcs"
	"github.com/sirupsen/logrus"
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

type BackupGCSModule struct {
	logger          logrus.FieldLogger
	backendProvider modulecapabilities.BackupBackend
	config          gcs.Config
	dataPath        string
}

func New() *BackupGCSModule {
	return &BackupGCSModule{}
}

func (m *BackupGCSModule) Name() string {
	return Name
}

func (m *BackupGCSModule) AltNames() []string {
	return []string{AltName1}
}

func (m *BackupGCSModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *BackupGCSModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()

	if err := m.initBackupBackend(ctx); err != nil {
		return errors.Wrap(err, "init backup backend")
	}

	return nil
}

func (m *BackupGCSModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BackupGCSModule) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["bucketName"] = m.config.BucketName()
	if root := m.config.BackupPath(); root != "" {
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
