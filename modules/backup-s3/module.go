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

package modstgs3

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/backup-s3/s3"
	"github.com/sirupsen/logrus"
)

const (
	Name       = "backup-s3"
	AltName1   = "s3"
	s3Endpoint = "BACKUP_S3_ENDPOINT"
	s3Bucket   = "BACKUP_S3_BUCKET"
	s3UseSSL   = "BACKUP_S3_USE_SSL"

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket.
	//
	// if left unset, the snapshot files will
	// be stored directly in the root of the
	// bucket.
	s3Path = "BACKUP_S3_PATH"
)

type BackupS3Module struct {
	logger          logrus.FieldLogger
	backendProvider modulecapabilities.BackupBackend
	config          s3.Config
	dataPath        string
}

func New() *BackupS3Module {
	return &BackupS3Module{}
}

func (m *BackupS3Module) Name() string {
	return Name
}

func (m *BackupS3Module) AltNames() []string {
	return []string{AltName1}
}

func (m *BackupS3Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *BackupS3Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()

	if err := m.initBackupBackend(ctx); err != nil {
		return errors.Wrap(err, "init backup backend")
	}

	return nil
}

func (m *BackupS3Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BackupS3Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["endpoint"] = m.config.Endpoint()
	metaInfo["bucketName"] = m.config.BucketName()
	if root := m.config.BackupPath(); root != "" {
		metaInfo["rootName"] = root
	}
	metaInfo["useSSL"] = m.config.UseSSL()
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
)
