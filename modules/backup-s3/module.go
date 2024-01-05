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

package modstgs3

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
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
	// if left unset, the backup files will
	// be stored directly in the root of the
	// bucket.
	s3Path = "BACKUP_S3_PATH"
)

type Module struct {
	*s3Client
	logger   logrus.FieldLogger
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
	bucket := os.Getenv(s3Bucket)
	if bucket == "" {
		return errors.Errorf("backup init: '%s' must be set", s3Bucket)
	}
	// SSL on by default
	useSSL := strings.ToLower(os.Getenv(s3UseSSL)) != "false"
	config := newConfig(os.Getenv(s3Endpoint), bucket, os.Getenv(s3Path), useSSL)
	client, err := newClient(config, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize S3 backup module")
	}
	m.s3Client = client
	return nil
}

func (m *Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{}, 4)
	metaInfo["endpoint"] = m.config.Endpoint
	metaInfo["bucketName"] = m.config.Bucket
	if root := m.config.BackupPath; root != "" {
		metaInfo["rootName"] = root
	}
	metaInfo["useSSL"] = m.config.UseSSL
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
)
